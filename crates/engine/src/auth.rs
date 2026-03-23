// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Authentication service for ReifyDB.
//!
//! Provides a unified authentication API used by all transports (HTTP, WebSocket,
//! gRPC) and embedded mode. Supports pluggable authentication methods including
//! single-step (password, token) and multi-step challenge-response flows.

use std::{collections::HashMap, sync::Arc, time::Duration};

use reifydb_auth::{challenge::ChallengeStore, error::AuthError, registry::AuthenticationRegistry};
use reifydb_catalog::{
	catalog::Catalog, create_token, drop_expired_tokens, drop_token, drop_tokens_by_identity, find_token_by_value,
};
use reifydb_core::{
	event::EventBus,
	interface::{auth::AuthStep, catalog::token::TokenDef},
};
use reifydb_runtime::context::{clock::Clock, rng::Rng as SystemRng};
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction},
};
use reifydb_type::{
	error::Error,
	value::{datetime::DateTime, identity::IdentityId},
};
use tracing::instrument;

/// Response from an authentication attempt.
#[derive(Debug, Clone)]
pub enum AuthResponse {
	/// Authentication succeeded. Contains the session token and identity.
	Authenticated {
		identity: IdentityId,
		token: String,
	},
	/// The provider requires a challenge-response round-trip.
	Challenge {
		challenge_id: String,
		data: HashMap<String, String>,
	},
	/// Authentication failed (wrong credentials, unknown user, etc.).
	Failed {
		reason: String,
	},
}

/// Configuration for the authentication service.
#[derive(Debug, Clone)]
pub struct AuthServiceConfig {
	/// Default session token TTL. `None` means tokens don't expire.
	pub session_ttl: Option<Duration>,
	/// TTL for pending challenges (default: 60 seconds).
	pub challenge_ttl: Duration,
}

impl Default for AuthServiceConfig {
	fn default() -> Self {
		Self {
			session_ttl: Some(Duration::from_secs(24 * 60 * 60)), // 24 hours
			challenge_ttl: Duration::from_secs(60),
		}
	}
}

/// Shared authentication service.
///
/// Coordinates between the user catalog, authentication providers, and
/// token/challenge stores. All transports and embedded mode call through
/// this single service.
pub struct AuthService {
	catalog: Catalog,
	auth_registry: Arc<AuthenticationRegistry>,
	challenges: ChallengeStore,
	multi: MultiTransaction,
	single: SingleTransaction,
	event_bus: EventBus,
	rng: SystemRng,
	clock: Clock,
	session_ttl: Option<Duration>,
}

impl AuthService {
	pub fn new(
		catalog: Catalog,
		auth_registry: Arc<AuthenticationRegistry>,
		multi: MultiTransaction,
		single: SingleTransaction,
		event_bus: EventBus,
		rng: SystemRng,
		clock: Clock,
		config: AuthServiceConfig,
	) -> Self {
		Self {
			catalog,
			auth_registry,
			challenges: ChallengeStore::new(config.challenge_ttl),
			multi,
			single,
			event_bus,
			rng,
			clock,
			session_ttl: config.session_ttl,
		}
	}

	/// Get the current time as a DateTime.
	fn now(&self) -> DateTime {
		DateTime::from_timestamp_nanos(self.clock.now_nanos())
	}

	/// Compute the expiration DateTime from the configured session TTL.
	fn expires_at(&self) -> Option<DateTime> {
		self.session_ttl.map(|ttl| {
			let nanos = self.clock.now_nanos() + ttl.as_nanos();
			DateTime::from_timestamp_nanos(nanos)
		})
	}

	/// Persist a token to the database.
	fn persist_token(&self, token: &str, identity: IdentityId, user: u64) -> Result<TokenDef, Error> {
		let mut admin = AdminTransaction::new(
			self.multi.clone(),
			self.single.clone(),
			self.event_bus.clone(),
			Interceptors::default(),
			IdentityId::system(),
		)?;

		let def = create_token(&mut admin, token, identity, user, self.expires_at(), self.now())?;

		admin.commit()?;
		Ok(def)
	}

	/// Authenticate a user with the given method and credentials.
	///
	/// For single-step methods (password, token), returns `Authenticated` or `Failed`.
	/// For challenge-response methods, may return `Challenge` first, then `Authenticated`
	/// on the second call with the challenge response.
	#[instrument(name = "auth::authenticate", level = "debug", skip(self, credentials))]
	pub fn authenticate(
		&self,
		method: &str,
		username: &str,
		credentials: HashMap<String, String>,
	) -> Result<AuthResponse, Error> {
		// If this is a challenge response, resolve the original challenge
		if let Some(challenge_id) = credentials.get("challenge_id").cloned() {
			return self.authenticate_challenge_response(&challenge_id, credentials);
		}

		// Begin a read-only transaction to look up user and credentials
		let mut txn =
			QueryTransaction::new(self.multi.begin_query()?, self.single.clone(), IdentityId::system());

		// Look up user
		let user = match self.catalog.find_user_by_name(&mut Transaction::Query(&mut txn), username)? {
			Some(u) => u,
			None => {
				return Ok(AuthResponse::Failed {
					reason: "invalid credentials".to_string(),
				});
			}
		};

		if !user.enabled {
			return Ok(AuthResponse::Failed {
				reason: "user is disabled".to_string(),
			});
		}

		// Look up stored auth credentials for this method
		let stored_auth = match self.catalog.find_user_authentication_by_user_and_method(
			&mut Transaction::Query(&mut txn),
			user.id,
			method,
		)? {
			Some(a) => a,
			None => {
				return Ok(AuthResponse::Failed {
					reason: "invalid credentials".to_string(),
				});
			}
		};

		// Get the provider
		let provider = self.auth_registry.get(method).ok_or_else(|| {
			Error::from(AuthError::UnknownMethod {
				method: method.to_string(),
			})
		})?;

		// Call the provider
		match provider.authenticate(&stored_auth.properties, &credentials)? {
			AuthStep::Authenticated => {
				let token = generate_session_token(&self.rng);
				self.persist_token(&token, user.identity, user.id)?;
				Ok(AuthResponse::Authenticated {
					identity: user.identity,
					token,
				})
			}
			AuthStep::Failed => Ok(AuthResponse::Failed {
				reason: "invalid credentials".to_string(),
			}),
			AuthStep::Challenge {
				data,
			} => {
				let challenge_id =
					self.challenges.create(username.to_string(), method.to_string(), data.clone());
				Ok(AuthResponse::Challenge {
					challenge_id,
					data,
				})
			}
		}
	}

	/// Complete a challenge-response authentication flow.
	fn authenticate_challenge_response(
		&self,
		challenge_id: &str,
		mut credentials: HashMap<String, String>,
	) -> Result<AuthResponse, Error> {
		let challenge = match self.challenges.consume(challenge_id) {
			Some(c) => c,
			None => {
				return Ok(AuthResponse::Failed {
					reason: "invalid or expired challenge".to_string(),
				});
			}
		};

		// Merge challenge data into credentials so the provider can verify
		for (k, v) in &challenge.data {
			credentials.entry(k.clone()).or_insert_with(|| v.clone());
		}

		// Remove the challenge_id from credentials before passing to provider
		credentials.remove("challenge_id");

		// Look up user and auth again (challenge may have been issued a while ago)
		let mut txn =
			QueryTransaction::new(self.multi.begin_query()?, self.single.clone(), IdentityId::system());

		let user =
			match self.catalog.find_user_by_name(&mut Transaction::Query(&mut txn), &challenge.username)? {
				Some(u) if u.enabled => u,
				_ => {
					return Ok(AuthResponse::Failed {
						reason: "invalid credentials".to_string(),
					});
				}
			};

		let stored_auth = match self.catalog.find_user_authentication_by_user_and_method(
			&mut Transaction::Query(&mut txn),
			user.id,
			&challenge.method,
		)? {
			Some(a) => a,
			None => {
				return Ok(AuthResponse::Failed {
					reason: "invalid credentials".to_string(),
				});
			}
		};

		let provider = self.auth_registry.get(&challenge.method).ok_or_else(|| {
			Error::from(AuthError::UnknownMethod {
				method: challenge.method.clone(),
			})
		})?;

		match provider.authenticate(&stored_auth.properties, &credentials)? {
			AuthStep::Authenticated => {
				let token = generate_session_token(&self.rng);
				self.persist_token(&token, user.identity, user.id)?;
				Ok(AuthResponse::Authenticated {
					identity: user.identity,
					token,
				})
			}
			AuthStep::Failed => Ok(AuthResponse::Failed {
				reason: "invalid credentials".to_string(),
			}),
			AuthStep::Challenge {
				..
			} => Ok(AuthResponse::Failed {
				reason: "nested challenges are not supported".to_string(),
			}),
		}
	}

	/// Validate a bearer token and return the associated token definition.
	///
	/// Checks in order:
	/// 1. Persisted session tokens (from login)
	/// 2. Catalog tokens (from `CREATE AUTHENTICATION ... { method: token }`)
	pub fn validate_token(&self, token: &str) -> Option<TokenDef> {
		// 1. Check persisted session tokens
		let mut txn = QueryTransaction::new(
			self.multi.begin_query().ok()?,
			self.single.clone(),
			IdentityId::system(),
		);

		if let Ok(Some(def)) = find_token_by_value(&mut Transaction::Query(&mut txn), token) {
			// Check expiry
			if let Some(expires_at) = def.expires_at {
				if expires_at < self.now() {
					return None;
				}
			}
			return Some(def);
		}

		// 2. Fall back to catalog-stored tokens
		self.validate_catalog_token(token)
	}

	/// Check if a token matches any catalog-stored token authentication.
	fn validate_catalog_token(&self, token: &str) -> Option<TokenDef> {
		let provider = self.auth_registry.get("token")?;

		let mut txn = QueryTransaction::new(
			self.multi.begin_query().ok()?,
			self.single.clone(),
			IdentityId::system(),
		);

		let auths = self
			.catalog
			.list_user_authentications_by_method(&mut Transaction::Query(&mut txn), "token")
			.ok()?;

		let creds = HashMap::from([("token".to_string(), token.to_string())]);

		for auth in auths {
			if let Ok(AuthStep::Authenticated) = provider.authenticate(&auth.properties, &creds) {
				// Look up the user via materialized catalog (no transaction needed)
				if let Some(user) = self.catalog.materialized.find_user(auth.user_id) {
					if user.enabled {
						return Some(TokenDef {
							id: 0,
							token: token.to_string(),
							identity: user.identity,
							user: user.id,
							expires_at: None,
							created_at: DateTime::default(),
						});
					}
				}
			}
		}

		None
	}

	/// Revoke a specific session token.
	pub fn revoke_token(&self, token: &str) -> bool {
		let mut txn = match QueryTransaction::new(
			match self.multi.begin_query() {
				Ok(q) => q,
				Err(_) => return false,
			},
			self.single.clone(),
			IdentityId::system(),
		) {
			txn => txn,
		};

		let def = match find_token_by_value(&mut Transaction::Query(&mut txn), token) {
			Ok(Some(def)) => def,
			_ => return false,
		};
		drop(txn);

		let mut admin = match AdminTransaction::new(
			self.multi.clone(),
			self.single.clone(),
			self.event_bus.clone(),
			Interceptors::default(),
			IdentityId::system(),
		) {
			Ok(a) => a,
			Err(_) => return false,
		};

		if drop_token(&mut admin, def.id).is_err() {
			return false;
		}

		admin.commit().is_ok()
	}

	/// Revoke all session tokens for a given identity.
	pub fn revoke_all(&self, identity: IdentityId) {
		if let Ok(mut admin) = AdminTransaction::new(
			self.multi.clone(),
			self.single.clone(),
			self.event_bus.clone(),
			Interceptors::default(),
			IdentityId::system(),
		) {
			if drop_tokens_by_identity(&mut admin, identity).is_ok() {
				let _ = admin.commit();
			}
		}
	}

	/// Clean up expired sessions and challenges.
	pub fn cleanup_expired(&self) {
		if let Ok(mut admin) = AdminTransaction::new(
			self.multi.clone(),
			self.single.clone(),
			self.event_bus.clone(),
			Interceptors::default(),
			IdentityId::system(),
		) {
			if drop_expired_tokens(&mut admin, self.now()).is_ok() {
				let _ = admin.commit();
			}
		}
		self.challenges.cleanup_expired();
	}
}

/// Generate a session token (64 hex characters) using the provided RNG.
fn generate_session_token(rng: &SystemRng) -> String {
	let bytes = rng.bytes_32();
	bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
