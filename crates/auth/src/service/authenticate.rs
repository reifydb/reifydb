// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::auth::AuthStep;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;
use tracing::instrument;

use super::{AuthResponse, AuthService, generate_session_token};
use crate::error::AuthError;

impl AuthService {
	/// Authenticate an identity with the given method and credentials.
	///
	/// For single-step methods (password, token), returns `Authenticated` or `Failed`.
	/// For challenge-response methods, may return `Challenge` first, then `Authenticated`
	/// on the second call with the challenge response.
	///
	/// For Solana authentication, if the identity does not exist and the credentials contain
	/// a `public_key`, the identity and authentication method are auto-provisioned before
	/// proceeding with the challenge-response flow.
	#[instrument(name = "auth::authenticate", level = "debug", skip(self, credentials))]
	pub fn authenticate(&self, method: &str, credentials: HashMap<String, String>) -> Result<AuthResponse, Error> {
		if let Some(challenge_id) = credentials.get("challenge_id").cloned() {
			return self.authenticate_challenge_response(&challenge_id, credentials);
		}

		if method == "token" {
			return self.authenticate_token(credentials);
		}

		let identifier = credentials.get("identifier").map(|s| s.as_str()).unwrap_or("");

		let mut txn = self.engine.begin_query()?;
		let catalog = self.engine.catalog();

		let ident = match catalog.find_identity_by_name(&mut Transaction::Query(&mut txn), identifier)? {
			Some(u) => u,
			None => {
				drop(txn);

				if method == "solana" {
					if let Some(public_key) = credentials.get("public_key").cloned() {
						return self.auto_provision_solana(
							identifier,
							&public_key,
							&credentials,
						);
					}
				}
				return Ok(AuthResponse::Failed {
					reason: "invalid credentials".to_string(),
				});
			}
		};

		if !ident.enabled {
			return Ok(AuthResponse::Failed {
				reason: "identity is disabled".to_string(),
			});
		}

		let stored_auth = match catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Query(&mut txn),
			ident.id,
			method,
		)? {
			Some(a) => a,
			None => {
				return Ok(AuthResponse::Failed {
					reason: "invalid credentials".to_string(),
				});
			}
		};

		let provider = self.auth_registry.get(method).ok_or_else(|| {
			Error::from(AuthError::UnknownMethod {
				method: method.to_string(),
			})
		})?;

		match provider.authenticate(&stored_auth.properties, &credentials)? {
			AuthStep::Authenticated => {
				let token = generate_session_token(&self.rng);
				self.persist_token(&token, ident.id)?;
				Ok(AuthResponse::Authenticated {
					identity: ident.id,
					token,
				})
			}
			AuthStep::Failed => Ok(AuthResponse::Failed {
				reason: "invalid credentials".to_string(),
			}),
			AuthStep::Challenge {
				payload,
			} => {
				let challenge_id = self.challenges.create(
					identifier.to_string(),
					method.to_string(),
					payload.clone(),
				);
				Ok(AuthResponse::Challenge {
					challenge_id,
					payload,
				})
			}
		}
	}

	fn authenticate_token(&self, credentials: HashMap<String, String>) -> Result<AuthResponse, Error> {
		let token_value = match credentials.get("token") {
			Some(t) if !t.is_empty() => t,
			_ => {
				return Ok(AuthResponse::Failed {
					reason: "invalid credentials".to_string(),
				});
			}
		};

		match self.validate_token(token_value) {
			Some(token) => {
				let session_token = generate_session_token(&self.rng);
				self.persist_token(&session_token, token.identity)?;
				Ok(AuthResponse::Authenticated {
					identity: token.identity,
					token: session_token,
				})
			}
			None => Ok(AuthResponse::Failed {
				reason: "invalid credentials".to_string(),
			}),
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

		// Merge challenge payload into credentials so the provider can verify
		for (k, v) in &challenge.payload {
			credentials.entry(k.clone()).or_insert_with(|| v.clone());
		}

		// Remove the challenge_id from credentials before passing to provider
		credentials.remove("challenge_id");

		// Look up identity and auth again (challenge may have been issued a while ago)
		let mut txn = self.engine.begin_query()?;
		let catalog = self.engine.catalog();

		let ident = match catalog
			.find_identity_by_name(&mut Transaction::Query(&mut txn), &challenge.identifier)?
		{
			Some(u) if u.enabled => u,
			_ => {
				return Ok(AuthResponse::Failed {
					reason: "invalid credentials".to_string(),
				});
			}
		};

		let stored_auth = match catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Query(&mut txn),
			ident.id,
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
				self.persist_token(&token, ident.id)?;
				Ok(AuthResponse::Authenticated {
					identity: ident.id,
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
}
