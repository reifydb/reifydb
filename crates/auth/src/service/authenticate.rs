// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::{
	auth::{AuthStep, AuthenticationProvider},
	catalog::{authentication::Authentication, authentication_method::AuthenticationMethod, identity::Identity},
};
use reifydb_transaction::transaction::{Transaction, query::QueryTransaction};
use reifydb_value::{
	error::Error,
	reifydb_assertions,
	value::{Value, duration::Duration, identity::IdentityId},
};
use tracing::instrument;

use super::{AuthResponse, AuthService, generate_session_token, solana::SOLANA_PUBLIC_KEY_ATTRIBUTE};
use crate::error::AuthError;

impl AuthService {
	#[instrument(name = "auth::authenticate", level = "debug", skip(self, credentials))]
	pub fn authenticate(&self, method: &str, credentials: HashMap<String, String>) -> Result<AuthResponse, Error> {
		self.authenticate_as(IdentityId::anonymous(), method, credentials)
	}

	#[instrument(name = "auth::authenticate_as", level = "debug", skip(self, credentials))]
	pub fn authenticate_as(
		&self,
		caller: IdentityId,
		method: &str,
		credentials: HashMap<String, String>,
	) -> Result<AuthResponse, Error> {
		self.provider_for(method)?;
		let Some(declared) = self.declared_method(method)? else {
			return Ok(AuthResponse::MethodNotEnabled {
				method: method.to_string(),
			});
		};
		let ttl = declared.ttl;
		if method == "guest" {
			return self.authenticate_guest(&credentials, ttl);
		}
		if let Some(challenge_id) = credentials.get("challenge_id").cloned() {
			return self.authenticate_challenge_response(
				caller,
				method,
				&challenge_id,
				credentials,
				&declared,
			);
		}
		if method == "token" {
			return self.authenticate_token(credentials, ttl);
		}
		if method == "github" {
			return self.begin_github_login(caller, &declared);
		}
		self.authenticate_with_provider(caller, method, credentials, &declared)
	}

	fn authenticate_with_provider(
		&self,
		caller: IdentityId,
		method: &str,
		credentials: HashMap<String, String>,
		declared: &AuthenticationMethod,
	) -> Result<AuthResponse, Error> {
		let ttl = declared.ttl;
		let identifier = credentials.get("identifier").map(|s| s.as_str()).unwrap_or("");
		let mut txn = self.engine.begin_query()?;
		let catalog = self.engine.catalog();

		let Some(ident) = self.resolve_provider_identity(&mut txn, &catalog, method, identifier)? else {
			drop(txn);
			return self.handle_missing_identity(caller, method, identifier, &credentials, declared);
		};
		if !ident.enabled {
			return Ok(AuthResponse::Failed {
				reason: "identity is disabled".to_string(),
			});
		}

		let Some(stored_auth) = self.load_stored_auth(&mut txn, &catalog, ident.id, method)? else {
			return Ok(invalid_credentials());
		};

		self.run_provider_and_respond(caller, &stored_auth, &credentials, ident.id, identifier, method, ttl)
	}

	#[inline]
	fn resolve_provider_identity(
		&self,
		txn: &mut QueryTransaction,
		catalog: &Catalog,
		method: &str,
		identifier: &str,
	) -> Result<Option<Identity>, Error> {
		if let Some(u) = catalog.find_identity_by_name(&mut Transaction::Query(txn), identifier)? {
			return Ok(Some(u));
		}
		if method == "solana" {
			return catalog.find_identity_by_attribute_value(
				&mut Transaction::Query(txn),
				SOLANA_PUBLIC_KEY_ATTRIBUTE,
				&Value::Utf8(identifier.to_string()),
			);
		}
		Ok(None)
	}

	fn load_stored_auth(
		&self,
		txn: &mut QueryTransaction,
		catalog: &Catalog,
		identity: IdentityId,
		method: &str,
	) -> Result<Option<Authentication>, Error> {
		catalog.find_authentication_by_identity_and_method(&mut Transaction::Query(txn), identity, method)
	}

	#[inline]
	fn run_provider_and_respond(
		&self,
		caller: IdentityId,
		stored_auth: &Authentication,
		credentials: &HashMap<String, String>,
		identity: IdentityId,
		identifier: &str,
		method: &str,
		ttl: Option<Duration>,
	) -> Result<AuthResponse, Error> {
		let provider = self.provider_for(method)?;
		let step = provider.authenticate(&stored_auth.properties, credentials)?;
		self.respond_to_initial_auth_step(caller, step, identity, identifier, method, ttl)
	}

	#[inline]
	fn handle_missing_identity(
		&self,
		caller: IdentityId,
		method: &str,
		identifier: &str,
		credentials: &HashMap<String, String>,
		declared: &AuthenticationMethod,
	) -> Result<AuthResponse, Error> {
		if method == "solana"
			&& let Some(public_key) = credentials.get("public_key").cloned()
		{
			return self.begin_solana_provision(caller, identifier, &public_key, credentials);
		}
		if method == "password" && declared.promote_guest {
			let properties = self.provider_for(method)?.create(&self.rng, credentials)?;
			return self.promote_or_sign_up(caller, identifier, method, properties, None, declared.ttl);
		}
		Ok(invalid_credentials())
	}

	#[inline]
	fn respond_to_initial_auth_step(
		&self,
		caller: IdentityId,
		step: AuthStep,
		identity: IdentityId,
		identifier: &str,
		method: &str,
		ttl: Option<Duration>,
	) -> Result<AuthResponse, Error> {
		match step {
			AuthStep::Authenticated => self.finalize_authentication(identity, ttl),
			AuthStep::Failed => Ok(invalid_credentials()),
			AuthStep::Rejected {
				reason,
			} => Ok(AuthResponse::Failed {
				reason,
			}),
			AuthStep::Challenge {
				payload,
			} => Ok(self.issue_challenge(caller, identifier, method, payload)),
		}
	}

	#[inline]
	pub(super) fn finalize_authentication(
		&self,
		identity: IdentityId,
		ttl: Option<Duration>,
	) -> Result<AuthResponse, Error> {
		reifydb_assertions! {
			assert!(
				identity != IdentityId::default(),
				"authentication finalized for the nil placeholder identity instead of a resolved one, so an unauthenticated principal would receive a valid session token and gain authorization (identity={:?})",
				identity
			);
		}
		let token = generate_session_token(&self.rng);
		self.persist_token(&token, identity, ttl)?;
		Ok(AuthResponse::Authenticated {
			identity,
			token,
		})
	}

	#[inline]
	fn issue_challenge(
		&self,
		caller: IdentityId,
		identifier: &str,
		method: &str,
		payload: HashMap<String, String>,
	) -> AuthResponse {
		let challenge_id = self.challenges.create(
			caller,
			identifier.to_string(),
			method.to_string(),
			payload.clone(),
			None,
			&self.clock,
			&self.rng,
		);
		AuthResponse::Challenge {
			challenge_id,
			payload,
		}
	}

	fn declared_method(&self, method: &str) -> Result<Option<AuthenticationMethod>, Error> {
		let mut txn = self.engine.begin_query()?;
		Ok(self.engine
			.catalog()
			.find_authentication_method_by_name(&mut Transaction::Query(&mut txn), method)?)
	}

	#[inline]
	fn provider_for(&self, method: &str) -> Result<&dyn AuthenticationProvider, Error> {
		self.auth_registry.get(method).ok_or_else(|| {
			Error::from(AuthError::UnknownMethod {
				method: method.to_string(),
			})
		})
	}

	fn authenticate_token(
		&self,
		credentials: HashMap<String, String>,
		ttl: Option<Duration>,
	) -> Result<AuthResponse, Error> {
		let token_value = match credentials.get("token") {
			Some(t) if !t.is_empty() => t,
			_ => return Ok(invalid_credentials()),
		};

		match self.validate_token(token_value)? {
			Some(token) => self.finalize_authentication(token.identity, ttl),
			None => Ok(invalid_credentials()),
		}
	}

	fn authenticate_challenge_response(
		&self,
		caller: IdentityId,
		method: &str,
		challenge_id: &str,
		mut credentials: HashMap<String, String>,
		declared: &AuthenticationMethod,
	) -> Result<AuthResponse, Error> {
		let ttl = declared.ttl;
		let Some(challenge) = self.challenges.consume(challenge_id) else {
			return Ok(AuthResponse::Failed {
				reason: "invalid or expired challenge".to_string(),
			});
		};

		if method != challenge.method {
			return Ok(AuthResponse::Failed {
				reason: "challenge was issued for a different method".to_string(),
			});
		}

		if !caller.is_anonymous() && !challenge.caller.is_anonymous() && caller != challenge.caller {
			return Ok(AuthResponse::Failed {
				reason: "challenge was issued to a different caller".to_string(),
			});
		}

		if challenge.method == "github" {
			return self.complete_github_login(&challenge, &credentials, declared);
		}

		if let Some(public_key) = challenge.pending_public_key.as_deref() {
			return self.complete_solana_provision(
				challenge.caller,
				&challenge.identifier,
				public_key,
				&challenge.payload,
				&credentials,
				declared,
			);
		}

		merge_challenge_payload(&mut credentials, &challenge.payload);

		let mut txn = self.engine.begin_query()?;
		let catalog = self.engine.catalog();

		let Some(ident) =
			self.resolve_challenge_identity(&mut txn, &catalog, &challenge.identifier, &challenge.method)?
		else {
			return Ok(invalid_credentials());
		};

		let Some(stored_auth) = self.load_stored_auth(&mut txn, &catalog, ident.id, &challenge.method)? else {
			return Ok(invalid_credentials());
		};

		self.run_challenge_provider_and_respond(
			&stored_auth,
			&challenge.payload,
			&credentials,
			ident.id,
			&challenge.method,
			ttl,
		)
	}

	#[inline]
	fn resolve_challenge_identity(
		&self,
		txn: &mut QueryTransaction,
		catalog: &Catalog,
		identifier: &str,
		method: &str,
	) -> Result<Option<Identity>, Error> {
		let resolved = match catalog.find_identity_by_name(&mut Transaction::Query(txn), identifier)? {
			Some(u) if u.enabled => Some(u),
			Some(_) => None,
			None if method == "solana" => {
				match catalog.find_identity_by_attribute_value(
					&mut Transaction::Query(txn),
					SOLANA_PUBLIC_KEY_ATTRIBUTE,
					&Value::Utf8(identifier.to_string()),
				)? {
					Some(u) if u.enabled => Some(u),
					_ => None,
				}
			}
			None => None,
		};
		reifydb_assertions! {
			if let Some(ref ident) = resolved {
				assert!(
					ident.enabled,
					"challenge identity resolution returned a disabled identity (id={:?}, name={}); a disabled principal must never advance to provider authentication or it could obtain a session token",
					ident.id,
					ident.name
				);
			}
		}
		Ok(resolved)
	}

	#[inline]
	fn run_challenge_provider_and_respond(
		&self,
		stored_auth: &Authentication,
		challenge_payload: &HashMap<String, String>,
		credentials: &HashMap<String, String>,
		identity: IdentityId,
		method: &str,
		ttl: Option<Duration>,
	) -> Result<AuthResponse, Error> {
		let provider = self.provider_for(method)?;
		let step = provider.verify_challenge(&stored_auth.properties, challenge_payload, credentials)?;
		respond_to_challenge_step(step, identity, ttl, self)
	}
}

#[inline]
fn merge_challenge_payload(credentials: &mut HashMap<String, String>, payload: &HashMap<String, String>) {
	for (k, v) in payload {
		credentials.insert(k.clone(), v.clone());
	}
	credentials.remove("challenge_id");
}

#[inline]
fn respond_to_challenge_step(
	step: AuthStep,
	identity: IdentityId,
	ttl: Option<Duration>,
	service: &AuthService,
) -> Result<AuthResponse, Error> {
	match step {
		AuthStep::Authenticated => service.finalize_authentication(identity, ttl),
		AuthStep::Failed => Ok(invalid_credentials()),
		AuthStep::Rejected {
			reason,
		} => Ok(AuthResponse::Failed {
			reason,
		}),
		AuthStep::Challenge {
			..
		} => Ok(AuthResponse::Failed {
			reason: "nested challenges are not supported".to_string(),
		}),
	}
}

#[inline]
fn invalid_credentials() -> AuthResponse {
	AuthResponse::Failed {
		reason: "invalid credentials".to_string(),
	}
}
