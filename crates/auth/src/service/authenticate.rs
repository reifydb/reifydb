// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::{
	auth::{AuthStep, AuthenticationProvider},
	catalog::{authentication::Authentication, identity::Identity},
};
use reifydb_transaction::transaction::{Transaction, query::QueryTransaction};
use reifydb_value::{error::Error, reifydb_assertions, value::identity::IdentityId};
use tracing::instrument;

use super::{AuthResponse, AuthService, generate_session_token};
use crate::{challenge::ChallengeInfo, error::AuthError};

impl AuthService {
	#[instrument(name = "auth::authenticate", level = "debug", skip(self, credentials))]
	pub fn authenticate(&self, method: &str, credentials: HashMap<String, String>) -> Result<AuthResponse, Error> {
		if let Some(challenge_id) = credentials.get("challenge_id").cloned() {
			return self.authenticate_challenge_response(&challenge_id, credentials);
		}
		if method == "token" {
			return self.authenticate_token(credentials);
		}
		self.authenticate_with_provider(method, credentials)
	}

	fn authenticate_with_provider(
		&self,
		method: &str,
		credentials: HashMap<String, String>,
	) -> Result<AuthResponse, Error> {
		let identifier = credentials.get("identifier").map(|s| s.as_str()).unwrap_or("");
		let mut txn = self.engine.begin_query()?;
		let catalog = self.engine.catalog();

		let Some(ident) = self.resolve_provider_identity(&mut txn, &catalog, method, identifier)? else {
			drop(txn);
			return self.handle_missing_identity(method, identifier, &credentials);
		};
		if !ident.enabled {
			return Ok(AuthResponse::Failed {
				reason: "identity is disabled".to_string(),
			});
		}

		let Some(stored_auth) = self.load_stored_auth(&mut txn, &catalog, ident.id, method)? else {
			return Ok(invalid_credentials());
		};

		self.run_provider_and_respond(&stored_auth, &credentials, ident.id, identifier, method)
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
			return catalog.find_identity_by_solana_pubkey(&mut Transaction::Query(txn), identifier);
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
		stored_auth: &Authentication,
		credentials: &HashMap<String, String>,
		identity: IdentityId,
		identifier: &str,
		method: &str,
	) -> Result<AuthResponse, Error> {
		let provider = self.provider_for(method)?;
		let step = provider.authenticate(&stored_auth.properties, credentials)?;
		self.respond_to_initial_auth_step(step, identity, identifier, method)
	}

	#[inline]
	fn handle_missing_identity(
		&self,
		method: &str,
		identifier: &str,
		credentials: &HashMap<String, String>,
	) -> Result<AuthResponse, Error> {
		if method == "solana"
			&& let Some(public_key) = credentials.get("public_key").cloned()
		{
			return self.auto_provision_solana(identifier, &public_key, credentials);
		}
		Ok(invalid_credentials())
	}

	#[inline]
	fn respond_to_initial_auth_step(
		&self,
		step: AuthStep,
		identity: IdentityId,
		identifier: &str,
		method: &str,
	) -> Result<AuthResponse, Error> {
		match step {
			AuthStep::Authenticated => self.finalize_authentication(identity),
			AuthStep::Failed => Ok(invalid_credentials()),
			AuthStep::Challenge {
				payload,
			} => Ok(self.issue_challenge(identifier, method, payload)),
		}
	}

	#[inline]
	fn finalize_authentication(&self, identity: IdentityId) -> Result<AuthResponse, Error> {
		reifydb_assertions! {
			assert!(
				identity != IdentityId::default(),
				"authentication finalized for the nil placeholder identity instead of a resolved one, so an unauthenticated principal would receive a valid session token and gain authorization (identity={:?})",
				identity
			);
		}
		let token = generate_session_token(&self.rng);
		self.persist_token(&token, identity)?;
		Ok(AuthResponse::Authenticated {
			identity,
			token,
		})
	}

	#[inline]
	fn issue_challenge(&self, identifier: &str, method: &str, payload: HashMap<String, String>) -> AuthResponse {
		let challenge_id = self.challenges.create(
			identifier.to_string(),
			method.to_string(),
			payload.clone(),
			&self.clock,
			&self.rng,
		);
		AuthResponse::Challenge {
			challenge_id,
			payload,
		}
	}

	#[inline]
	fn provider_for(&self, method: &str) -> Result<&dyn AuthenticationProvider, Error> {
		self.auth_registry.get(method).ok_or_else(|| {
			Error::from(AuthError::UnknownMethod {
				method: method.to_string(),
			})
		})
	}

	fn authenticate_token(&self, credentials: HashMap<String, String>) -> Result<AuthResponse, Error> {
		let token_value = match credentials.get("token") {
			Some(t) if !t.is_empty() => t,
			_ => return Ok(invalid_credentials()),
		};

		match self.validate_token(token_value) {
			Some(token) => self.finalize_authentication(token.identity),
			None => Ok(invalid_credentials()),
		}
	}

	fn authenticate_challenge_response(
		&self,
		challenge_id: &str,
		mut credentials: HashMap<String, String>,
	) -> Result<AuthResponse, Error> {
		let Some(challenge) = self.consume_challenge(challenge_id, &mut credentials) else {
			return Ok(AuthResponse::Failed {
				reason: "invalid or expired challenge".to_string(),
			});
		};

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

		self.run_challenge_provider_and_respond(&stored_auth, &credentials, ident.id, &challenge.method)
	}

	#[inline]
	fn consume_challenge(
		&self,
		challenge_id: &str,
		credentials: &mut HashMap<String, String>,
	) -> Option<ChallengeInfo> {
		let challenge = self.challenges.consume(challenge_id)?;
		merge_challenge_payload(credentials, &challenge.payload);
		Some(challenge)
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
				match catalog
					.find_identity_by_solana_pubkey(&mut Transaction::Query(txn), identifier)?
				{
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
		credentials: &HashMap<String, String>,
		identity: IdentityId,
		method: &str,
	) -> Result<AuthResponse, Error> {
		let provider = self.provider_for(method)?;
		let step = provider.authenticate(&stored_auth.properties, credentials)?;
		respond_to_challenge_step(step, identity, self)
	}
}

#[inline]
fn merge_challenge_payload(credentials: &mut HashMap<String, String>, payload: &HashMap<String, String>) {
	for (k, v) in payload {
		credentials.entry(k.clone()).or_insert_with(|| v.clone());
	}
	credentials.remove("challenge_id");
}

#[inline]
fn respond_to_challenge_step(
	step: AuthStep,
	identity: IdentityId,
	service: &AuthService,
) -> Result<AuthResponse, Error> {
	match step {
		AuthStep::Authenticated => service.finalize_authentication(identity),
		AuthStep::Failed => Ok(invalid_credentials()),
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
