// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::auth::{AuthStep, AuthenticationProvider};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{error::Error, value::identity::IdentityId};
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

		let ident = match catalog.find_identity_by_name(&mut Transaction::Query(&mut txn), identifier)? {
			Some(u) => u,
			None => {
				drop(txn);
				return self.handle_missing_identity(method, identifier, &credentials);
			}
		};
		if !ident.enabled {
			return Ok(AuthResponse::Failed {
				reason: "identity is disabled".to_string(),
			});
		}

		let Some(stored_auth) = catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Query(&mut txn),
			ident.id,
			method,
		)?
		else {
			return Ok(invalid_credentials());
		};

		let provider = self.provider_for(method)?;
		let step = provider.authenticate(&stored_auth.properties, &credentials)?;
		self.respond_to_initial_auth_step(step, ident.id, identifier, method)
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

	/// Complete a challenge-response authentication flow.
	fn authenticate_challenge_response(
		&self,
		challenge_id: &str,
		mut credentials: HashMap<String, String>,
	) -> Result<AuthResponse, Error> {
		let Some(challenge) = self.challenges.consume(challenge_id) else {
			return Ok(AuthResponse::Failed {
				reason: "invalid or expired challenge".to_string(),
			});
		};

		merge_challenge_payload(&mut credentials, &challenge.payload);

		let mut txn = self.engine.begin_query()?;
		let catalog = self.engine.catalog();

		let ident = match catalog
			.find_identity_by_name(&mut Transaction::Query(&mut txn), &challenge.identifier)?
		{
			Some(u) if u.enabled => u,
			_ => return Ok(invalid_credentials()),
		};

		let Some(stored_auth) = catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Query(&mut txn),
			ident.id,
			&challenge.method,
		)?
		else {
			return Ok(invalid_credentials());
		};

		let provider = self.provider_for(&challenge.method)?;
		let step = provider.authenticate(&stored_auth.properties, &credentials)?;
		respond_to_challenge_step(step, ident.id, self)
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
