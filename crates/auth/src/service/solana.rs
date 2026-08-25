// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::auth::{AuthStep, AuthenticationProvider};
use reifydb_value::{
	error::Error,
	reifydb_assertions,
	value::identity::{IdentityId, IdentityKind},
};

use super::{AuthResponse, AuthService};
use crate::error::AuthError;

pub(crate) const SOLANA_PUBLIC_KEY_ATTRIBUTE: &str = "solana_public_key";

impl AuthService {
	pub(crate) fn begin_solana_provision(
		&self,
		identifier: &str,
		public_key: &str,
		credentials: &HashMap<String, String>,
	) -> Result<AuthResponse, Error> {
		let provider = self.solana_provider()?;
		let properties = self.solana_properties(provider, public_key)?;

		let AuthStep::Challenge {
			payload,
		} = provider.authenticate(&properties, credentials)?
		else {
			return Ok(AuthResponse::Failed {
				reason: "wallet provisioning requires a signing challenge".to_string(),
			});
		};

		let challenge_id = self.challenges.create(
			identifier.to_string(),
			"solana".to_string(),
			payload.clone(),
			Some(public_key.to_string()),
			&self.clock,
			&self.rng,
		);
		Ok(AuthResponse::Challenge {
			challenge_id,
			payload,
		})
	}

	pub(crate) fn complete_solana_provision(
		&self,
		identifier: &str,
		public_key: &str,
		challenge_payload: &HashMap<String, String>,
		credentials: &HashMap<String, String>,
	) -> Result<AuthResponse, Error> {
		let provider = self.solana_provider()?;
		let properties = self.solana_properties(provider, public_key)?;

		match provider.verify_challenge(&properties, challenge_payload, credentials)? {
			AuthStep::Authenticated => {
				let identity = self.create_solana_identity(identifier, public_key, properties)?;
				self.finalize_authentication(identity)
			}
			AuthStep::Rejected {
				reason,
			} => Ok(AuthResponse::Failed {
				reason,
			}),
			AuthStep::Failed => Ok(invalid_credentials()),
			AuthStep::Challenge {
				..
			} => Ok(AuthResponse::Failed {
				reason: "nested challenges are not supported".to_string(),
			}),
		}
	}

	#[inline]
	fn solana_provider(&self) -> Result<&dyn AuthenticationProvider, Error> {
		self.auth_registry.get("solana").ok_or_else(|| {
			Error::from(AuthError::UnknownMethod {
				method: "solana".to_string(),
			})
		})
	}

	#[inline]
	fn solana_properties(
		&self,
		provider: &dyn AuthenticationProvider,
		public_key: &str,
	) -> Result<HashMap<String, String>, Error> {
		provider.create(&self.rng, &HashMap::from([("public_key".to_string(), public_key.to_string())]))
	}

	#[inline]
	fn create_solana_identity(
		&self,
		identifier: &str,
		public_key: &str,
		properties: HashMap<String, String>,
	) -> Result<IdentityId, Error> {
		let mut admin = self.engine.begin_admin()?;
		let catalog = self.engine.catalog();

		let ident =
			catalog.create_identity(&mut admin, identifier, IdentityKind::User, &self.clock, &self.rng)?;
		catalog.create_authentication(&mut admin, ident.id, "solana", properties)?;
		self.set_lookup_attribute(&mut admin, ident.id, SOLANA_PUBLIC_KEY_ATTRIBUTE, public_key)?;
		admin.commit()?;

		reifydb_assertions! {
			assert!(
				ident.id != IdentityId::default(),
				"auto-provisioning created the nil placeholder identity instead of a freshly generated one, so the provisioned principal would later be minted a session token bound to the default id and gain authorization (identifier={identifier:?})"
			);
		}
		Ok(ident.id)
	}
}

#[inline]
fn invalid_credentials() -> AuthResponse {
	AuthResponse::Failed {
		reason: "invalid credentials".to_string(),
	}
}
