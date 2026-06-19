// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::auth::{AuthStep, AuthenticationProvider};
use reifydb_value::{error::Error, reifydb_assertions, value::identity::IdentityId};

use super::{AuthResponse, AuthService, generate_session_token};
use crate::error::AuthError;

impl AuthService {
	pub(crate) fn auto_provision_solana(
		&self,
		identifier: &str,
		public_key: &str,
		credentials: &HashMap<String, String>,
	) -> Result<AuthResponse, Error> {
		let provider = self.solana_provider()?;
		let properties = provider
			.create(&self.rng, &HashMap::from([("public_key".to_string(), public_key.to_string())]))?;
		let identity = self.create_solana_identity(identifier, properties.clone())?;
		let step = provider.authenticate(&properties, credentials)?;
		self.respond_to_provisioned_auth_step(step, identity, identifier)
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
	fn create_solana_identity(
		&self,
		identifier: &str,
		properties: HashMap<String, String>,
	) -> Result<IdentityId, Error> {
		let mut admin = self.engine.begin_admin()?;
		let catalog = self.engine.catalog();

		let ident = catalog.create_identity(&mut admin, identifier, &self.clock, &self.rng)?;
		catalog.create_authentication(&mut admin, ident.id, "solana", properties)?;
		admin.commit()?;

		reifydb_assertions! {
			assert!(
				ident.id != IdentityId::default(),
				"auto-provisioning created the nil placeholder identity instead of a freshly generated one, so the provisioned principal would later be minted a session token bound to the default id and gain authorization (identifier={identifier:?})"
			);
		}
		Ok(ident.id)
	}

	#[inline]
	fn respond_to_provisioned_auth_step(
		&self,
		step: AuthStep,
		identity: IdentityId,
		identifier: &str,
	) -> Result<AuthResponse, Error> {
		match step {
			AuthStep::Challenge {
				payload,
			} => {
				let challenge_id = self.challenges.create(
					identifier.to_string(),
					"solana".to_string(),
					payload.clone(),
					&self.clock,
					&self.rng,
				);
				Ok(AuthResponse::Challenge {
					challenge_id,
					payload,
				})
			}
			AuthStep::Authenticated => {
				let token = generate_session_token(&self.rng);
				self.persist_token(&token, identity)?;
				Ok(AuthResponse::Authenticated {
					identity,
					token,
				})
			}
			AuthStep::Failed => Ok(AuthResponse::Failed {
				reason: "auto-provision succeeded but authentication failed".to_string(),
			}),
		}
	}
}
