// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::auth::AuthStep;
use reifydb_type::error::Error;

use super::{AuthResponse, AuthService, generate_session_token};
use crate::error::AuthError;

impl AuthService {
	pub(crate) fn auto_provision_solana(
		&self,
		identifier: &str,
		public_key: &str,
		credentials: &HashMap<String, String>,
	) -> Result<AuthResponse, Error> {
		let provider = self.auth_registry.get("solana").ok_or_else(|| {
			Error::from(AuthError::UnknownMethod {
				method: "solana".to_string(),
			})
		})?;

		let properties = provider
			.create(&self.rng, &HashMap::from([("public_key".to_string(), public_key.to_string())]))?;

		let mut admin = self.engine.begin_admin()?;
		let catalog = self.engine.catalog();

		let ident = catalog.create_identity(&mut admin, identifier, &self.clock, &self.rng)?;
		catalog.create_authentication(&mut admin, ident.id, "solana", properties.clone())?;
		admin.commit()?;

		match provider.authenticate(&properties, credentials)? {
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
				self.persist_token(&token, ident.id)?;
				Ok(AuthResponse::Authenticated {
					identity: ident.id,
					token,
				})
			}
			AuthStep::Failed => Ok(AuthResponse::Failed {
				reason: "auto-provision succeeded but authentication failed".to_string(),
			}),
		}
	}
}
