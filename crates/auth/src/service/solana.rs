// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::auth::AuthStep;
use reifydb_type::error::Error;

use super::{AuthResponse, AuthService, generate_session_token};
use crate::error::AuthError;

impl AuthService {
	/// Auto-provision a new user with Solana authentication.
	///
	/// Creates the user and Solana auth method, then proceeds with the challenge flow.
	pub(crate) fn auto_provision_solana(
		&self,
		username: &str,
		public_key: &str,
		credentials: &HashMap<String, String>,
	) -> Result<AuthResponse, Error> {
		let provider = self.auth_registry.get("solana").ok_or_else(|| {
			Error::from(AuthError::UnknownMethod {
				method: "solana".to_string(),
			})
		})?;

		// Validate the public key via the provider
		let properties = provider
			.create(&self.rng, &HashMap::from([("public_key".to_string(), public_key.to_string())]))?;

		// Create user + auth in an admin transaction
		let mut admin = self.engine.begin_admin()?;
		let catalog = self.engine.catalog();

		let user = catalog.create_user(&mut admin, username)?;
		catalog.create_authentication(&mut admin, user.id, "solana", properties.clone())?;
		admin.commit()?;

		// Proceed with challenge flow using the stored properties
		match provider.authenticate(&properties, credentials)? {
			AuthStep::Challenge {
				payload,
			} => {
				let challenge_id = self.challenges.create(
					username.to_string(),
					"solana".to_string(),
					payload.clone(),
				);
				Ok(AuthResponse::Challenge {
					challenge_id,
					payload,
				})
			}
			AuthStep::Authenticated => {
				let token = generate_session_token(&self.rng);
				self.persist_token(&token, user.identity, user.id)?;
				Ok(AuthResponse::Authenticated {
					identity: user.identity,
					token,
				})
			}
			AuthStep::Failed => Ok(AuthResponse::Failed {
				reason: "auto-provision succeeded but authentication failed".to_string(),
			}),
		}
	}
}
