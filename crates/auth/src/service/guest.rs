// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_catalog::create_token;
use reifydb_core::retry::RetryStrategy;
use reifydb_value::{
	error::Error,
	value::{duration::Duration, identity::IdentityKind, uuid::Uuid7},
};

use super::{AuthResponse, AuthService, generate_session_token};
use crate::error::AuthError;

const GUEST_NAME_PREFIX: &str = "guest:";

impl AuthService {
	pub(super) fn authenticate_guest(
		&self,
		credentials: &HashMap<String, String>,
		ttl: Option<Duration>,
	) -> Result<AuthResponse, Error> {
		if !credentials.is_empty() {
			let mut keys: Vec<&str> = credentials.keys().map(String::as_str).collect();
			keys.sort_unstable();
			return Err(Error::from(AuthError::GuestTakesNoCredentials {
				keys: keys.join(", "),
			}));
		}

		let token = generate_session_token(&self.rng);
		let identity = RetryStrategy::default().retry(&self.rng, "auth::authenticate_guest", || {
			let mut admin = self.engine.begin_admin()?;
			let name = format!("{GUEST_NAME_PREFIX}{}", Uuid7::generate(&self.clock, &self.rng));
			let identity = self.engine.catalog().create_identity(
				&mut admin,
				&name,
				IdentityKind::Guest,
				&self.clock,
				&self.rng,
			)?;
			create_token(&mut admin, &token, identity.id, self.expires_at(ttl)?, self.now()?)?;
			admin.commit()?;
			Ok(identity.id)
		})?;

		Ok(AuthResponse::Authenticated {
			identity,
			token,
		})
	}
}
