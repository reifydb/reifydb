// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use rand::Rng;
use reifydb_core::interface::auth::AuthenticationProvider;
use reifydb_type::{Result, error::Error};

use crate::{crypto::constant_time_eq, error::AuthError};

pub struct TokenProvider;

impl AuthenticationProvider for TokenProvider {
	fn method(&self) -> &str {
		"token"
	}

	fn create(&self, _config: &HashMap<String, String>) -> Result<HashMap<String, String>> {
		let mut bytes = [0u8; 32];
		rand::rng().fill_bytes(&mut bytes);

		// Encode as hex for readability
		let token: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();

		Ok(HashMap::from([("token".into(), token)]))
	}

	fn validate(&self, stored: &HashMap<String, String>, credential: &str) -> Result<bool> {
		let token = stored.get("token").ok_or_else(|| Error::from(AuthError::MissingToken))?;

		// Constant-time comparison
		Ok(constant_time_eq(token.as_bytes(), credential.as_bytes()))
	}
}
