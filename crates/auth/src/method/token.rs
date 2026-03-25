// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::auth::{AuthStep, AuthenticationProvider};
use reifydb_runtime::context::rng::Rng;
use reifydb_type::{Result, error::Error};
use subtle::ConstantTimeEq;

use crate::error::AuthError;

pub struct TokenProvider;

impl AuthenticationProvider for TokenProvider {
	fn method(&self) -> &str {
		"token"
	}

	fn create(&self, rng: &Rng, config: &HashMap<String, String>) -> Result<HashMap<String, String>> {
		let token = if let Some(explicit) = config.get("token") {
			explicit.clone()
		} else {
			let bytes = rng.bytes_32();
			bytes.iter().map(|b| format!("{:02x}", b)).collect()
		};

		Ok(HashMap::from([("token".into(), token)]))
	}

	fn authenticate(
		&self,
		stored: &HashMap<String, String>,
		credentials: &HashMap<String, String>,
	) -> Result<AuthStep> {
		let credential = credentials.get("token").ok_or_else(|| Error::from(AuthError::MissingToken))?;
		let token = stored.get("token").ok_or_else(|| Error::from(AuthError::MissingToken))?;

		// Constant-time comparison
		if token.as_bytes().ct_eq(credential.as_bytes()).into() {
			Ok(AuthStep::Authenticated)
		} else {
			Ok(AuthStep::Failed)
		}
	}
}
