// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::auth::{AuthStep, AuthenticationProvider};
use reifydb_runtime::context::rng::Rng;
use reifydb_value::{Result, error::Error};

use crate::error::AuthError;

pub struct GuestProvider;

impl AuthenticationProvider for GuestProvider {
	fn method(&self) -> &str {
		"guest"
	}

	fn create(&self, _rng: &Rng, _config: &HashMap<String, String>) -> Result<HashMap<String, String>> {
		Err(Error::from(AuthError::GuestHasNoCredentials))
	}

	fn authenticate(
		&self,
		_stored: &HashMap<String, String>,
		_credentials: &HashMap<String, String>,
	) -> Result<AuthStep> {
		Ok(AuthStep::Failed)
	}
}
