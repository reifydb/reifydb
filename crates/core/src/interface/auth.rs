// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_runtime::context::rng::Rng;
use reifydb_type::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthStep {
	Authenticated,

	Failed,

	Challenge {
		payload: HashMap<String, String>,
	},
}

pub trait AuthenticationProvider: Send + Sync {
	fn method(&self) -> &str;

	fn create(&self, rng: &Rng, config: &HashMap<String, String>) -> Result<HashMap<String, String>>;

	fn authenticate(
		&self,
		stored: &HashMap<String, String>,
		credentials: &HashMap<String, String>,
	) -> Result<AuthStep>;
}
