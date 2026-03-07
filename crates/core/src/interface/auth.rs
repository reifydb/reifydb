// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_type::Result;

pub trait AuthenticationProvider: Send + Sync {
	fn method(&self) -> &str;
	fn create(&self, config: &HashMap<String, String>) -> Result<HashMap<String, String>>;
	fn validate(&self, stored: &HashMap<String, String>, credential: &str) -> Result<bool>;
}
