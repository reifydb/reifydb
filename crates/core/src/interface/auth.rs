// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

pub trait AuthenticationProvider: Send + Sync {
	fn method(&self) -> &str;
	fn create(&self, config: &HashMap<String, String>) -> reifydb_type::Result<HashMap<String, String>>;
	fn validate(&self, stored: &HashMap<String, String>, credential: &str) -> reifydb_type::Result<bool>;
}
