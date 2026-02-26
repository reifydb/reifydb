// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::auth::AuthenticationProvider;

use crate::{password::PasswordProvider, token::TokenProvider};

pub struct AuthenticationRegistry {
	providers: HashMap<String, Box<dyn AuthenticationProvider>>,
}

impl AuthenticationRegistry {
	pub fn new() -> Self {
		let mut providers: HashMap<String, Box<dyn AuthenticationProvider>> = HashMap::new();
		providers.insert("password".to_string(), Box::new(PasswordProvider));
		providers.insert("token".to_string(), Box::new(TokenProvider));
		Self {
			providers,
		}
	}

	pub fn get(&self, method: &str) -> Option<&dyn AuthenticationProvider> {
		self.providers.get(method).map(|p| p.as_ref())
	}
}

impl Default for AuthenticationRegistry {
	fn default() -> Self {
		Self::new()
	}
}
