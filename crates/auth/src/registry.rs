// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::auth::AuthenticationProvider;
use reifydb_runtime::context::clock::Clock;

use crate::method::{password::PasswordProvider, solana::SolanaProvider, token::TokenProvider};

pub struct AuthenticationRegistry {
	providers: HashMap<String, Box<dyn AuthenticationProvider>>,
}

impl AuthenticationRegistry {
	pub fn new(clock: Clock) -> Self {
		let mut providers: HashMap<String, Box<dyn AuthenticationProvider>> = HashMap::new();
		providers.insert("password".to_string(), Box::new(PasswordProvider));
		providers.insert("token".to_string(), Box::new(TokenProvider));
		providers.insert("solana".to_string(), Box::new(SolanaProvider::new(clock)));
		Self {
			providers,
		}
	}

	pub fn register(&mut self, provider: Box<dyn AuthenticationProvider>) {
		self.providers.insert(provider.method().to_string(), provider);
	}

	pub fn get(&self, method: &str) -> Option<&dyn AuthenticationProvider> {
		self.providers.get(method).map(|p| p.as_ref())
	}
}

impl Default for AuthenticationRegistry {
	fn default() -> Self {
		Self::new(Clock::Real)
	}
}
