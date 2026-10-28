// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::auth::AuthenticationProvider;
use reifydb_runtime::context::{clock::Clock, rng::Rng};

use crate::method::{
	github::GithubProvider, guest::GuestProvider, password::PasswordProvider, solana::SolanaProvider,
	token::TokenProvider,
};

pub struct AuthenticationRegistry {
	providers: HashMap<String, Box<dyn AuthenticationProvider>>,
	github_client_secret: Option<String>,
}

impl AuthenticationRegistry {
	pub fn new(clock: Clock, rng: Rng) -> Self {
		let mut providers: HashMap<String, Box<dyn AuthenticationProvider>> = HashMap::new();
		providers.insert("password".to_string(), Box::new(PasswordProvider));
		providers.insert("token".to_string(), Box::new(TokenProvider));
		providers.insert("solana".to_string(), Box::new(SolanaProvider::new(clock, rng)));
		providers.insert("github".to_string(), Box::new(GithubProvider::new(None)));
		providers.insert("guest".to_string(), Box::new(GuestProvider));
		Self {
			providers,
			github_client_secret: None,
		}
	}

	pub fn with_github_client_secret(mut self, secret: String) -> Self {
		self.providers.insert("github".to_string(), Box::new(GithubProvider::new(Some(secret.clone()))));
		self.github_client_secret = Some(secret);
		self
	}

	pub fn github_client_secret(&self) -> Option<&str> {
		self.github_client_secret.as_deref()
	}

	pub fn register(&mut self, provider: Box<dyn AuthenticationProvider>) {
		self.providers.insert(provider.method().to_string(), provider);
	}

	pub fn get(&self, method: &str) -> Option<&dyn AuthenticationProvider> {
		self.providers.get(method).map(|p| p.as_ref())
	}

	pub fn names(&self) -> Vec<&str> {
		let mut names: Vec<&str> = self.providers.keys().map(String::as_str).collect();
		names.sort_unstable();
		names
	}
}

impl Default for AuthenticationRegistry {
	fn default() -> Self {
		Self::new(Clock::Real, Rng::default())
	}
}
