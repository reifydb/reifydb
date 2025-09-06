// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[derive(Debug, Clone)]
pub struct AdminConfig {
	pub enabled: bool,
	pub port: u16,
	pub bind_address: String,
	pub auth_required: bool,
	pub auth_token: Option<String>,
}

impl AdminConfig {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_port(mut self, port: u16) -> Self {
		self.port = port;
		self
	}

	pub fn with_auth(mut self, token: String) -> Self {
		self.auth_required = true;
		self.auth_token = Some(token);
		self
	}

	pub fn address(&self) -> String {
		format!("{}:{}", self.bind_address, self.port)
	}
}

impl Default for AdminConfig {
	fn default() -> Self {
		Self {
			enabled: true,
			port: 9090,
			bind_address: "127.0.0.1".to_string(),
			auth_required: false,
			auth_token: None,
		}
	}
}
