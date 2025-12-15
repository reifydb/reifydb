// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! Configuration for the admin server subsystem.

use std::time::Duration;

use reifydb_sub_server::SharedRuntime;

/// Configuration for the admin server subsystem.
#[derive(Clone)]
pub struct AdminConfig {
	/// Address to bind the admin server to (e.g., "127.0.0.1:9090").
	pub bind_addr: String,
	/// Maximum number of concurrent connections.
	pub max_connections: usize,
	/// Timeout for entire request lifecycle.
	pub request_timeout: Duration,
	/// Whether authentication is required.
	pub auth_required: bool,
	/// Authentication token (if auth is required).
	pub auth_token: Option<String>,
	/// Optional shared runtime. If not provided, a default one will be created.
	pub runtime: Option<SharedRuntime>,
}

impl std::fmt::Debug for AdminConfig {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("AdminConfig")
			.field("bind_addr", &self.bind_addr)
			.field("max_connections", &self.max_connections)
			.field("request_timeout", &self.request_timeout)
			.field("auth_required", &self.auth_required)
			.field("auth_token", &self.auth_token.as_ref().map(|_| "****"))
			.field("runtime", &self.runtime.as_ref().map(|_| "SharedRuntime"))
			.finish()
	}
}

impl Default for AdminConfig {
	fn default() -> Self {
		Self {
			bind_addr: "127.0.0.1:9090".to_string(),
			max_connections: 1_000,
			request_timeout: Duration::from_secs(30),
			auth_required: false,
			auth_token: None,
			runtime: None,
		}
	}
}

impl AdminConfig {
	/// Create a new AdminConfig with default values.
	pub fn new() -> Self {
		Self::default()
	}

	/// Set the bind address.
	pub fn bind_addr(mut self, addr: impl Into<String>) -> Self {
		self.bind_addr = addr.into();
		self
	}

	/// Set the maximum number of connections.
	pub fn max_connections(mut self, max: usize) -> Self {
		self.max_connections = max;
		self
	}

	/// Set the request timeout.
	pub fn request_timeout(mut self, timeout: Duration) -> Self {
		self.request_timeout = timeout;
		self
	}

	/// Enable authentication with the given token.
	pub fn with_auth(mut self, token: String) -> Self {
		self.auth_required = true;
		self.auth_token = Some(token);
		self
	}

	/// Set the shared runtime.
	pub fn runtime(mut self, runtime: SharedRuntime) -> Self {
		self.runtime = Some(runtime);
		self
	}
}
