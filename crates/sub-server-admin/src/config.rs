// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{fmt, time::Duration};

use reifydb_runtime::SharedRuntime;

pub struct AdminConfigurator {
	bind_addr: String,
	max_connections: usize,
	request_timeout: Duration,
	auth_required: bool,
	auth_token: Option<String>,
	runtime: Option<SharedRuntime>,
}

impl Default for AdminConfigurator {
	fn default() -> Self {
		Self::new()
	}
}

impl AdminConfigurator {
	pub fn new() -> Self {
		Self {
			bind_addr: "127.0.0.1:9090".to_string(),
			max_connections: 1_000,
			request_timeout: Duration::from_secs(30),
			auth_required: false,
			auth_token: None,
			runtime: None,
		}
	}

	pub fn bind_addr(mut self, addr: impl Into<String>) -> Self {
		self.bind_addr = addr.into();
		self
	}

	pub fn max_connections(mut self, max: usize) -> Self {
		self.max_connections = max;
		self
	}

	pub fn request_timeout(mut self, timeout: Duration) -> Self {
		self.request_timeout = timeout;
		self
	}

	pub fn with_auth(mut self, token: String) -> Self {
		self.auth_required = true;
		self.auth_token = Some(token);
		self
	}

	pub fn runtime(mut self, runtime: SharedRuntime) -> Self {
		self.runtime = Some(runtime);
		self
	}

	pub(crate) fn configure(self) -> AdminConfig {
		AdminConfig {
			bind_addr: self.bind_addr,
			max_connections: self.max_connections,
			request_timeout: self.request_timeout,
			auth_required: self.auth_required,
			auth_token: self.auth_token,
			runtime: self.runtime,
		}
	}
}

#[derive(Clone)]
pub struct AdminConfig {
	pub bind_addr: String,

	pub max_connections: usize,

	pub request_timeout: Duration,

	pub auth_required: bool,

	pub auth_token: Option<String>,

	pub runtime: Option<SharedRuntime>,
}

impl fmt::Debug for AdminConfig {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("AdminConfig")
			.field("bind_addr", &self.bind_addr)
			.field("max_connections", &self.max_connections)
			.field("request_timeout", &self.request_timeout)
			.field("auth_required", &self.auth_required)
			.field("auth_token", &self.auth_token.as_ref().map(|_| "****"))
			.finish()
	}
}

impl Default for AdminConfig {
	fn default() -> Self {
		AdminConfigurator::new().configure()
	}
}
