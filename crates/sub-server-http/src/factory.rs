// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Factory for creating HTTP subsystem instances.

use std::time::Duration;

use reifydb_core::util::ioc::IocContainer;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_sub_server::state::{AppState, StateConfig};
use reifydb_type::Result;

use crate::subsystem::HttpSubsystem;

/// Configuration for the HTTP server subsystem.
#[derive(Clone, Debug)]
pub struct HttpConfig {
	/// Address to bind the HTTP server to (e.g., "0.0.0.0:8091").
	pub bind_addr: String,
	/// Maximum number of concurrent connections.
	pub max_connections: usize,
	/// Timeout for query execution.
	pub query_timeout: Duration,
	/// Timeout for entire request lifecycle.
	pub request_timeout: Duration,
	/// Optional shared runtime .
	pub runtime: Option<SharedRuntime>,
}

impl Default for HttpConfig {
	fn default() -> Self {
		Self {
			bind_addr: "0.0.0.0:8091".to_string(),
			max_connections: 10_000,
			query_timeout: Duration::from_secs(30),
			request_timeout: Duration::from_secs(60),
			runtime: None,
		}
	}
}

impl HttpConfig {
	/// Create a new HTTP config with default values.
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

	/// Set the query timeout.
	pub fn query_timeout(mut self, timeout: Duration) -> Self {
		self.query_timeout = timeout;
		self
	}

	/// Set the request timeout.
	pub fn request_timeout(mut self, timeout: Duration) -> Self {
		self.request_timeout = timeout;
		self
	}

	/// Set the shared runtime.
	pub fn runtime(mut self, runtime: SharedRuntime) -> Self {
		self.runtime = Some(runtime);
		self
	}
}

/// Factory for creating HTTP subsystem instances.
pub struct HttpSubsystemFactory {
	config: HttpConfig,
}

impl HttpSubsystemFactory {
	/// Create a new HTTP subsystem factory with the given configuration.
	pub fn new(config: HttpConfig) -> Self {
		Self {
			config,
		}
	}
}

impl SubsystemFactory for HttpSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let ioc_runtime = ioc.resolve::<SharedRuntime>()?;

		let query_config = StateConfig::new()
			.query_timeout(self.config.query_timeout)
			.request_timeout(self.config.request_timeout)
			.max_connections(self.config.max_connections);

		let runtime = self.config.runtime.unwrap_or(ioc_runtime);

		let state = AppState::new(runtime.actor_system(), engine, query_config);
		let subsystem = HttpSubsystem::new(self.config.bind_addr.clone(), state, runtime);

		Ok(Box::new(subsystem))
	}
}
