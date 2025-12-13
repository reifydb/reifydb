// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! Factory for creating HTTP subsystem instances.

use std::sync::Arc;
use std::time::Duration;

use reifydb_core::ioc::IocContainer;
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_sub_api::{Subsystem, SubsystemFactory};
use reifydb_sub_server::{AppState, QueryConfig, SharedRuntime};

use crate::HttpSubsystem;

/// Configuration for the HTTP server subsystem.
#[derive(Clone)]
pub struct HttpConfig {
	/// Address to bind the HTTP server to (e.g., "0.0.0.0:8090").
	pub bind_addr: String,
	/// Maximum number of concurrent connections.
	pub max_connections: usize,
	/// Timeout for query execution.
	pub query_timeout: Duration,
	/// Timeout for entire request lifecycle.
	pub request_timeout: Duration,
	/// Optional shared runtime. If not provided, a default one will be created.
	pub runtime: Option<Arc<SharedRuntime>>,
}

impl std::fmt::Debug for HttpConfig {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("HttpConfig")
			.field("bind_addr", &self.bind_addr)
			.field("max_connections", &self.max_connections)
			.field("query_timeout", &self.query_timeout)
			.field("request_timeout", &self.request_timeout)
			.field("runtime", &self.runtime.as_ref().map(|_| "SharedRuntime"))
			.finish()
	}
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
	pub fn runtime(mut self, runtime: Arc<SharedRuntime>) -> Self {
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
		Self { config }
	}
}

impl SubsystemFactory<StandardCommandTransaction> for HttpSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> reifydb_type::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;

		// Use provided runtime or create a default one
		let runtime = self.config.runtime.unwrap_or_else(|| Arc::new(SharedRuntime::default()));

		let query_config = QueryConfig::new()
			.query_timeout(self.config.query_timeout)
			.request_timeout(self.config.request_timeout)
			.max_connections(self.config.max_connections);

		let state = AppState::new(engine, query_config);
		let subsystem = HttpSubsystem::with_runtime(self.config.bind_addr.clone(), state, runtime);

		Ok(Box::new(subsystem))
	}
}
