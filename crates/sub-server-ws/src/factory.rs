// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! Factory for creating WebSocket subsystem instances.

use std::{sync::Arc, time::Duration};

use reifydb_core::ioc::IocContainer;
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_sub_api::{Subsystem, SubsystemFactory};
use reifydb_sub_server::{AppState, QueryConfig, SharedRuntime};

use crate::WsSubsystem;

/// Configuration for the WebSocket server subsystem.
#[derive(Clone)]
pub struct WsConfig {
	/// Address to bind the WebSocket server to (e.g., "0.0.0.0:8091").
	pub bind_addr: String,
	/// Maximum number of concurrent connections.
	pub max_connections: usize,
	/// Timeout for query execution.
	pub query_timeout: Duration,
	/// Maximum WebSocket frame size in bytes.
	pub max_frame_size: usize,
	/// Optional shared runtime. If not provided, a default one will be created.
	pub runtime: Option<Arc<SharedRuntime>>,
}

impl std::fmt::Debug for WsConfig {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("WsConfig")
			.field("bind_addr", &self.bind_addr)
			.field("max_connections", &self.max_connections)
			.field("query_timeout", &self.query_timeout)
			.field("max_frame_size", &self.max_frame_size)
			.field("runtime", &self.runtime.as_ref().map(|_| "SharedRuntime"))
			.finish()
	}
}

impl Default for WsConfig {
	fn default() -> Self {
		Self {
			bind_addr: "0.0.0.0:8090".to_string(),
			max_connections: 10_000,
			query_timeout: Duration::from_secs(30),
			max_frame_size: 16 << 20, // 16MB
			runtime: None,
		}
	}
}

impl WsConfig {
	/// Create a new WebSocket config with default values.
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

	/// Set the maximum frame size.
	pub fn max_frame_size(mut self, size: usize) -> Self {
		self.max_frame_size = size;
		self
	}

	/// Set the shared runtime.
	pub fn runtime(mut self, runtime: Arc<SharedRuntime>) -> Self {
		self.runtime = Some(runtime);
		self
	}
}

/// Factory for creating WebSocket subsystem instances.
pub struct WsSubsystemFactory {
	config: WsConfig,
}

impl WsSubsystemFactory {
	/// Create a new WebSocket subsystem factory with the given configuration.
	pub fn new(config: WsConfig) -> Self {
		Self {
			config,
		}
	}
}

impl SubsystemFactory<StandardCommandTransaction> for WsSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> reifydb_type::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;

		// Use provided runtime or create a default one
		let runtime = self.config.runtime.unwrap_or_else(|| Arc::new(SharedRuntime::default()));

		let query_config = QueryConfig::new()
			.query_timeout(self.config.query_timeout)
			.max_connections(self.config.max_connections);

		let state = AppState::new(engine, query_config);
		let subsystem = WsSubsystem::with_runtime(self.config.bind_addr.clone(), state, runtime);

		Ok(Box::new(subsystem))
	}
}
