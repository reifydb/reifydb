// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Factory for creating WebSocket subsystem instances.

use std::time::Duration;

use async_trait::async_trait;
use reifydb_core::ioc::IocContainer;
use reifydb_engine::StandardEngine;
use reifydb_sub_api::{Subsystem, SubsystemFactory};
use reifydb_sub_server::{AppState, QueryConfig};

use crate::WsSubsystem;

/// Configuration for the WebSocket server subsystem.
#[derive(Clone, Debug)]
pub struct WsConfig {
	/// Address to bind the WebSocket server to (e.g., "0.0.0.0:8090").
	pub bind_addr: String,
	/// Maximum number of concurrent connections.
	pub max_connections: usize,
	/// Timeout for query execution.
	pub query_timeout: Duration,
	/// Maximum WebSocket frame size in bytes.
	pub max_frame_size: usize,
}

impl Default for WsConfig {
	fn default() -> Self {
		Self {
			bind_addr: "0.0.0.0:8090".to_string(),
			max_connections: 10_000,
			query_timeout: Duration::from_secs(30),
			max_frame_size: 16 << 20, // 16MB
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

#[async_trait]
impl SubsystemFactory for WsSubsystemFactory {
	async fn create(self: Box<Self>, ioc: &IocContainer) -> reifydb_core::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;

		let query_config = QueryConfig::new()
			.query_timeout(self.config.query_timeout)
			.max_connections(self.config.max_connections);

		let state = AppState::new(engine, query_config);
		let subsystem = WsSubsystem::new(self.config.bind_addr.clone(), state);

		Ok(Box::new(subsystem))
	}
}
