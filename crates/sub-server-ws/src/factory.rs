// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Factory for creating WebSocket subsystem instances.

use std::time::Duration;

use reifydb_core::{runtime::SharedRuntime, util::ioc::IocContainer};
use reifydb_engine::engine::StandardEngine;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_sub_server::state::{AppState, StateConfig};

use crate::subsystem::WsSubsystem;

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
	/// Optional shared runtime .
	pub runtime: Option<SharedRuntime>,
	/// Subscription polling interval (how often to check for new data).
	pub poll_interval: Duration,
	/// Maximum rows to read per subscription per poll cycle.
	pub poll_batch_size: usize,
}

impl Default for WsConfig {
	fn default() -> Self {
		Self {
			bind_addr: "0.0.0.0:8090".to_string(),
			max_connections: 10_000,
			query_timeout: Duration::from_secs(30),
			max_frame_size: 16 << 20, // 16MB
			runtime: None,
			poll_interval: Duration::from_millis(250), // Poll every 250ms
			poll_batch_size: 100,                      // Read up to 100 rows per poll
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
	pub fn runtime(mut self, runtime: SharedRuntime) -> Self {
		self.runtime = Some(runtime);
		self
	}

	/// Set the subscription polling interval.
	pub fn poll_interval(mut self, interval: Duration) -> Self {
		self.poll_interval = interval;
		self
	}

	/// Set the subscription polling batch size.
	pub fn poll_batch_size(mut self, size: usize) -> Self {
		self.poll_batch_size = size;
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

impl SubsystemFactory for WsSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> reifydb_type::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let ioc_runtime = ioc.resolve::<SharedRuntime>()?;

		let query_config = StateConfig::new()
			.query_timeout(self.config.query_timeout)
			.max_connections(self.config.max_connections);

		let runtime = self.config.runtime.unwrap_or(ioc_runtime);
		let state = AppState::new(runtime.compute_pool(), engine, query_config);
		let subsystem = WsSubsystem::new(
			self.config.bind_addr.clone(),
			state,
			runtime,
			self.config.poll_interval,
			self.config.poll_batch_size,
		);

		Ok(Box::new(subsystem))
	}
}
