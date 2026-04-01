// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Factory for creating WebSocket subsystem instances.

use std::{sync::Arc, time::Duration};

use reifydb_auth::{
	registry::AuthenticationRegistry,
	service::{AuthService, AuthServiceConfig},
};
use reifydb_core::util::ioc::IocContainer;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_sub_server::{
	interceptor::RequestInterceptorChain,
	state::{AppState, StateConfig},
};
use reifydb_sub_subscription::store::SubscriptionStore;
use reifydb_type::Result;

use crate::subsystem::WsSubsystem;

/// Configurator for the WebSocket server subsystem.
pub struct WsConfigurator {
	/// Address to bind the WebSocket server to (e.g., "0.0.0.0:8090").
	bind_addr: Option<String>,
	/// Address to bind the admin WebSocket server to (e.g., "127.0.0.1:9090").
	/// When set, admin operations are only available on this port.
	/// When not set, admin operations are not available.
	admin_bind_addr: Option<String>,
	/// Maximum number of concurrent connections.
	max_connections: usize,
	/// Timeout for query execution.
	query_timeout: Duration,
	/// Maximum WebSocket frame size in bytes.
	max_frame_size: usize,
	/// Optional shared runtime.
	runtime: Option<SharedRuntime>,
	/// Subscription polling interval (how often to check for new data).
	poll_interval: Duration,
	/// Maximum rows to read per subscription per poll cycle.
	poll_batch_size: usize,
}

impl Default for WsConfigurator {
	fn default() -> Self {
		Self {
			bind_addr: None,
			admin_bind_addr: None,
			max_connections: 10_000,
			query_timeout: Duration::from_secs(30),
			max_frame_size: 16 << 20, // 16MB
			runtime: None,
			poll_interval: Duration::from_millis(10), // Poll every 10ms
			poll_batch_size: 100,                     // Read up to 100 rows per poll
		}
	}
}

impl WsConfigurator {
	/// Create a new WebSocket configurator with default values.
	pub fn new() -> Self {
		Self::default()
	}

	/// Set the bind address.
	pub fn bind_addr(mut self, addr: impl Into<String>) -> Self {
		self.bind_addr = Some(addr.into());
		self
	}

	/// Set the admin bind address.
	/// When set, admin operations are served on this separate port.
	pub fn admin_bind_addr(mut self, addr: impl Into<String>) -> Self {
		self.admin_bind_addr = Some(addr.into());
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

	/// Consume the configurator and produce an immutable config.
	pub(crate) fn configure(self) -> WsConfig {
		WsConfig {
			bind_addr: self.bind_addr,
			admin_bind_addr: self.admin_bind_addr,
			max_connections: self.max_connections,
			query_timeout: self.query_timeout,
			max_frame_size: self.max_frame_size,
			runtime: self.runtime,
			poll_interval: self.poll_interval,
			poll_batch_size: self.poll_batch_size,
		}
	}
}

/// Configuration for the WebSocket server subsystem.
#[derive(Clone, Debug)]
pub struct WsConfig {
	/// Address to bind the WebSocket server to (e.g., "0.0.0.0:8090").
	pub bind_addr: Option<String>,
	/// Address to bind the admin WebSocket server to (e.g., "127.0.0.1:9090").
	/// When set, admin operations are only available on this port.
	/// When not set, admin operations are not available.
	pub admin_bind_addr: Option<String>,
	/// Maximum number of concurrent connections.
	pub max_connections: usize,
	/// Timeout for query execution.
	pub query_timeout: Duration,
	/// Maximum WebSocket frame size in bytes.
	pub max_frame_size: usize,
	/// Optional shared runtime.
	pub runtime: Option<SharedRuntime>,
	/// Subscription polling interval (how often to check for new data).
	pub poll_interval: Duration,
	/// Maximum rows to read per subscription per poll cycle.
	pub poll_batch_size: usize,
}

impl Default for WsConfig {
	fn default() -> Self {
		WsConfigurator::new().configure()
	}
}

/// Factory for creating WebSocket subsystem instances.
pub struct WsSubsystemFactory {
	config_fn: Box<dyn FnOnce() -> WsConfig + Send>,
}

impl WsSubsystemFactory {
	/// Create a new WebSocket subsystem factory with a configurator closure.
	pub fn new<F>(configurator: F) -> Self
	where
		F: FnOnce(WsConfigurator) -> WsConfigurator + Send + 'static,
	{
		Self {
			config_fn: Box::new(move || configurator(WsConfigurator::new()).configure()),
		}
	}
}

impl SubsystemFactory for WsSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let config = (self.config_fn)();

		let engine = ioc.resolve::<StandardEngine>()?;
		let ioc_runtime = ioc.resolve::<SharedRuntime>()?;

		let query_config =
			StateConfig::new().query_timeout(config.query_timeout).max_connections(config.max_connections);

		let interceptors = ioc.resolve::<RequestInterceptorChain>().unwrap_or_default();
		let runtime = config.runtime.unwrap_or(ioc_runtime);

		let auth_service = AuthService::new(
			Arc::new(engine.clone()),
			Arc::new(AuthenticationRegistry::new(runtime.clock().clone())),
			runtime.rng().clone(),
			runtime.clock().clone(),
			AuthServiceConfig::default(),
		);

		let state = AppState::new(
			runtime.actor_system(),
			engine,
			auth_service,
			query_config,
			interceptors,
			runtime.clock().clone(),
			runtime.rng().clone(),
		);
		let subscription_store = ioc.resolve::<Arc<SubscriptionStore>>().ok();
		let subsystem = WsSubsystem::new(
			config.bind_addr.clone(),
			config.admin_bind_addr.clone(),
			state,
			runtime,
			config.poll_interval,
			config.poll_batch_size,
			subscription_store,
		);

		Ok(Box::new(subsystem))
	}
}
