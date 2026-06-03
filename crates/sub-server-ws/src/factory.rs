// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_auth::{
	registry::AuthenticationRegistry,
	service::{AuthService, AuthServiceConfig},
};
use reifydb_core::util::ioc::IocContainer;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::system::ActorSpawner,
	context::{clock::Clock, rng::Rng},
};
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_sub_server::{
	interceptor::RequestInterceptorChain,
	state::{AppState, StateConfig},
};
use reifydb_sub_subscription::store::SubscriptionStore;
use reifydb_value::{Result, value::duration::Duration};
use tokio::runtime::Handle;

use crate::subsystem::WsSubsystem;

pub struct WsConfigurator {
	bind_addr: Option<String>,

	admin_bind_addr: Option<String>,

	max_connections: usize,

	query_timeout: Duration,

	max_frame_size: usize,

	runtime: Option<Handle>,

	poll_batch_size: usize,
}

impl Default for WsConfigurator {
	fn default() -> Self {
		Self {
			bind_addr: None,
			admin_bind_addr: None,
			max_connections: 10_000,
			query_timeout: Duration::from_seconds(30).unwrap(),
			max_frame_size: 16 << 20,
			runtime: None,
			poll_batch_size: 100,
		}
	}
}

impl WsConfigurator {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn bind_addr(mut self, addr: impl Into<String>) -> Self {
		self.bind_addr = Some(addr.into());
		self
	}

	pub fn admin_bind_addr(mut self, addr: impl Into<String>) -> Self {
		self.admin_bind_addr = Some(addr.into());
		self
	}

	pub fn max_connections(mut self, max: usize) -> Self {
		self.max_connections = max;
		self
	}

	pub fn query_timeout(mut self, timeout: Duration) -> Self {
		self.query_timeout = timeout;
		self
	}

	pub fn max_frame_size(mut self, size: usize) -> Self {
		self.max_frame_size = size;
		self
	}

	pub fn runtime(mut self, runtime: Handle) -> Self {
		self.runtime = Some(runtime);
		self
	}

	pub fn poll_batch_size(mut self, size: usize) -> Self {
		self.poll_batch_size = size;
		self
	}

	pub(crate) fn configure(self) -> WsConfig {
		WsConfig {
			bind_addr: self.bind_addr,
			admin_bind_addr: self.admin_bind_addr,
			max_connections: self.max_connections,
			query_timeout: self.query_timeout,
			max_frame_size: self.max_frame_size,
			runtime: self.runtime,
			poll_batch_size: self.poll_batch_size,
		}
	}
}

#[derive(Clone, Debug)]
pub struct WsConfig {
	pub bind_addr: Option<String>,

	pub admin_bind_addr: Option<String>,

	pub max_connections: usize,

	pub query_timeout: Duration,

	pub max_frame_size: usize,

	pub runtime: Option<Handle>,

	pub poll_batch_size: usize,
}

impl Default for WsConfig {
	fn default() -> Self {
		WsConfigurator::new().configure()
	}
}

pub struct WsSubsystemFactory {
	config_fn: Box<dyn FnOnce() -> WsConfig + Send>,
}

impl WsSubsystemFactory {
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
		let spawner = ioc.resolve::<ActorSpawner>()?;
		let clock = ioc.resolve::<Clock>()?;
		let rng = ioc.resolve::<Rng>()?;
		let ioc_handle = ioc.resolve::<Handle>()?;

		let query_config =
			StateConfig::new().query_timeout(config.query_timeout).max_connections(config.max_connections);

		let interceptors = ioc.resolve::<RequestInterceptorChain>().unwrap_or_default();
		let handle = config.runtime.unwrap_or(ioc_handle);

		let auth_service = AuthService::new(
			Arc::new(engine.clone()),
			Arc::new(AuthenticationRegistry::new(clock.clone())),
			rng.clone(),
			clock.clone(),
			AuthServiceConfig::default(),
		);

		let state = AppState::new(
			spawner,
			engine,
			auth_service,
			query_config,
			interceptors,
			clock.clone(),
			rng.clone(),
		);
		let subscription_store = ioc.resolve::<Arc<SubscriptionStore>>().ok();
		let subsystem = WsSubsystem::new(
			config.bind_addr.clone(),
			config.admin_bind_addr.clone(),
			state,
			handle,
			config.poll_batch_size,
			subscription_store,
		)?;

		Ok(Box::new(subsystem))
	}
}
