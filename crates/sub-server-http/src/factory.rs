// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
use reifydb_type::Result;

use crate::subsystem::HttpSubsystem;

pub struct HttpConfigurator {
	bind_addr: Option<String>,
	admin_bind_addr: Option<String>,
	max_connections: usize,
	query_timeout: Duration,
	request_timeout: Duration,
	runtime: Option<SharedRuntime>,
}

impl Default for HttpConfigurator {
	fn default() -> Self {
		Self::new()
	}
}

impl HttpConfigurator {
	pub fn new() -> Self {
		Self {
			bind_addr: None,
			admin_bind_addr: None,
			max_connections: 10_000,
			query_timeout: Duration::from_secs(30),
			request_timeout: Duration::from_secs(60),
			runtime: None,
		}
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

	pub fn request_timeout(mut self, timeout: Duration) -> Self {
		self.request_timeout = timeout;
		self
	}

	pub fn runtime(mut self, runtime: SharedRuntime) -> Self {
		self.runtime = Some(runtime);
		self
	}

	pub(crate) fn configure(self) -> HttpConfig {
		HttpConfig {
			bind_addr: self.bind_addr,
			admin_bind_addr: self.admin_bind_addr,
			max_connections: self.max_connections,
			query_timeout: self.query_timeout,
			request_timeout: self.request_timeout,
			runtime: self.runtime,
		}
	}
}

#[derive(Clone, Debug)]
pub struct HttpConfig {
	pub bind_addr: Option<String>,

	pub admin_bind_addr: Option<String>,

	pub max_connections: usize,

	pub query_timeout: Duration,

	pub request_timeout: Duration,

	pub runtime: Option<SharedRuntime>,
}

impl Default for HttpConfig {
	fn default() -> Self {
		HttpConfigurator::new().configure()
	}
}

pub struct HttpSubsystemFactory {
	config_fn: Box<dyn FnOnce() -> HttpConfig + Send>,
}

impl HttpSubsystemFactory {
	pub fn new<F>(configurator: F) -> Self
	where
		F: FnOnce(HttpConfigurator) -> HttpConfigurator + Send + 'static,
	{
		Self {
			config_fn: Box::new(move || configurator(HttpConfigurator::new()).configure()),
		}
	}
}

impl SubsystemFactory for HttpSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let config = (self.config_fn)();

		let engine = ioc.resolve::<StandardEngine>()?;
		let ioc_runtime = ioc.resolve::<SharedRuntime>()?;
		let interceptors = ioc.resolve::<RequestInterceptorChain>().unwrap_or_default();

		let query_config = StateConfig::new()
			.query_timeout(config.query_timeout)
			.request_timeout(config.request_timeout)
			.max_connections(config.max_connections);

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
		let subsystem =
			HttpSubsystem::new(config.bind_addr.clone(), config.admin_bind_addr.clone(), state, runtime);

		Ok(Box::new(subsystem))
	}
}
