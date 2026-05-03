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
use reifydb_sub_subscription::store::SubscriptionStore;
use reifydb_type::Result;

use crate::subsystem::GrpcSubsystem;

pub struct GrpcConfigurator {
	bind_addr: Option<String>,
	admin_bind_addr: Option<String>,
	max_connections: usize,
	query_timeout: Duration,
	request_timeout: Duration,
	runtime: Option<SharedRuntime>,
	poll_interval: Duration,
	poll_batch_size: usize,
}

impl Default for GrpcConfigurator {
	fn default() -> Self {
		Self::new()
	}
}

impl GrpcConfigurator {
	pub fn new() -> Self {
		Self {
			bind_addr: None,
			admin_bind_addr: None,
			max_connections: 10_000,
			query_timeout: Duration::from_secs(30),
			request_timeout: Duration::from_secs(60),
			runtime: None,
			poll_interval: Duration::from_millis(10),
			poll_batch_size: 100,
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

	pub fn poll_interval(mut self, interval: Duration) -> Self {
		self.poll_interval = interval;
		self
	}

	pub fn poll_batch_size(mut self, size: usize) -> Self {
		self.poll_batch_size = size;
		self
	}

	pub(crate) fn configure(self) -> GrpcConfig {
		GrpcConfig {
			bind_addr: self.bind_addr,
			admin_bind_addr: self.admin_bind_addr,
			max_connections: self.max_connections,
			query_timeout: self.query_timeout,
			request_timeout: self.request_timeout,
			runtime: self.runtime,
			poll_interval: self.poll_interval,
			poll_batch_size: self.poll_batch_size,
		}
	}
}

#[derive(Clone, Debug)]
pub struct GrpcConfig {
	pub bind_addr: Option<String>,

	pub admin_bind_addr: Option<String>,
	pub max_connections: usize,
	pub query_timeout: Duration,
	pub request_timeout: Duration,
	pub runtime: Option<SharedRuntime>,
	pub poll_interval: Duration,
	pub poll_batch_size: usize,
}

impl Default for GrpcConfig {
	fn default() -> Self {
		GrpcConfigurator::new().configure()
	}
}

pub struct GrpcSubsystemFactory {
	config_fn: Box<dyn FnOnce() -> GrpcConfig + Send>,
}

impl GrpcSubsystemFactory {
	pub fn new<F>(configurator: F) -> Self
	where
		F: FnOnce(GrpcConfigurator) -> GrpcConfigurator + Send + 'static,
	{
		Self {
			config_fn: Box::new(move || configurator(GrpcConfigurator::new()).configure()),
		}
	}
}

impl SubsystemFactory for GrpcSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let ioc_runtime = ioc.resolve::<SharedRuntime>()?;

		let config = (self.config_fn)();

		let query_config = StateConfig::new()
			.query_timeout(config.query_timeout)
			.request_timeout(config.request_timeout)
			.max_connections(config.max_connections);

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
		let subsystem = GrpcSubsystem::new(
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
