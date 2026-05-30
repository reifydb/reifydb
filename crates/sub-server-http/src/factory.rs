// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{sync::Arc, time::Duration};

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
use reifydb_value::Result;
use tokio::runtime::Handle;

use crate::subsystem::HttpSubsystem;

pub struct HttpConfigurator {
	bind_addr: Option<String>,
	admin_bind_addr: Option<String>,
	max_connections: usize,
	query_timeout: Duration,
	request_timeout: Duration,
	spawner: Option<ActorSpawner>,
	clock: Option<Clock>,
	rng: Option<Rng>,
	handle: Option<Handle>,
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
			spawner: None,
			clock: None,
			rng: None,
			handle: None,
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

	pub fn spawner(mut self, spawner: ActorSpawner) -> Self {
		self.spawner = Some(spawner);
		self
	}

	pub fn clock(mut self, clock: Clock) -> Self {
		self.clock = Some(clock);
		self
	}

	pub fn rng(mut self, rng: Rng) -> Self {
		self.rng = Some(rng);
		self
	}

	pub fn handle(mut self, handle: Handle) -> Self {
		self.handle = Some(handle);
		self
	}

	pub(crate) fn configure(self) -> HttpConfig {
		HttpConfig {
			bind_addr: self.bind_addr,
			admin_bind_addr: self.admin_bind_addr,
			max_connections: self.max_connections,
			query_timeout: self.query_timeout,
			request_timeout: self.request_timeout,
			spawner: self.spawner,
			clock: self.clock,
			rng: self.rng,
			handle: self.handle,
		}
	}
}

#[derive(Clone)]
pub struct HttpConfig {
	pub bind_addr: Option<String>,

	pub admin_bind_addr: Option<String>,

	pub max_connections: usize,

	pub query_timeout: Duration,

	pub request_timeout: Duration,

	pub spawner: Option<ActorSpawner>,

	pub clock: Option<Clock>,

	pub rng: Option<Rng>,

	pub handle: Option<Handle>,
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
		let interceptors = ioc.resolve::<RequestInterceptorChain>().unwrap_or_default();

		let spawner = match config.spawner {
			Some(spawner) => spawner,
			None => ioc.resolve::<ActorSpawner>()?,
		};
		let clock = match config.clock {
			Some(clock) => clock,
			None => ioc.resolve::<Clock>()?,
		};
		let rng = match config.rng {
			Some(rng) => rng,
			None => ioc.resolve::<Rng>()?,
		};
		let handle = match config.handle {
			Some(handle) => handle,
			None => ioc.resolve::<Handle>()?,
		};

		let query_config = StateConfig::new()
			.query_timeout(config.query_timeout)
			.request_timeout(config.request_timeout)
			.max_connections(config.max_connections);

		let auth_service = AuthService::new(
			Arc::new(engine.clone()),
			Arc::new(AuthenticationRegistry::new(clock.clone())),
			rng.clone(),
			clock.clone(),
			AuthServiceConfig::default(),
		);

		let state = AppState::new(spawner, engine, auth_service, query_config, interceptors, clock, rng);
		let subsystem =
			HttpSubsystem::new(config.bind_addr.clone(), config.admin_bind_addr.clone(), state, handle)?;

		Ok(Box::new(subsystem))
	}
}
