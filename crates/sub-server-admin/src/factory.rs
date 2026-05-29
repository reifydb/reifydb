// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::util::ioc::IocContainer;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{actor::system::ActorSpawner, context::clock::Clock};
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::Result;
use tokio::runtime::Handle;

use crate::{
	config::{AdminConfig, AdminConfigurator},
	state::AdminState,
	subsystem::AdminSubsystem,
};

pub struct AdminSubsystemFactory {
	config_fn: Box<dyn FnOnce() -> AdminConfig + Send>,
}

impl AdminSubsystemFactory {
	pub fn new<F>(configurator: F) -> Self
	where
		F: FnOnce(AdminConfigurator) -> AdminConfigurator + Send + 'static,
	{
		Self {
			config_fn: Box::new(move || configurator(AdminConfigurator::new()).configure()),
		}
	}
}

impl SubsystemFactory for AdminSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let ioc_spawner = ioc.resolve::<ActorSpawner>()?;
		let ioc_clock = ioc.resolve::<Clock>()?;
		let ioc_handle = ioc.resolve::<Handle>()?;

		let config = (self.config_fn)();

		let spawner = config.spawner.clone().unwrap_or(ioc_spawner);
		let clock = config.clock.clone().unwrap_or(ioc_clock);
		let handle = config.handle.clone().unwrap_or(ioc_handle);

		let state = AdminState::new(
			engine,
			config.max_connections,
			config.request_timeout,
			config.auth_required,
			config.auth_token.clone(),
			clock,
			spawner,
		);

		let subsystem = AdminSubsystem::new(config.bind_addr.clone(), state, handle);

		Ok(Box::new(subsystem))
	}
}
