// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::util::ioc::IocContainer;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_type::Result;

use crate::{
	config::{AdminConfig, AdminConfigurator},
	state::AdminState,
	subsystem::AdminSubsystem,
};

/// Factory for creating admin subsystem instances.
pub struct AdminSubsystemFactory {
	config_fn: Box<dyn FnOnce() -> AdminConfig + Send>,
}

impl AdminSubsystemFactory {
	/// Create a new admin subsystem factory with the given configurator.
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
		let ioc_runtime = ioc.resolve::<SharedRuntime>()?;

		let config = (self.config_fn)();

		let runtime = config.runtime.as_ref().unwrap_or(&ioc_runtime);
		let actor_system = runtime.actor_system();
		let clock = runtime.clock().clone();

		// Create admin state from config
		let state = AdminState::new(
			engine,
			config.max_connections,
			config.request_timeout,
			config.auth_required,
			config.auth_token.clone(),
			clock,
			actor_system,
		);

		let subsystem =
			AdminSubsystem::new(config.bind_addr.clone(), state, config.runtime.unwrap_or(ioc_runtime));

		Ok(Box::new(subsystem))
	}
}
