// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Factory for creating admin subsystem instances.

use reifydb_core::{SharedRuntime, ioc::IocContainer};
use reifydb_engine::StandardEngine;
use reifydb_sub_api::{Subsystem, SubsystemFactory};

use crate::{config::AdminConfig, state::AdminState, subsystem::AdminSubsystem};

/// Factory for creating admin subsystem instances.
pub struct AdminSubsystemFactory {
	config: AdminConfig,
}

impl AdminSubsystemFactory {
	/// Create a new admin subsystem factory with the given configuration.
	pub fn new(config: AdminConfig) -> Self {
		Self {
			config,
		}
	}
}

impl SubsystemFactory for AdminSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> reifydb_core::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let ioc_runtime = ioc.resolve::<SharedRuntime>()?;

		// Create admin state from config
		let state = AdminState::new(
			engine,
			self.config.max_connections,
			self.config.request_timeout,
			self.config.auth_required,
			self.config.auth_token.clone(),
		);

		let subsystem = AdminSubsystem::new(self.config.bind_addr.clone(), state, self.config.runtime.unwrap_or(ioc_runtime));

		Ok(Box::new(subsystem))
	}
}
