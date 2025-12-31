// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Factory for creating admin subsystem instances.

use async_trait::async_trait;
use reifydb_core::ioc::IocContainer;
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
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

#[async_trait]
impl SubsystemFactory<StandardCommandTransaction> for AdminSubsystemFactory {
	async fn create(self: Box<Self>, ioc: &IocContainer) -> reifydb_core::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;

		// Create admin state from config
		let state = AdminState::new(
			engine,
			self.config.max_connections,
			self.config.request_timeout,
			self.config.auth_required,
			self.config.auth_token.clone(),
		);

		let subsystem = AdminSubsystem::new(self.config.bind_addr.clone(), state);

		Ok(Box::new(subsystem))
	}
}
