// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::ioc::IocContainer;
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_sub_api::{Subsystem, SubsystemFactory};

use crate::{config::ServerConfig, subsystem::ServerSubsystem};

/// Factory for creating server subsystem instances
pub struct ServerSubsystemFactory {
	config: ServerConfig,
}

impl ServerSubsystemFactory {
	/// Create a new server subsystem factory with the given configuration
	pub fn new(config: ServerConfig) -> Self {
		Self {
			config,
		}
	}
}

impl SubsystemFactory<StandardCommandTransaction> for ServerSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> reifydb_type::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let subsystem = ServerSubsystem::new(self.config, engine);
		Ok(Box::new(subsystem))
	}
}
