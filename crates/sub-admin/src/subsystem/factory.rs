// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::ioc::IocContainer;
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_sub_api::{Subsystem, SubsystemFactory};

use super::AdminSubsystem;
use crate::config::AdminConfig;

pub struct AdminSubsystemFactory {
	config: AdminConfig,
}

impl AdminSubsystemFactory {
	pub fn new(config: AdminConfig) -> Self {
		Self {
			config,
		}
	}
}

impl SubsystemFactory<StandardCommandTransaction> for AdminSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> reifydb_type::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let subsystem = AdminSubsystem::new(self.config, engine);
		Ok(Box::new(subsystem))
	}
}
