// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::util::ioc::IocContainer;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::Result;

use crate::{
	engine::StandardEngine,
	flow::{
		builder::{FlowConfig, FlowConfigurator},
		transactional_engine::TransactionalFlowEngine,
	},
};

pub struct TransactionalFlowSubsystemFactory {
	config: FlowConfig,
}

impl TransactionalFlowSubsystemFactory {
	pub fn new() -> Self {
		Self {
			config: FlowConfigurator::new().configure(),
		}
	}

	pub fn with_configurator<F>(configurator: F) -> Self
	where
		F: FnOnce(FlowConfigurator) -> FlowConfigurator + Send + 'static,
	{
		Self {
			config: configurator(FlowConfigurator::new()).configure(),
		}
	}
}

impl Default for TransactionalFlowSubsystemFactory {
	fn default() -> Self {
		Self::new()
	}
}

impl SubsystemFactory for TransactionalFlowSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		Ok(Box::new(TransactionalFlowEngine::new(self.config, engine)?))
	}
}
