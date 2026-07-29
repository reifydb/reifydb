// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::util::ioc::IocContainer;
use reifydb_engine::engine::StandardEngine;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_transaction::interceptor::builder::InterceptorBuilder;
use reifydb_value::Result;

use super::FlowSubsystem;
use crate::builder::{FlowConfig, FlowConfigurator};

pub struct FlowSubsystemFactory {
	config: FlowConfig,
}

impl FlowSubsystemFactory {
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

impl Default for FlowSubsystemFactory {
	fn default() -> Self {
		Self::new()
	}
}

impl SubsystemFactory for FlowSubsystemFactory {
	fn provide_interceptors(&self, builder: InterceptorBuilder, _ioc: &IocContainer) -> InterceptorBuilder {
		builder
	}

	fn publish_catalog(&self, ioc: &IocContainer) -> Result<()> {
		let engine = ioc.resolve::<StandardEngine>()?;
		FlowSubsystem::publish_operator_catalog(&self.config, &engine);
		Ok(())
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		Ok(Box::new(FlowSubsystem::new(self.config, engine, ioc)?))
	}
}
