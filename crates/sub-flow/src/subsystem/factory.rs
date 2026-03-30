// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::util::ioc::IocContainer;
use reifydb_engine::engine::StandardEngine;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_transaction::interceptor::builder::InterceptorBuilder;
use reifydb_type::Result;

use super::FlowSubsystem;
use crate::builder::FlowConfigurator;

/// Factory for creating FlowSubsystem with proper interceptor registration
pub struct FlowSubsystemFactory {
	configurator: Option<Box<dyn FnOnce(FlowConfigurator) -> FlowConfigurator + Send>>,
}

impl FlowSubsystemFactory {
	pub fn new() -> Self {
		Self {
			configurator: None,
		}
	}

	pub fn with_configurator<F>(configurator: F) -> Self
	where
		F: FnOnce(FlowConfigurator) -> FlowConfigurator + Send + 'static,
	{
		Self {
			configurator: Some(Box::new(configurator)),
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
		// Independent flow consumer doesn't need interceptors
		builder
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;

		let config = if let Some(configure_fn) = self.configurator {
			configure_fn(FlowConfigurator::new()).configure()
		} else {
			FlowConfigurator::new().configure()
		};

		Ok(Box::new(FlowSubsystem::new(config, engine, ioc)))
	}
}
