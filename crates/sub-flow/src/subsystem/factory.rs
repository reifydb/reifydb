// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::util::ioc::IocContainer;
use reifydb_engine::engine::StandardEngine;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_transaction::interceptor::builder::StandardInterceptorBuilder;
use reifydb_type::Result;

use crate::builder::FlowBuilder;

/// Configuration function for the flow subsystem
pub type FlowConfigurator = Box<dyn FnOnce(FlowBuilder) -> FlowBuilder + Send>;

/// Factory for creating FlowSubsystem with proper interceptor registration
pub struct FlowSubsystemFactory {
	configurator: Option<FlowConfigurator>,
}

impl FlowSubsystemFactory {
	pub fn new() -> Self {
		Self {
			configurator: None,
		}
	}

	pub fn with_configurator<F>(configurator: F) -> Self
	where
		F: FnOnce(FlowBuilder) -> FlowBuilder + Send + 'static,
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
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder,
		_ioc: &IocContainer,
	) -> StandardInterceptorBuilder {
		// Independent flow consumer doesn't need interceptors
		builder
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		use super::FlowSubsystem;

		let engine = ioc.resolve::<StandardEngine>()?;

		// Extract full config from builder
		let config = if let Some(configurator) = self.configurator {
			configurator(FlowBuilder::new()).build_config()
		} else {
			FlowBuilder::new().build_config()
		};

		Ok(Box::new(FlowSubsystem::new(config, engine, ioc)))
	}
}
