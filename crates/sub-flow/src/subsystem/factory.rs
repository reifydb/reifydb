// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{Result, interceptor::StandardInterceptorBuilder, util::ioc::IocContainer};
use reifydb_engine::StandardCommandTransaction;
use reifydb_sub_api::{Subsystem, SubsystemFactory};

use super::FlowSubsystem;
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

impl SubsystemFactory<StandardCommandTransaction> for FlowSubsystemFactory {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<StandardCommandTransaction>,
		_ioc: &IocContainer,
	) -> StandardInterceptorBuilder<StandardCommandTransaction> {
		// Independent flow consumer doesn't need interceptors
		builder
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let builder = if let Some(configurator) = self.configurator {
			configurator(FlowBuilder::new())
		} else {
			FlowBuilder::default()
		};
		let config = builder.build_config();
		Ok(Box::new(FlowSubsystem::new(config, ioc)?))
	}
}
