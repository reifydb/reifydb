// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::{Result, util::ioc::IocContainer};
use reifydb_engine::StandardEngine;
use reifydb_sub_api::{Subsystem, SubsystemFactory};
use reifydb_transaction::interceptor::StandardInterceptorBuilder;

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

#[async_trait]
impl SubsystemFactory for FlowSubsystemFactory {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder,
		_ioc: &IocContainer,
	) -> StandardInterceptorBuilder {
		// Independent flow consumer doesn't need interceptors
		builder
	}

	async fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		use super::FlowSubsystem;

		let engine = ioc.resolve::<StandardEngine>()?;

		// Get operators_dir from config if configurator is present
		let operators_dir = if let Some(configurator) = self.configurator {
			configurator(FlowBuilder::new()).build_config().operators_dir
		} else {
			None
		};

		Ok(Box::new(FlowSubsystem::new(engine, operators_dir, ioc)))
	}
}
