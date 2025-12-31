// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use async_trait::async_trait;
use reifydb_core::ioc::IocContainer;
use reifydb_engine::StandardCommandTransaction;
use reifydb_sub_api::{Subsystem, SubsystemFactory};

use super::TracingBuilder;

/// Configuration function for the tracing subsystem
pub type TracingConfigurator = Box<dyn FnOnce(TracingBuilder) -> TracingBuilder + Send>;

/// Factory for creating TracingSubsystem instances
pub struct TracingSubsystemFactory {
	configurator: Option<TracingConfigurator>,
}

impl TracingSubsystemFactory {
	/// Create a new factory with default configuration
	pub fn new() -> Self {
		Self {
			configurator: None,
		}
	}

	/// Create a factory with a custom configurator
	pub fn with_configurator<F>(configurator: F) -> Self
	where
		F: FnOnce(TracingBuilder) -> TracingBuilder + Send + 'static,
	{
		Self {
			configurator: Some(Box::new(configurator)),
		}
	}
}

impl Default for TracingSubsystemFactory {
	fn default() -> Self {
		Self::new()
	}
}

#[async_trait]
impl SubsystemFactory<StandardCommandTransaction> for TracingSubsystemFactory {
	fn provide_interceptors(
		&self,
		builder: reifydb_core::interceptor::StandardInterceptorBuilder<StandardCommandTransaction>,
		_ioc: &IocContainer,
	) -> reifydb_core::interceptor::StandardInterceptorBuilder<StandardCommandTransaction> {
		// Tracing subsystem doesn't need any special interceptors
		builder
	}

	async fn create(self: Box<Self>, _ioc: &IocContainer) -> reifydb_core::Result<Box<dyn Subsystem>> {
		let builder = if let Some(configurator) = self.configurator {
			configurator(TracingBuilder::new())
		} else {
			TracingBuilder::default()
		};
		Ok(Box::new(builder.build()))
	}
}
