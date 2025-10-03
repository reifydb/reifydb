// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::ioc::IocContainer;
use reifydb_engine::StandardCommandTransaction;
use reifydb_sub_api::{Subsystem, SubsystemFactory};

use super::LoggingBuilder;

/// Configuration function for the logging subsystem
pub type LoggingConfigurator = Box<dyn FnOnce(LoggingBuilder) -> LoggingBuilder + Send>;

/// Factory for creating LoggingSubsystem instances
pub struct LoggingSubsystemFactory {
	configurator: Option<LoggingConfigurator>,
}

impl LoggingSubsystemFactory {
	/// Create a new factory with default configuration
	pub fn new() -> Self {
		Self {
			configurator: None,
		}
	}

	/// Create a factory with a custom configurator
	pub fn with_configurator<F>(configurator: F) -> Self
	where
		F: FnOnce(LoggingBuilder) -> LoggingBuilder + Send + 'static,
	{
		Self {
			configurator: Some(Box::new(configurator)),
		}
	}
}

impl Default for LoggingSubsystemFactory {
	fn default() -> Self {
		Self::new()
	}
}

impl SubsystemFactory<StandardCommandTransaction> for LoggingSubsystemFactory {
	fn provide_interceptors(
		&self,
		builder: reifydb_core::interceptor::StandardInterceptorBuilder<StandardCommandTransaction>,
		_ioc: &IocContainer,
	) -> reifydb_core::interceptor::StandardInterceptorBuilder<StandardCommandTransaction> {
		// Logging subsystem doesn't need any special interceptors
		builder
	}

	fn create(self: Box<Self>, _ioc: &IocContainer) -> crate::Result<Box<dyn Subsystem>> {
		let builder = if let Some(configurator) = self.configurator {
			configurator(LoggingBuilder::new())
		} else {
			LoggingBuilder::default()
		};
		Ok(Box::new(builder.build()))
	}
}
