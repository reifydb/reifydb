// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_core::{
	interface::{
		Transaction,
		subsystem::{Subsystem, SubsystemFactory},
	},
	ioc::IocContainer,
};
use reifydb_engine::StandardCommandTransaction;

use super::LoggingBuilder;

/// Configuration function for the logging subsystem
pub type LoggingConfigurator = Box<dyn FnOnce(LoggingBuilder) -> LoggingBuilder + Send>;

/// Factory for creating LoggingSubsystem instances
pub struct LoggingSubsystemFactory<T: Transaction> {
	configurator: Option<LoggingConfigurator>,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> LoggingSubsystemFactory<T> {
	/// Create a new factory with default configuration
	pub fn new() -> Self {
		Self {
			configurator: None,
			_phantom: PhantomData,
		}
	}

	/// Create a factory with a custom configurator
	pub fn with_configurator<F>(configurator: F) -> Self
	where
		F: FnOnce(LoggingBuilder) -> LoggingBuilder + Send + 'static,
	{
		Self {
			configurator: Some(Box::new(configurator)),
			_phantom: PhantomData,
		}
	}
}

impl<T: Transaction> Default for LoggingSubsystemFactory<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: Transaction> SubsystemFactory<StandardCommandTransaction<T>> for LoggingSubsystemFactory<T> {
	fn provide_interceptors(
		&self,
		builder: reifydb_core::interceptor::StandardInterceptorBuilder<StandardCommandTransaction<T>>,
		_ioc: &IocContainer,
	) -> reifydb_core::interceptor::StandardInterceptorBuilder<StandardCommandTransaction<T>> {
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
