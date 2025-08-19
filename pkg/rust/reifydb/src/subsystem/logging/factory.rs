// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_core::{
	Result, interceptor::StandardInterceptorBuilder,
	interface::Transaction, ioc::IocContainer,
};
use reifydb_sub_log::LoggingBuilder;

use super::LoggingSubsystem;
use crate::subsystem::{Subsystem, SubsystemFactory};

/// Factory for creating LoggingSubsystem instances
pub struct LoggingSubsystemFactory<T: Transaction> {
	_phantom: PhantomData<T>,
}

impl<T: Transaction> LoggingSubsystemFactory<T> {
	/// Create a new factory with default configuration
	pub fn new() -> Self {
		Self {
			_phantom: PhantomData,
		}
	}
}

impl<T: Transaction> Default for LoggingSubsystemFactory<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: Transaction> SubsystemFactory<T> for LoggingSubsystemFactory<T> {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<T>,
		_ioc: &IocContainer,
	) -> StandardInterceptorBuilder<T> {
		// Logging doesn't need any interceptors
		builder
	}

	fn create(self: Box<Self>, _ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		// Create a new LoggingBuilder for each instance
		let builder = LoggingBuilder::new().with_console();
		Ok(Box::new(LoggingSubsystem::from_builder(builder)))
	}
}