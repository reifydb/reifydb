// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use super::LoggingBuilder;
use reifydb_core::interface::subsystem::{Subsystem, SubsystemFactory};
use reifydb_core::{
    interface::Transaction,
    ioc::IocContainer,
};

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
	fn create(
		self: Box<Self>,
		_ioc: &IocContainer,
	) -> reifydb_core::Result<Box<dyn Subsystem>> {
		let builder = LoggingBuilder::new().with_console();
		Ok(Box::new(builder.build()))
	}
}
