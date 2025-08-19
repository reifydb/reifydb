// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_core::{
	Result, interceptor::StandardInterceptorBuilder,
	interface::Transaction, ioc::IocContainer,
};

use super::{WorkerPoolConfig, WorkerPoolSubsystem};
use crate::subsystem::{Subsystem, SubsystemFactory};

/// Factory for creating WorkerPoolSubsystem instances
pub struct WorkerPoolSubsystemFactory<T: Transaction> {
	config: WorkerPoolConfig,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> WorkerPoolSubsystemFactory<T> {
	/// Create a new factory with default configuration
	pub fn new() -> Self {
		Self::with_config(WorkerPoolConfig::default())
	}

	/// Create a new factory with custom configuration
	pub fn with_config(config: WorkerPoolConfig) -> Self {
		Self {
			config,
			_phantom: PhantomData,
		}
	}
}

impl<T: Transaction> Default for WorkerPoolSubsystemFactory<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: Transaction> SubsystemFactory<T> for WorkerPoolSubsystemFactory<T> {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<T>,
		_ioc: &IocContainer,
	) -> StandardInterceptorBuilder<T> {
		// WorkerPool doesn't need any interceptors
		builder
	}

	fn create(
		self: Box<Self>,
		_ioc: &IocContainer,
	) -> Result<Box<dyn Subsystem>> {
		Ok(Box::new(WorkerPoolSubsystem::with_config(self.config)))
	}
}
