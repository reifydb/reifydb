// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_core::{Result, interceptor::StandardInterceptorBuilder, interface::Transaction, ioc::IocContainer};
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_sub_api::{Subsystem, SubsystemFactory};

use super::{WorkerBuilder, WorkerConfig, WorkerSubsystem};

/// Configuration function for the worker pool subsystem
pub type WorkerPoolConfigurator = Box<dyn FnOnce(WorkerBuilder) -> WorkerBuilder + Send>;

/// Factory for creating WorkerPoolSubsystem instances
pub struct WorkerSubsystemFactory<T: Transaction> {
	configurator: Option<WorkerPoolConfigurator>,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> WorkerSubsystemFactory<T> {
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
		F: FnOnce(WorkerBuilder) -> WorkerBuilder + Send + 'static,
	{
		Self {
			configurator: Some(Box::new(configurator)),
			_phantom: PhantomData,
		}
	}

	/// Create a new factory with custom configuration (legacy method)
	pub fn with_config(config: WorkerConfig) -> Self {
		Self::with_configurator(move |_| {
			WorkerBuilder::new()
				.num_workers(config.num_workers)
				.max_queue_size(config.max_queue_size)
				.scheduler_interval(config.scheduler_interval)
				.task_timeout_warning(config.task_timeout_warning)
		})
	}
}

impl<T: Transaction> Default for WorkerSubsystemFactory<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: Transaction> SubsystemFactory<StandardCommandTransaction<T>> for WorkerSubsystemFactory<T> {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<StandardCommandTransaction<T>>,
		_ioc: &IocContainer,
	) -> StandardInterceptorBuilder<StandardCommandTransaction<T>> {
		// WorkerPool doesn't need any interceptors
		builder
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		// Build WorkerSubsystem configuration
		let builder = if let Some(configurator) = self.configurator {
			configurator(WorkerBuilder::new())
		} else {
			WorkerBuilder::default()
		};

		// Get the StandardEngine from IoC
		let engine = ioc.resolve::<StandardEngine<T>>()?;

		// Create subsystem
		let config = builder.build();
		let subsystem = WorkerSubsystem::with_config_and_engine(config, engine);

		Ok(Box::new(subsystem))
	}
}
