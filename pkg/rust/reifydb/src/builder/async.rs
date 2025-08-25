// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	hook::Hooks,
	interceptor::{RegisterInterceptor, StandardInterceptorBuilder},
	interface::{Transaction, subsystem::SubsystemFactory},
};
use reifydb_engine::StandardCommandTransaction;
#[cfg(feature = "sub_logging")]
use reifydb_sub_logging::{LoggingBuilder, LoggingSubsystemFactory};

use super::{DatabaseBuilder, traits::WithSubsystem};
use crate::Database;

#[cfg(feature = "async")]
pub struct AsyncBuilder<T: Transaction> {
	versioned: T::Versioned,
	unversioned: T::Unversioned,
	cdc: T::Cdc,
	hooks: Hooks,
	interceptors: StandardInterceptorBuilder<StandardCommandTransaction<T>>,
	subsystem_factories: Vec<
		Box<dyn SubsystemFactory<StandardCommandTransaction<T>>>,
	>,
}

#[cfg(feature = "async")]
impl<T: Transaction> AsyncBuilder<T> {
	pub fn new(
		versioned: T::Versioned,
		unversioned: T::Unversioned,
		cdc: T::Cdc,
		hooks: Hooks,
	) -> Self {
		Self {
			versioned,
			unversioned,
			cdc,
			hooks,
			interceptors: StandardInterceptorBuilder::new(),
			subsystem_factories: Vec::new(),
		}
	}

	pub fn intercept<I>(mut self, interceptor: I) -> Self
	where
		I: RegisterInterceptor<StandardCommandTransaction<T>>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		self.interceptors =
			self.interceptors.add_factory(move |interceptors| {
				interceptors.register(interceptor.clone());
			});
		self
	}

	pub fn build(self) -> crate::Result<Database<T>> {
		let mut builder = DatabaseBuilder::new(
			self.versioned,
			self.unversioned,
			self.cdc,
			self.hooks,
		)
		.with_interceptor_builder(self.interceptors);

		// Add any custom subsystem factories configured via fluent API
		for factory in self.subsystem_factories {
			builder = builder.add_subsystem_factory(factory);
		}

		// Add default subsystems (worker pool, flow, etc.)
		// This will only add logging if no subsystems were configured
		builder = builder.with_default_subsystems();

		builder.build()
	}
}

#[cfg(feature = "async")]
impl<T: Transaction> WithSubsystem<T> for AsyncBuilder<T> {
	#[cfg(feature = "sub_logging")]
	fn with_logging<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(LoggingBuilder) -> LoggingBuilder + Send + 'static,
	{
		self.subsystem_factories.push(Box::new(
			LoggingSubsystemFactory::with_configurator(
				configurator,
			),
		));
		self
	}

	fn with_subsystem(
		mut self,
		factory: Box<
			dyn SubsystemFactory<StandardCommandTransaction<T>>,
		>,
	) -> Self {
		self.subsystem_factories.push(factory);
		self
	}
}
