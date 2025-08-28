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
#[cfg(feature = "sub_server")]
use reifydb_sub_server::{ServerConfig, ServerSubsystemFactory};
#[cfg(feature = "sub_ws")]
use reifydb_sub_ws::{WsConfig, WsSubsystemFactory};

use super::{DatabaseBuilder, traits::WithSubsystem};
use crate::Database;

#[cfg(any(feature = "sub_ws", feature = "sub_server"))]
pub struct ServerBuilder<T: Transaction> {
	versioned: T::Versioned,
	unversioned: T::Unversioned,
	cdc: T::Cdc,
	hooks: Hooks,
	interceptors: StandardInterceptorBuilder<StandardCommandTransaction<T>>,
	subsystem_factories: Vec<
		Box<dyn SubsystemFactory<StandardCommandTransaction<T>>>,
	>,
}

#[cfg(any(feature = "sub_ws", feature = "sub_server"))]
impl<T: Transaction> ServerBuilder<T> {
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

	#[cfg(feature = "sub_ws")]
	pub fn with_ws(mut self, config: WsConfig) -> Self {
		let factory = WsSubsystemFactory::new(config);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	#[cfg(feature = "sub_server")]
	pub fn with_server(mut self, config: ServerConfig) -> Self {
		let factory = ServerSubsystemFactory::new(config);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	pub fn build(self) -> crate::Result<Database<T>> {
		let mut database_builder = DatabaseBuilder::new(
			self.versioned,
			self.unversioned,
			self.cdc,
			self.hooks,
		)
		.with_interceptor_builder(self.interceptors);

		// Add all subsystem factories
		for factory in self.subsystem_factories {
			database_builder =
				database_builder.add_subsystem_factory(factory);
		}

		// Add default subsystems
		database_builder = database_builder.with_default_subsystems();

		database_builder.build()
	}
}

#[cfg(any(feature = "sub_ws", feature = "sub_server"))]
impl<T: Transaction> WithSubsystem<T> for ServerBuilder<T> {
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
