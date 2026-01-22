// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_function::registry::FunctionsBuilder;
use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig};
use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::builder::FlowBuilder;
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingBuilder;
use reifydb_transaction::interceptor::{builder::StandardInterceptorBuilder, interceptors::RegisterInterceptor};

use super::{DatabaseBuilder, WithInterceptorBuilder, traits::WithSubsystem};
use crate::{
	Database,
	api::{StorageFactory, transaction},
};

pub struct EmbeddedBuilder {
	storage_factory: StorageFactory,
	runtime_config: Option<SharedRuntimeConfig>,
	interceptors: StandardInterceptorBuilder,
	subsystem_factories: Vec<Box<dyn SubsystemFactory>>,
	functions_configurator: Option<Box<dyn FnOnce(FunctionsBuilder) -> FunctionsBuilder + Send + 'static>>,
	#[cfg(feature = "sub_tracing")]
	tracing_configurator: Option<Box<dyn FnOnce(TracingBuilder) -> TracingBuilder + Send + 'static>>,
	#[cfg(feature = "sub_flow")]
	flow_configurator: Option<Box<dyn FnOnce(FlowBuilder) -> FlowBuilder + Send + 'static>>,
}

impl EmbeddedBuilder {
	pub fn new(storage_factory: StorageFactory) -> Self {
		Self {
			storage_factory,
			runtime_config: None,
			interceptors: StandardInterceptorBuilder::new(),
			subsystem_factories: Vec::new(),
			functions_configurator: None,
			#[cfg(feature = "sub_tracing")]
			tracing_configurator: None,
			#[cfg(feature = "sub_flow")]
			flow_configurator: None,
		}
	}

	/// Configure the shared runtime.
	///
	/// If not set, a default configuration will be used.
	pub fn with_runtime_config(mut self, config: SharedRuntimeConfig) -> Self {
		self.runtime_config = Some(config);
		self
	}

	pub fn intercept<I>(mut self, interceptor: I) -> Self
	where
		I: RegisterInterceptor + Clone + 'static,
	{
		self.interceptors = self.interceptors.add_factory(move |interceptors| {
			interceptor.clone().register(interceptors);
		});
		self
	}

	pub fn with_functions<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(FunctionsBuilder) -> FunctionsBuilder + Send + 'static,
	{
		self.functions_configurator = Some(Box::new(configurator));
		self
	}

	pub fn build(self) -> crate::Result<Database> {
		let runtime_config = self.runtime_config.unwrap_or_default();
		let runtime = SharedRuntime::from_config(runtime_config);

		// Create storage
		let (multi_store, single_store, transaction_single, eventbus) =
			self.storage_factory.create();

		// Create transaction layer using the runtime's actor system
		let actor_system = runtime.actor_system();
		let (multi, single, eventbus) = transaction(
			(multi_store.clone(), single_store.clone(), transaction_single, eventbus),
			actor_system,
		);

		let mut builder = DatabaseBuilder::new(multi, single, eventbus)
			.with_interceptor_builder(self.interceptors)
			.with_runtime(runtime)
			.with_stores(multi_store, single_store);

		if let Some(configurator) = self.functions_configurator {
			builder = builder.with_functions_configurator(configurator);
		}

		#[cfg(feature = "sub_tracing")]
		if let Some(configurator) = self.tracing_configurator {
			builder = builder.with_tracing(configurator);
		}

		#[cfg(feature = "sub_flow")]
		if let Some(configurator) = self.flow_configurator {
			builder = builder.with_flow(configurator);
		}

		for factory in self.subsystem_factories {
			builder = builder.add_subsystem_factory(factory);
		}

		builder.build()
	}
}

impl WithSubsystem for EmbeddedBuilder {
	#[cfg(feature = "sub_tracing")]
	fn with_tracing<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(TracingBuilder) -> TracingBuilder + Send + 'static,
	{
		self.tracing_configurator = Some(Box::new(configurator));
		self
	}

	#[cfg(feature = "sub_flow")]
	fn with_flow<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(FlowBuilder) -> FlowBuilder + Send + 'static,
	{
		self.flow_configurator = Some(Box::new(configurator));
		self
	}

	fn with_subsystem(mut self, factory: Box<dyn SubsystemFactory>) -> Self {
		self.subsystem_factories.push(factory);
		self
	}
}

impl WithInterceptorBuilder for EmbeddedBuilder {
	fn interceptor_builder_mut(&mut self) -> &mut StandardInterceptorBuilder {
		&mut self.interceptors
	}
}
