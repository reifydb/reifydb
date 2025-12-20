// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	event::EventBus,
	interceptor::{RegisterInterceptor, StandardInterceptorBuilder},
};
use reifydb_engine::{StandardCommandTransaction, function::FunctionsBuilder};
use reifydb_sub_api::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::FlowBuilder;
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::TracingBuilder;
use reifydb_sub_worker::WorkerBuilder;
use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMultiVersion, single::TransactionSingleVersion};

use super::{DatabaseBuilder, WithInterceptorBuilder, traits::WithSubsystem};
use crate::Database;

pub struct EmbeddedBuilder {
	multi: TransactionMultiVersion,
	single: TransactionSingleVersion,
	cdc: TransactionCdc,
	eventbus: EventBus,
	interceptors: StandardInterceptorBuilder<StandardCommandTransaction>,
	subsystem_factories: Vec<Box<dyn SubsystemFactory<StandardCommandTransaction>>>,
	functions_configurator: Option<Box<dyn FnOnce(FunctionsBuilder) -> FunctionsBuilder + Send + 'static>>,
	#[cfg(feature = "sub_tracing")]
	tracing_configurator: Option<Box<dyn FnOnce(TracingBuilder) -> TracingBuilder + Send + 'static>>,
	worker_configurator: Option<Box<dyn FnOnce(WorkerBuilder) -> WorkerBuilder + Send + 'static>>,
	#[cfg(feature = "sub_flow")]
	flow_configurator: Option<Box<dyn FnOnce(FlowBuilder) -> FlowBuilder + Send + 'static>>,
}

impl EmbeddedBuilder {
	pub fn new(
		multi: TransactionMultiVersion,
		single: TransactionSingleVersion,
		cdc: TransactionCdc,
		eventbus: EventBus,
	) -> Self {
		Self {
			multi,
			single,
			cdc,
			eventbus,
			interceptors: StandardInterceptorBuilder::new(),
			subsystem_factories: Vec::new(),
			functions_configurator: None,
			#[cfg(feature = "sub_tracing")]
			tracing_configurator: None,
			worker_configurator: None,
			#[cfg(feature = "sub_flow")]
			flow_configurator: None,
		}
	}

	pub fn intercept<I>(mut self, interceptor: I) -> Self
	where
		I: RegisterInterceptor<StandardCommandTransaction> + Send + Sync + Clone + 'static,
	{
		self.interceptors = self.interceptors.add_factory(move |interceptors| {
			interceptors.register(interceptor.clone());
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
		let mut builder = DatabaseBuilder::new(self.multi, self.single, self.cdc, self.eventbus)
			.with_interceptor_builder(self.interceptors);

		// Pass functions configurator if provided
		if let Some(configurator) = self.functions_configurator {
			builder = builder.with_functions_configurator(configurator);
		}

		// Add configured subsystems using the proper methods
		#[cfg(feature = "sub_tracing")]
		if let Some(configurator) = self.tracing_configurator {
			builder = builder.with_tracing(configurator);
		}

		if let Some(configurator) = self.worker_configurator {
			builder = builder.with_worker(configurator);
		}

		#[cfg(feature = "sub_flow")]
		if let Some(configurator) = self.flow_configurator {
			builder = builder.with_flow(configurator);
		}

		// Add any other custom subsystem factories
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

	fn with_worker<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(WorkerBuilder) -> WorkerBuilder + Send + 'static,
	{
		self.worker_configurator = Some(Box::new(configurator));
		self
	}

	fn with_subsystem(mut self, factory: Box<dyn SubsystemFactory<StandardCommandTransaction>>) -> Self {
		self.subsystem_factories.push(factory);
		self
	}
}

impl WithInterceptorBuilder for EmbeddedBuilder {
	fn interceptor_builder_mut(&mut self) -> &mut StandardInterceptorBuilder<StandardCommandTransaction> {
		&mut self.interceptors
	}
}
