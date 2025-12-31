// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_builtin::FunctionsBuilder;
use reifydb_core::{ComputePool, event::EventBus};
use reifydb_sub_api::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::FlowBuilder;
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::TracingBuilder;
use reifydb_transaction::{
	cdc::TransactionCdc,
	interceptor::{RegisterInterceptor, StandardInterceptorBuilder},
	multi::TransactionMultiVersion,
	single::TransactionSingle,
};

use super::{DatabaseBuilder, WithInterceptorBuilder, traits::WithSubsystem};
use crate::Database;

pub struct EmbeddedBuilder {
	multi: TransactionMultiVersion,
	single: TransactionSingle,
	cdc: TransactionCdc,
	eventbus: EventBus,
	interceptors: StandardInterceptorBuilder,
	subsystem_factories: Vec<Box<dyn SubsystemFactory>>,
	functions_configurator: Option<Box<dyn FnOnce(FunctionsBuilder) -> FunctionsBuilder + Send + 'static>>,
	compute_pool: Option<ComputePool>,
	#[cfg(feature = "sub_tracing")]
	tracing_configurator: Option<Box<dyn FnOnce(TracingBuilder) -> TracingBuilder + Send + 'static>>,
	#[cfg(feature = "sub_flow")]
	flow_configurator: Option<Box<dyn FnOnce(FlowBuilder) -> FlowBuilder + Send + 'static>>,
}

impl EmbeddedBuilder {
	pub fn new(
		multi: TransactionMultiVersion,
		single: TransactionSingle,
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
			compute_pool: None,
			#[cfg(feature = "sub_tracing")]
			tracing_configurator: None,
			#[cfg(feature = "sub_flow")]
			flow_configurator: None,
		}
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

	pub fn with_compute_pool(mut self, pool: ComputePool) -> Self {
		self.compute_pool = Some(pool);
		self
	}

	pub async fn build(self) -> crate::Result<Database> {
		let mut builder = DatabaseBuilder::new(self.multi, self.single, self.cdc, self.eventbus)
			.with_interceptor_builder(self.interceptors);

		// Pass functions configurator if provided
		if let Some(configurator) = self.functions_configurator {
			builder = builder.with_functions_configurator(configurator);
		}

		// Pass compute pool if provided
		if let Some(pool) = self.compute_pool {
			builder = builder.with_compute_pool(pool);
		}

		// Add configured subsystems using the proper methods
		#[cfg(feature = "sub_tracing")]
		if let Some(configurator) = self.tracing_configurator {
			builder = builder.with_tracing(configurator);
		}

		#[cfg(feature = "sub_flow")]
		if let Some(configurator) = self.flow_configurator {
			builder = builder.with_flow(configurator);
		}

		// Add any other custom subsystem factories
		for factory in self.subsystem_factories {
			builder = builder.add_subsystem_factory(factory);
		}

		builder.build().await
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
