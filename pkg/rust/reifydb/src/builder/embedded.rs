// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	event::EventBus,
	interceptor::{RegisterInterceptor, StandardInterceptorBuilder},
};
use reifydb_engine::{EngineTransaction, StandardCommandTransaction, TransactionCdc};
use reifydb_sub_api::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::FlowBuilder;
#[cfg(feature = "sub_logging")]
use reifydb_sub_logging::LoggingBuilder;
#[cfg(feature = "sub_worker")]
use reifydb_sub_worker::WorkerBuilder;
use reifydb_transaction::{multi::TransactionMultiVersion, single::TransactionSingleVersion};

use super::{DatabaseBuilder, traits::WithSubsystem};
use crate::Database;

pub struct EmbeddedBuilder {
	multi: TransactionMultiVersion,
	single: TransactionSingleVersion,
	cdc: TransactionCdc,
	eventbus: EventBus,
	interceptors: StandardInterceptorBuilder<
		StandardCommandTransaction<
			EngineTransaction<TransactionMultiVersion, TransactionSingleVersion, TransactionCdc>,
		>,
	>,
	subsystem_factories: Vec<
		Box<
			dyn SubsystemFactory<
				StandardCommandTransaction<
					EngineTransaction<
						TransactionMultiVersion,
						TransactionSingleVersion,
						TransactionCdc,
					>,
				>,
			>,
		>,
	>,
	#[cfg(feature = "sub_logging")]
	logging_configurator: Option<Box<dyn FnOnce(LoggingBuilder) -> LoggingBuilder + Send + 'static>>,
	#[cfg(feature = "sub_worker")]
	worker_configurator: Option<Box<dyn FnOnce(WorkerBuilder) -> WorkerBuilder + Send + 'static>>,
	#[cfg(feature = "sub_flow")]
	flow_configurator: Option<
		Box<
			dyn FnOnce(
					FlowBuilder<
						EngineTransaction<
							TransactionMultiVersion,
							TransactionSingleVersion,
							TransactionCdc,
						>,
					>,
				) -> FlowBuilder<
					EngineTransaction<
						TransactionMultiVersion,
						TransactionSingleVersion,
						TransactionCdc,
					>,
				> + Send
				+ 'static,
		>,
	>,
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
			#[cfg(feature = "sub_logging")]
			logging_configurator: None,
			#[cfg(feature = "sub_worker")]
			worker_configurator: None,
			#[cfg(feature = "sub_flow")]
			flow_configurator: None,
		}
	}

	pub fn intercept<I>(mut self, interceptor: I) -> Self
	where
		I: RegisterInterceptor<
				StandardCommandTransaction<
					EngineTransaction<
						TransactionMultiVersion,
						TransactionSingleVersion,
						TransactionCdc,
					>,
				>,
			> + Send
			+ Sync
			+ Clone
			+ 'static,
	{
		self.interceptors = self.interceptors.add_factory(move |interceptors| {
			interceptors.register(interceptor.clone());
		});
		self
	}

	pub fn build(self) -> crate::Result<Database> {
		let mut builder = DatabaseBuilder::new(self.multi, self.single, self.cdc, self.eventbus)
			.with_interceptor_builder(self.interceptors);

		// Add configured subsystems using the proper methods
		#[cfg(feature = "sub_logging")]
		if let Some(configurator) = self.logging_configurator {
			builder = builder.with_logging(configurator);
		}

		#[cfg(feature = "sub_worker")]
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

impl WithSubsystem<EngineTransaction<TransactionMultiVersion, TransactionSingleVersion, TransactionCdc>>
	for EmbeddedBuilder
{
	#[cfg(feature = "sub_logging")]
	fn with_logging<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(LoggingBuilder) -> LoggingBuilder + Send + 'static,
	{
		self.logging_configurator = Some(Box::new(configurator));
		self
	}

	#[cfg(feature = "sub_flow")]
	fn with_flow<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(
				FlowBuilder<
					EngineTransaction<
						TransactionMultiVersion,
						TransactionSingleVersion,
						TransactionCdc,
					>,
				>,
			) -> FlowBuilder<
				EngineTransaction<TransactionMultiVersion, TransactionSingleVersion, TransactionCdc>,
			> + Send
			+ 'static,
	{
		self.flow_configurator = Some(Box::new(configurator));
		self
	}

	#[cfg(feature = "sub_worker")]
	fn with_worker<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(WorkerBuilder) -> WorkerBuilder + Send + 'static,
	{
		self.worker_configurator = Some(Box::new(configurator));
		self
	}

	fn with_subsystem(
		mut self,
		factory: Box<
			dyn SubsystemFactory<
				StandardCommandTransaction<
					EngineTransaction<
						TransactionMultiVersion,
						TransactionSingleVersion,
						TransactionCdc,
					>,
				>,
			>,
		>,
	) -> Self {
		self.subsystem_factories.push(factory);
		self
	}
}
