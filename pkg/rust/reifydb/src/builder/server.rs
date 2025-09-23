// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	event::EventBus,
	interceptor::{RegisterInterceptor, StandardInterceptorBuilder},
	interface::{CdcTransaction, MultiVersionTransaction, SingleVersionTransaction},
};
use reifydb_engine::{EngineTransaction, StandardCommandTransaction};
#[cfg(feature = "sub_admin")]
use reifydb_sub_admin::{AdminConfig, AdminSubsystemFactory};
use reifydb_sub_api::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::FlowBuilder;
#[cfg(feature = "sub_logging")]
use reifydb_sub_logging::LoggingBuilder;
#[cfg(feature = "sub_server")]
use reifydb_sub_server::{ServerConfig, ServerSubsystemFactory};
#[cfg(feature = "sub_worker")]
use reifydb_sub_worker::WorkerBuilder;

use super::{DatabaseBuilder, traits::WithSubsystem};
use crate::Database;

#[cfg(feature = "sub_server")]
pub struct ServerBuilder<MVT: MultiVersionTransaction, SVT: SingleVersionTransaction, C: CdcTransaction> {
	multi: MVT,
	single: SVT,
	cdc: C,
	eventbus: EventBus,
	interceptors: StandardInterceptorBuilder<StandardCommandTransaction<EngineTransaction<MVT, SVT, C>>>,
	subsystem_factories: Vec<Box<dyn SubsystemFactory<StandardCommandTransaction<EngineTransaction<MVT, SVT, C>>>>>,
	#[cfg(feature = "sub_logging")]
	logging_configurator: Option<Box<dyn FnOnce(LoggingBuilder) -> LoggingBuilder + Send + 'static>>,
	#[cfg(feature = "sub_worker")]
	worker_configurator: Option<Box<dyn FnOnce(WorkerBuilder) -> WorkerBuilder + Send + 'static>>,
	#[cfg(feature = "sub_flow")]
	flow_configurator: Option<
		Box<
			dyn FnOnce(
					FlowBuilder<EngineTransaction<MVT, SVT, C>>,
				) -> FlowBuilder<EngineTransaction<MVT, SVT, C>>
				+ Send
				+ 'static,
		>,
	>,
}

#[cfg(feature = "sub_server")]
impl<MVT: MultiVersionTransaction, SVT: SingleVersionTransaction, C: CdcTransaction> ServerBuilder<MVT, SVT, C> {
	pub fn new(multi: MVT, single: SVT, cdc: C, eventbus: EventBus) -> Self {
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
		I: RegisterInterceptor<StandardCommandTransaction<EngineTransaction<MVT, SVT, C>>>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		self.interceptors = self.interceptors.add_factory(move |interceptors| {
			interceptors.register(interceptor.clone());
		});
		self
	}

	#[cfg(feature = "sub_server")]
	pub fn with_config(mut self, config: ServerConfig) -> Self {
		let factory = ServerSubsystemFactory::new(config);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	#[cfg(feature = "sub_admin")]
	pub fn with_admin(mut self, config: AdminConfig) -> Self {
		let factory = AdminSubsystemFactory::new(config);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	pub fn build(self) -> crate::Result<Database<MVT, SVT, C>> {
		let mut database_builder = DatabaseBuilder::new(self.multi, self.single, self.cdc, self.eventbus)
			.with_interceptor_builder(self.interceptors);

		// Add configured subsystems using the proper methods
		#[cfg(feature = "sub_logging")]
		if let Some(configurator) = self.logging_configurator {
			database_builder = database_builder.with_logging(configurator);
		}

		#[cfg(feature = "sub_worker")]
		if let Some(configurator) = self.worker_configurator {
			database_builder = database_builder.with_worker(configurator);
		}

		#[cfg(feature = "sub_flow")]
		if let Some(configurator) = self.flow_configurator {
			database_builder = database_builder.with_flow(configurator);
		}

		// Add any other custom subsystem factories
		for factory in self.subsystem_factories {
			database_builder = database_builder.add_subsystem_factory(factory);
		}

		database_builder.build()
	}
}

#[cfg(feature = "sub_server")]
impl<MVT: MultiVersionTransaction, SVT: SingleVersionTransaction, C: CdcTransaction>
	WithSubsystem<EngineTransaction<MVT, SVT, C>> for ServerBuilder<MVT, SVT, C>
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
				FlowBuilder<EngineTransaction<MVT, SVT, C>>,
			) -> FlowBuilder<EngineTransaction<MVT, SVT, C>>
			+ Send
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
		factory: Box<dyn SubsystemFactory<StandardCommandTransaction<EngineTransaction<MVT, SVT, C>>>>,
	) -> Self {
		self.subsystem_factories.push(factory);
		self
	}
}
