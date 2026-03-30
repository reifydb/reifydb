// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::path::PathBuf;
#[cfg(feature = "sub_server")]
use std::sync::Arc;

use reifydb_auth::service::AuthConfigurator;
use reifydb_core::config::SystemConfig;
use reifydb_routine::{function::registry::FunctionsConfigurator, procedure::registry::ProceduresConfigurator};
use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig};
use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::builder::FlowConfigurator;
#[cfg(feature = "sub_replication")]
use reifydb_sub_replication::{
	builder::{ReplicationConfig, ReplicationConfigurator},
	factory::ReplicationSubsystemFactory,
};
#[cfg(feature = "sub_server")]
use reifydb_sub_server::interceptor::{RequestInterceptor, RequestInterceptorChain};
#[cfg(feature = "sub_server_admin")]
use reifydb_sub_server_admin::{config::AdminConfigurator, factory::AdminSubsystemFactory};
#[cfg(feature = "sub_server_grpc")]
use reifydb_sub_server_grpc::factory::{GrpcConfigurator, GrpcSubsystemFactory};
#[cfg(feature = "sub_server_http")]
use reifydb_sub_server_http::factory::{HttpConfigurator, HttpSubsystemFactory};
#[cfg(feature = "sub_server_otel")]
use reifydb_sub_server_otel::{config::OtelConfigurator, factory::OtelSubsystemFactory, subsystem::OtelSubsystem};
#[cfg(feature = "sub_server_ws")]
use reifydb_sub_server_ws::factory::{WsConfigurator, WsSubsystemFactory};
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingConfigurator;
use reifydb_transaction::interceptor::builder::InterceptorBuilder;

use super::{DatabaseBuilder, WithInterceptorBuilder, traits::WithSubsystem};
use crate::{
	Database, Migration,
	api::{StorageFactory, transaction},
};

#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel"))]
type OtelTracingConfig = (
	Box<dyn FnOnce(OtelConfigurator) -> OtelConfigurator + Send + 'static>,
	Box<dyn FnOnce(TracingConfigurator) -> TracingConfigurator + Send + 'static>,
);

pub struct ServerBuilder {
	storage_factory: StorageFactory,
	runtime_config: Option<SharedRuntimeConfig>,
	migrations: Vec<Migration>,
	interceptors: InterceptorBuilder,
	#[cfg(feature = "sub_server")]
	request_interceptors: Vec<Arc<dyn RequestInterceptor>>,
	subsystem_factories: Vec<Box<dyn SubsystemFactory>>,
	functions_configurator:
		Option<Box<dyn FnOnce(FunctionsConfigurator) -> FunctionsConfigurator + Send + 'static>>,
	procedures_configurator:
		Option<Box<dyn FnOnce(ProceduresConfigurator) -> ProceduresConfigurator + Send + 'static>>,
	handlers_configurator:
		Option<Box<dyn FnOnce(ProceduresConfigurator) -> ProceduresConfigurator + Send + 'static>>,
	#[cfg(reifydb_target = "native")]
	procedure_dir: Option<PathBuf>,
	#[cfg(feature = "sub_tracing")]
	tracing_configurator: Option<Box<dyn FnOnce(TracingConfigurator) -> TracingConfigurator + Send + 'static>>,
	#[cfg(feature = "sub_flow")]
	flow_configurator: Option<Box<dyn FnOnce(FlowConfigurator) -> FlowConfigurator + Send + 'static>>,
	#[cfg(feature = "sub_replication")]
	replication_factory: Option<Box<dyn SubsystemFactory>>,
	#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel"))]
	otel_tracing_config: Option<OtelTracingConfig>,
	auth_configurator: Option<Box<dyn FnOnce(AuthConfigurator) -> AuthConfigurator + Send + 'static>>,
}

impl ServerBuilder {
	pub fn new(storage_factory: StorageFactory) -> Self {
		Self {
			storage_factory,
			runtime_config: None,
			migrations: Vec::new(),
			interceptors: InterceptorBuilder::new(),
			#[cfg(feature = "sub_server")]
			request_interceptors: Vec::new(),
			subsystem_factories: Vec::new(),
			functions_configurator: None,
			procedures_configurator: None,
			handlers_configurator: None,
			#[cfg(reifydb_target = "native")]
			procedure_dir: None,
			#[cfg(feature = "sub_tracing")]
			tracing_configurator: None,
			#[cfg(feature = "sub_flow")]
			flow_configurator: None,
			#[cfg(feature = "sub_replication")]
			replication_factory: None,
			#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel"))]
			otel_tracing_config: None,
			auth_configurator: None,
		}
	}

	/// Configure the shared runtime.
	///
	/// If not set, a default configuration will be used.
	pub fn with_runtime_config(mut self, config: SharedRuntimeConfig) -> Self {
		self.runtime_config = Some(config);
		self
	}

	pub fn with_auth<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(AuthConfigurator) -> AuthConfigurator + Send + 'static,
	{
		self.auth_configurator = Some(Box::new(configurator));
		self
	}

	pub fn with_migrations(mut self, migrations: Vec<Migration>) -> Self {
		self.migrations = migrations;
		self
	}

	pub fn with_functions<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(FunctionsConfigurator) -> FunctionsConfigurator + Send + 'static,
	{
		self.functions_configurator = Some(Box::new(configurator));
		self
	}

	pub fn with_procedures<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(ProceduresConfigurator) -> ProceduresConfigurator + Send + 'static,
	{
		self.procedures_configurator = Some(Box::new(configurator));
		self
	}

	pub fn with_handlers<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(ProceduresConfigurator) -> ProceduresConfigurator + Send + 'static,
	{
		self.handlers_configurator = Some(Box::new(configurator));
		self
	}

	#[cfg(reifydb_target = "native")]
	pub fn with_procedure_dir(mut self, dir: impl Into<PathBuf>) -> Self {
		self.procedure_dir = Some(dir.into());
		self
	}

	/// Configure and add a gRPC subsystem.
	#[cfg(feature = "sub_server_grpc")]
	pub fn with_grpc<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(GrpcConfigurator) -> GrpcConfigurator + Send + 'static,
	{
		let factory = GrpcSubsystemFactory::new(configurator);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	/// Configure and add an HTTP subsystem.
	#[cfg(feature = "sub_server_http")]
	pub fn with_http<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(HttpConfigurator) -> HttpConfigurator + Send + 'static,
	{
		let factory = HttpSubsystemFactory::new(configurator);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	/// Configure and add a WebSocket subsystem.
	#[cfg(feature = "sub_server_ws")]
	pub fn with_ws<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(WsConfigurator) -> WsConfigurator + Send + 'static,
	{
		let factory = WsSubsystemFactory::new(configurator);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	/// Configure and add an OpenTelemetry subsystem.
	#[cfg(feature = "sub_server_otel")]
	pub fn with_otel<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(OtelConfigurator) -> OtelConfigurator + Send + 'static,
	{
		let factory = OtelSubsystemFactory::new(configurator);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	/// Configure tracing with OpenTelemetry integration.
	///
	/// This method coordinates the initialization of both the tracing subsystem
	/// and OpenTelemetry subsystem, ensuring the OpenTelemetry tracer is available
	/// before the tracing subscriber is initialized.
	///
	/// # Arguments
	///
	/// * `otel_config` - OpenTelemetry configuration
	/// * `tracing_configurator` - Function to configure the TracingConfigurator
	///
	/// # Example
	///
	/// ```ignore
	/// let db = server::memory()
	///     .with_tracing_otel(
	///         OtelConfig::new()
	///             .service_name("my-service")
	///             .endpoint("http://localhost:4317"),
	///         |t| t.with_filter("info")
	///     )
	///     .build()?;
	/// ```
	#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel"))]
	pub fn with_tracing_otel<O, F>(mut self, otel_configurator: O, tracing_configurator: F) -> Self
	where
		O: FnOnce(OtelConfigurator) -> OtelConfigurator + Send + 'static,
		F: FnOnce(TracingConfigurator) -> TracingConfigurator + Send + 'static,
	{
		// Store the configurators to be initialized later in build()
		self.otel_tracing_config = Some((Box::new(otel_configurator), Box::new(tracing_configurator)));
		self
	}

	/// Register a request-level interceptor.
	///
	/// Interceptors are called in registration order for `pre_execute`,
	/// and in reverse order for `post_execute`.
	#[cfg(feature = "sub_server")]
	pub fn with_request_interceptor<I: RequestInterceptor>(mut self, interceptor: I) -> Self {
		self.request_interceptors.push(Arc::new(interceptor));
		self
	}

	#[cfg(feature = "sub_server_admin")]
	pub fn with_admin<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(AdminConfigurator) -> AdminConfigurator + Send + 'static,
	{
		let factory = AdminSubsystemFactory::new(configurator);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	pub fn build(self) -> crate::Result<Database> {
		let runtime_config = self.runtime_config.unwrap_or_default();
		let runtime = SharedRuntime::from_config(runtime_config);

		let actor_system = runtime.actor_system().scope();
		let (multi_store, single_store, transaction_single, eventbus) =
			self.storage_factory.create(&actor_system);
		let system_config = SystemConfig::new();
		crate::config::register_defaults(&system_config);
		let (multi, single, eventbus) = transaction(
			(multi_store.clone(), single_store.clone(), transaction_single, eventbus),
			actor_system.clone(),
			runtime.clock().clone(),
			runtime.rng().clone(),
			system_config.clone(),
		);

		let mut database_builder = DatabaseBuilder::new(system_config, multi, single, eventbus)
			.with_interceptor_builder(self.interceptors)
			.with_runtime(runtime.clone())
			.with_actor_system(actor_system)
			.with_stores(multi_store, single_store);

		#[cfg(feature = "sub_server")]
		{
			let chain = RequestInterceptorChain::new(self.request_interceptors);
			database_builder = database_builder.with_request_interceptor_chain(chain);
		}

		if let Some(configurator) = self.auth_configurator {
			database_builder = database_builder.with_auth(configurator);
		}

		if !self.migrations.is_empty() {
			database_builder = database_builder.with_migrations(self.migrations);
		}

		if let Some(configurator) = self.functions_configurator {
			database_builder = database_builder.with_functions_configurator(configurator);
		}

		if let Some(configurator) = self.procedures_configurator {
			database_builder = database_builder.with_procedures_configurator(configurator);
		}

		if let Some(configurator) = self.handlers_configurator {
			database_builder = database_builder.with_handlers_configurator(configurator);
		}

		#[cfg(reifydb_target = "native")]
		if let Some(dir) = self.procedure_dir {
			database_builder = database_builder.with_procedure_dir(dir);
		}

		#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel"))]
		if let Some((otel_configurator, tracing_configurator)) = self.otel_tracing_config {
			use reifydb_sub_api::subsystem::Subsystem;

			let otel_config = otel_configurator(OtelConfigurator::new()).configure();
			let mut otel_subsystem = OtelSubsystem::new(otel_config, runtime.clone());
			otel_subsystem.start().expect("Failed to start OpenTelemetry subsystem");

			let tracer =
				otel_subsystem.tracer().expect("Tracer not available after starting OtelSubsystem");

			database_builder = database_builder.with_tracing(move |builder| {
				let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
				let builder_with_otel = builder.with_layer(otel_layer);
				tracing_configurator(builder_with_otel)
			});

			let factory = OtelSubsystemFactory::with_subsystem(otel_subsystem);
			database_builder = database_builder.add_subsystem_factory(Box::new(factory));
		} else {
			#[cfg(feature = "sub_tracing")]
			{
				let configurator = self.tracing_configurator.unwrap_or_else(|| Box::new(|t| t));
				database_builder = database_builder.with_tracing(configurator);
			}
		}

		#[cfg(not(all(feature = "sub_tracing", feature = "sub_server_otel")))]
		{
			#[cfg(feature = "sub_tracing")]
			{
				let configurator = self.tracing_configurator.unwrap_or_else(|| Box::new(|t| t));
				database_builder = database_builder.with_tracing(configurator);
			}
		}

		#[cfg(feature = "sub_flow")]
		if let Some(configurator) = self.flow_configurator {
			database_builder = database_builder.with_flow(configurator);
		}

		#[cfg(feature = "sub_replication")]
		if let Some(factory) = self.replication_factory {
			database_builder = database_builder.add_replication_factory(factory);
		}

		for factory in self.subsystem_factories {
			database_builder = database_builder.add_subsystem_factory(factory);
		}

		database_builder.build()
	}
}

impl WithSubsystem for ServerBuilder {
	#[cfg(feature = "sub_tracing")]
	fn with_tracing<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(TracingConfigurator) -> TracingConfigurator + Send + 'static,
	{
		self.tracing_configurator = Some(Box::new(configurator));
		self
	}

	#[cfg(feature = "sub_flow")]
	fn with_flow<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(FlowConfigurator) -> FlowConfigurator + Send + 'static,
	{
		self.flow_configurator = Some(Box::new(configurator));
		self
	}

	#[cfg(feature = "sub_replication")]
	fn with_replication<F, C>(mut self, configurator: F) -> Self
	where
		F: FnOnce(ReplicationConfigurator) -> C + Send + 'static,
		C: Into<ReplicationConfig> + 'static,
	{
		self.replication_factory = Some(Box::new(ReplicationSubsystemFactory::new(configurator)));
		self
	}

	fn with_subsystem(mut self, factory: Box<dyn SubsystemFactory>) -> Self {
		self.subsystem_factories.push(factory);
		self
	}
}

impl WithInterceptorBuilder for ServerBuilder {
	fn interceptor_builder_mut(&mut self) -> &mut InterceptorBuilder {
		&mut self.interceptors
	}
}
