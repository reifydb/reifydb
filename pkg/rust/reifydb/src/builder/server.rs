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
#[cfg(feature = "sub_server_otel")]
use reifydb_sub_server::SharedRuntime;
#[cfg(feature = "sub_server_admin")]
use reifydb_sub_server_admin::{AdminConfig, AdminSubsystemFactory};
#[cfg(feature = "sub_server_http")]
use reifydb_sub_server_http::{HttpConfig, HttpSubsystemFactory};
#[cfg(feature = "sub_server_otel")]
use reifydb_sub_server_otel::{OtelConfig, OtelSubsystem, OtelSubsystemFactory};
#[cfg(feature = "sub_server_ws")]
use reifydb_sub_server_ws::{WsConfig, WsSubsystemFactory};
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::TracingBuilder;
use reifydb_sub_worker::WorkerBuilder;
use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMultiVersion, single::TransactionSingleVersion};

use super::{DatabaseBuilder, WithInterceptorBuilder, traits::WithSubsystem};
use crate::Database;

pub struct ServerBuilder {
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

impl ServerBuilder {
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

	/// Configure and add an HTTP subsystem.
	#[cfg(feature = "sub_server_http")]
	pub fn with_http(mut self, config: HttpConfig) -> Self {
		let factory = HttpSubsystemFactory::new(config);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	/// Configure and add a WebSocket subsystem.
	#[cfg(feature = "sub_server_ws")]
	pub fn with_ws(mut self, config: WsConfig) -> Self {
		let factory = WsSubsystemFactory::new(config);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	/// Configure and add an OpenTelemetry subsystem.
	#[cfg(feature = "sub_server_otel")]
	pub fn with_otel(mut self, config: OtelConfig) -> Self {
		let factory = OtelSubsystemFactory::new(config);
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
	/// * `tracing_configurator` - Function to configure the TracingBuilder
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
	pub fn with_tracing_otel<F>(mut self, otel_config: OtelConfig, tracing_configurator: F) -> Self
	where
		F: FnOnce(TracingBuilder) -> TracingBuilder + Send + 'static,
	{
		use reifydb_sub_api::Subsystem;

		// Step 1: Create and start the OtelSubsystem early
		let runtime = otel_config.runtime.clone().unwrap_or_else(SharedRuntime::default);
		let mut otel_subsystem = OtelSubsystem::new(otel_config, runtime);
		otel_subsystem.start().expect("Failed to start OpenTelemetry subsystem");

		// Step 2: Get the concrete tracer from the initialized provider
		let tracer = otel_subsystem.tracer().expect("Tracer not available after starting OtelSubsystem");

		// Step 3: Configure tracing with the OpenTelemetry layer
		self.tracing_configurator = Some(Box::new(move |builder| {
			let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
			let builder_with_otel = builder.with_layer(otel_layer);
			tracing_configurator(builder_with_otel)
		}));

		// Step 4: Store the pre-initialized subsystem to be added during build
		let factory = OtelSubsystemFactory::with_subsystem(otel_subsystem);
		self.subsystem_factories.push(Box::new(factory));

		self
	}

	#[cfg(feature = "sub_server_admin")]
	pub fn with_admin(mut self, config: AdminConfig) -> Self {
		let factory = AdminSubsystemFactory::new(config);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	pub fn build(self) -> crate::Result<Database> {
		let mut database_builder = DatabaseBuilder::new(self.multi, self.single, self.cdc, self.eventbus)
			.with_interceptor_builder(self.interceptors);

		// Pass functions configurator if provided
		if let Some(configurator) = self.functions_configurator {
			database_builder = database_builder.with_functions_configurator(configurator);
		}

		// Add configured subsystems using the proper methods
		#[cfg(feature = "sub_tracing")]
		if let Some(configurator) = self.tracing_configurator {
			database_builder = database_builder.with_tracing(configurator);
		}

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

impl WithSubsystem for ServerBuilder {
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

impl WithInterceptorBuilder for ServerBuilder {
	fn interceptor_builder_mut(&mut self) -> &mut StandardInterceptorBuilder<StandardCommandTransaction> {
		&mut self.interceptors
	}
}
