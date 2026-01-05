// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{ComputePool, event::EventBus};
use reifydb_function::FunctionsBuilder;
use reifydb_sub_api::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::FlowBuilder;
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
use reifydb_transaction::{
	cdc::TransactionCdc,
	interceptor::{RegisterInterceptor, StandardInterceptorBuilder},
	multi::TransactionMultiVersion,
	single::TransactionSingle,
};

use super::{DatabaseBuilder, WithInterceptorBuilder, traits::WithSubsystem};
use crate::Database;

pub struct ServerBuilder {
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

impl ServerBuilder {
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
		use tokio::runtime::Handle;

		// Step 1: Create and start the OtelSubsystem early
		// Note: We need to start synchronously here to get the tracer for the tracing layer.
		// This requires being called from within a tokio runtime context.
		let mut otel_subsystem = OtelSubsystem::new(otel_config);
		Handle::current().block_on(otel_subsystem.start()).expect("Failed to start OpenTelemetry subsystem");

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

	pub async fn build(self) -> crate::Result<Database> {
		let mut database_builder = DatabaseBuilder::new(self.multi, self.single, self.cdc, self.eventbus)
			.with_interceptor_builder(self.interceptors);

		// Pass functions configurator if provided
		if let Some(configurator) = self.functions_configurator {
			database_builder = database_builder.with_functions_configurator(configurator);
		}

		// Pass compute pool if provided
		if let Some(pool) = self.compute_pool {
			database_builder = database_builder.with_compute_pool(pool);
		}

		// Add configured subsystems using the proper methods
		#[cfg(feature = "sub_tracing")]
		if let Some(configurator) = self.tracing_configurator {
			database_builder = database_builder.with_tracing(configurator);
		}

		#[cfg(feature = "sub_flow")]
		if let Some(configurator) = self.flow_configurator {
			database_builder = database_builder.with_flow(configurator);
		}

		// Add any other custom subsystem factories
		for factory in self.subsystem_factories {
			database_builder = database_builder.add_subsystem_factory(factory);
		}

		database_builder.build().await
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

	fn with_subsystem(mut self, factory: Box<dyn SubsystemFactory>) -> Self {
		self.subsystem_factories.push(factory);
		self
	}
}

impl WithInterceptorBuilder for ServerBuilder {
	fn interceptor_builder_mut(&mut self) -> &mut StandardInterceptorBuilder {
		&mut self.interceptors
	}
}
