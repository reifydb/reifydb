// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_function::registry::FunctionsBuilder;
use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig};
use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::builder::FlowBuilder;
#[cfg(feature = "sub_server_admin")]
use reifydb_sub_server_admin::{config::AdminConfig, factory::AdminSubsystemFactory};
#[cfg(feature = "sub_server_http")]
use reifydb_sub_server_http::factory::{HttpConfig, HttpSubsystemFactory};
#[cfg(feature = "sub_server_otel")]
use reifydb_sub_server_otel::{config::OtelConfig, factory::OtelSubsystemFactory, subsystem::OtelSubsystem};
#[cfg(feature = "sub_server_ws")]
use reifydb_sub_server_ws::factory::{WsConfig, WsSubsystemFactory};
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingBuilder;
use reifydb_transaction::interceptor::{builder::StandardInterceptorBuilder, interceptors::RegisterInterceptor};

use super::{DatabaseBuilder, WithInterceptorBuilder, traits::WithSubsystem};
use crate::{
	Database,
	api::{StorageFactory, transaction},
};

pub struct ServerBuilder {
	storage_factory: StorageFactory,
	runtime_config: Option<SharedRuntimeConfig>,
	interceptors: StandardInterceptorBuilder,
	subsystem_factories: Vec<Box<dyn SubsystemFactory>>,
	functions_configurator: Option<Box<dyn FnOnce(FunctionsBuilder) -> FunctionsBuilder + Send + 'static>>,
	#[cfg(feature = "sub_tracing")]
	tracing_configurator: Option<Box<dyn FnOnce(TracingBuilder) -> TracingBuilder + Send + 'static>>,
	#[cfg(feature = "sub_flow")]
	flow_configurator: Option<Box<dyn FnOnce(FlowBuilder) -> FlowBuilder + Send + 'static>>,
	#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel"))]
	otel_tracing_config: Option<(OtelConfig, Box<dyn FnOnce(TracingBuilder) -> TracingBuilder + Send + 'static>)>,
}

impl ServerBuilder {
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
			#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel"))]
			otel_tracing_config: None,
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
		// Store the config and configurator to be initialized later in build()
		self.otel_tracing_config = Some((otel_config, Box::new(tracing_configurator)));
		self
	}

	#[cfg(feature = "sub_server_admin")]
	pub fn with_admin(mut self, config: AdminConfig) -> Self {
		let factory = AdminSubsystemFactory::new(config);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	pub fn build(self) -> crate::Result<Database> {
		let runtime_config = self.runtime_config.unwrap_or_default();
		let runtime = SharedRuntime::from_config(runtime_config);

		// Create storage
		let (multi_store, single_store, transaction_single, eventbus) = self.storage_factory.create();

		// Create transaction layer using the runtime's actor system
		let actor_system = runtime.actor_system();
		let (multi, single, eventbus) = transaction(
			(multi_store.clone(), single_store.clone(), transaction_single, eventbus),
			actor_system,
			runtime.clock().clone(),
		);

		let mut database_builder = DatabaseBuilder::new(multi, single, eventbus)
			.with_interceptor_builder(self.interceptors)
			.with_runtime(runtime.clone())
			.with_stores(multi_store, single_store);

		if let Some(configurator) = self.functions_configurator {
			database_builder = database_builder.with_functions_configurator(configurator);
		}

		#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel"))]
		if let Some((otel_config, tracing_configurator)) = self.otel_tracing_config {
			use reifydb_sub_api::subsystem::Subsystem;

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
			if let Some(configurator) = self.tracing_configurator {
				database_builder = database_builder.with_tracing(configurator);
			}
		}

		#[cfg(not(all(feature = "sub_tracing", feature = "sub_server_otel")))]
		{
			#[cfg(feature = "sub_tracing")]
			if let Some(configurator) = self.tracing_configurator {
				database_builder = database_builder.with_tracing(configurator);
			}
		}

		#[cfg(feature = "sub_flow")]
		if let Some(configurator) = self.flow_configurator {
			database_builder = database_builder.with_flow(configurator);
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
