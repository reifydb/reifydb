// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{path::PathBuf, sync::Arc};

use reifydb_auth::service::AuthConfigurator;
use reifydb_catalog::{bootstrap::read_configs, materialized::MaterializedCatalog};
use reifydb_core::interface::catalog::config::ConfigKey;
#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
use reifydb_metric::{
	accumulator::StatementStatsAccumulator,
	registry::{MetricRegistry, StaticMetricRegistry},
};
use reifydb_routine::routine::registry::RoutinesConfigurator;
#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
use reifydb_runtime::context::clock::Clock;
use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig, pool::PoolConfig};
use reifydb_store_multi::hot::storage::HotStorage;
use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::builder::FlowConfigurator;
#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
use reifydb_sub_metric::{factory::MetricSubsystemFactory, interceptor::RequestMetricsInterceptor};
#[cfg(feature = "sub_replication")]
use reifydb_sub_replication::builder::{ReplicationConfig, ReplicationConfigurator};
#[cfg(all(feature = "sub_replication", not(reifydb_single_threaded)))]
use reifydb_sub_replication::factory::ReplicationSubsystemFactory;
#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
use reifydb_sub_server::interceptor::{RequestInterceptor, RequestInterceptorChain};
#[cfg(all(feature = "sub_server_admin", not(reifydb_single_threaded)))]
use reifydb_sub_server_admin::{config::AdminConfigurator, factory::AdminSubsystemFactory};
#[cfg(all(feature = "sub_server_grpc", not(reifydb_single_threaded)))]
use reifydb_sub_server_grpc::factory::{GrpcConfigurator, GrpcSubsystemFactory};
#[cfg(all(feature = "sub_server_http", not(reifydb_single_threaded)))]
use reifydb_sub_server_http::factory::{HttpConfigurator, HttpSubsystemFactory};
#[cfg(all(feature = "sub_server_otel", not(reifydb_single_threaded)))]
use reifydb_sub_server_otel::{config::OtelConfigurator, factory::OtelSubsystemFactory, subsystem::OtelSubsystem};
#[cfg(all(feature = "sub_server_ws", not(reifydb_single_threaded)))]
use reifydb_sub_server_ws::factory::{WsConfigurator, WsSubsystemFactory};
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingConfigurator;
use reifydb_transaction::interceptor::builder::InterceptorBuilder;
use reifydb_type::value::Value;

fn pool_config_from_sources(
	factory: &StorageFactory,
	overrides: &[(ConfigKey, Value)],
) -> Result<(HotStorage, PoolConfig)> {
	let multi_hot = factory.open_multi_hot();
	let persisted = read_configs(
		Some(&multi_hot),
		None,
		None,
		&[ConfigKey::ThreadsAsync, ConfigKey::ThreadsSystem, ConfigKey::ThreadsQuery],
	)?;

	let resolve = |key: ConfigKey| -> usize {
		let value = overrides
			.iter()
			.rev()
			.find(|(k, _)| *k == key)
			.and_then(|(_, v)| key.accept(v.clone()).ok())
			.unwrap_or_else(|| persisted[&key].clone());
		match value {
			Value::Uint2(v) => v as usize,
			other => panic!("config key {key} expected Uint2, got {other:?}"),
		}
	};

	let pools = PoolConfig {
		async_threads: resolve(ConfigKey::ThreadsAsync),
		system_threads: resolve(ConfigKey::ThreadsSystem),
		query_threads: resolve(ConfigKey::ThreadsQuery),
	};
	Ok((multi_hot, pools))
}

use super::{DatabaseBuilder, WithInterceptorBuilder, database::CdcBackend, traits::WithSubsystem};
use crate::{
	Database, MigrationSource, Result,
	api::{StorageFactory, transaction},
};

#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel", not(reifydb_single_threaded)))]
type OtelTracingConfig = (
	Box<dyn FnOnce(OtelConfigurator) -> OtelConfigurator + Send + 'static>,
	Box<dyn FnOnce(TracingConfigurator) -> TracingConfigurator + Send + 'static>,
);

pub struct ServerBuilder {
	storage_factory: StorageFactory,
	runtime_config: Option<SharedRuntimeConfig>,
	migrations: Option<MigrationSource>,
	interceptors: InterceptorBuilder,
	#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
	request_interceptors: Vec<Arc<dyn RequestInterceptor>>,
	subsystem_factories: Vec<Box<dyn SubsystemFactory>>,
	routines_configurator: Option<Box<dyn FnOnce(RoutinesConfigurator) -> RoutinesConfigurator + Send + 'static>>,
	handlers_configurator: Option<Box<dyn FnOnce(RoutinesConfigurator) -> RoutinesConfigurator + Send + 'static>>,
	#[cfg(reifydb_target = "native")]
	procedure_dir: Option<PathBuf>,
	#[cfg(feature = "sub_tracing")]
	tracing_configurator: Option<Box<dyn FnOnce(TracingConfigurator) -> TracingConfigurator + Send + 'static>>,
	#[cfg(feature = "sub_flow")]
	flow_configurator: Option<Box<dyn FnOnce(FlowConfigurator) -> FlowConfigurator + Send + 'static>>,
	#[cfg(feature = "sub_replication")]
	replication_factory: Option<Box<dyn SubsystemFactory>>,
	#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel", not(reifydb_single_threaded)))]
	otel_tracing_config: Option<OtelTracingConfig>,
	auth_configurator: Option<Box<dyn FnOnce(AuthConfigurator) -> AuthConfigurator + Send + 'static>>,
	#[cfg(feature = "sub_replication")]
	is_replica: bool,
	bootstrap_configs: Vec<(ConfigKey, Value)>,
}

impl ServerBuilder {
	pub fn new(storage_factory: StorageFactory) -> Self {
		Self {
			storage_factory,
			runtime_config: None,
			migrations: None,
			interceptors: InterceptorBuilder::new(),
			#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
			request_interceptors: Vec::new(),
			subsystem_factories: Vec::new(),
			routines_configurator: None,
			handlers_configurator: None,
			#[cfg(reifydb_target = "native")]
			procedure_dir: None,
			#[cfg(feature = "sub_tracing")]
			tracing_configurator: None,
			#[cfg(feature = "sub_flow")]
			flow_configurator: None,
			#[cfg(feature = "sub_replication")]
			replication_factory: None,
			#[cfg(feature = "sub_replication")]
			is_replica: false,
			#[cfg(all(
				feature = "sub_tracing",
				feature = "sub_server_otel",
				not(reifydb_single_threaded)
			))]
			otel_tracing_config: None,
			auth_configurator: None,
			bootstrap_configs: Vec::new(),
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

	pub fn with_migrations(mut self, source: impl Into<MigrationSource>) -> Self {
		self.migrations = Some(source.into());
		self
	}

	/// Set a system configuration value applied during bootstrap.
	///
	/// Applied on every `build()`, overwriting any previously persisted value.
	pub fn with_config(mut self, key: ConfigKey, value: Value) -> Self {
		self.bootstrap_configs.push((key, value));
		self
	}

	/// Set multiple system configuration values applied during bootstrap.
	pub fn with_configs(mut self, configs: impl IntoIterator<Item = (ConfigKey, Value)>) -> Self {
		self.bootstrap_configs.extend(configs);
		self
	}

	pub fn with_routines<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(RoutinesConfigurator) -> RoutinesConfigurator + Send + 'static,
	{
		self.routines_configurator = Some(Box::new(configurator));
		self
	}

	pub fn with_handlers<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(RoutinesConfigurator) -> RoutinesConfigurator + Send + 'static,
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
	#[cfg(all(feature = "sub_server_grpc", not(reifydb_single_threaded)))]
	pub fn with_grpc<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(GrpcConfigurator) -> GrpcConfigurator + Send + 'static,
	{
		let factory = GrpcSubsystemFactory::new(configurator);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	/// Configure and add an HTTP subsystem.
	#[cfg(all(feature = "sub_server_http", not(reifydb_single_threaded)))]
	pub fn with_http<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(HttpConfigurator) -> HttpConfigurator + Send + 'static,
	{
		let factory = HttpSubsystemFactory::new(configurator);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	/// Configure and add a WebSocket subsystem.
	#[cfg(all(feature = "sub_server_ws", not(reifydb_single_threaded)))]
	pub fn with_ws<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(WsConfigurator) -> WsConfigurator + Send + 'static,
	{
		let factory = WsSubsystemFactory::new(configurator);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	/// Configure and add an OpenTelemetry subsystem.
	#[cfg(all(feature = "sub_server_otel", not(reifydb_single_threaded)))]
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
	#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel", not(reifydb_single_threaded)))]
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
	#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
	pub fn with_request_interceptor<I: RequestInterceptor>(mut self, interceptor: I) -> Self {
		self.request_interceptors.push(Arc::new(interceptor));
		self
	}

	#[cfg(all(feature = "sub_server_admin", not(reifydb_single_threaded)))]
	pub fn with_admin<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(AdminConfigurator) -> AdminConfigurator + Send + 'static,
	{
		let factory = AdminSubsystemFactory::new(configurator);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	#[allow(unused_mut)]
	pub fn build(mut self) -> Result<Database> {
		let (multi_hot, pool_config) =
			pool_config_from_sources(&self.storage_factory, &self.bootstrap_configs)?;

		let runtime_config = self.runtime_config.unwrap_or_default();
		let runtime = SharedRuntime::from_config(runtime_config, pool_config);

		let actor_system = runtime.actor_system().scope();
		let (multi_store, single_store, transaction_single, eventbus) =
			self.storage_factory.create_with_multi_hot(multi_hot, &actor_system);
		let materialized_catalog = MaterializedCatalog::new();
		let (multi, single, eventbus) = transaction(
			(multi_store.clone(), single_store.clone(), transaction_single, eventbus),
			actor_system.clone(),
			runtime.clock().clone(),
			runtime.rng().clone(),
			Arc::new(materialized_catalog.clone()),
		);

		let cdc_backend = match &self.storage_factory {
			StorageFactory::Memory => CdcBackend::Memory,
			#[cfg(not(target_arch = "wasm32"))]
			StorageFactory::Sqlite(config) => CdcBackend::Sqlite(config.clone()),
		};

		let mut database_builder = DatabaseBuilder::new(materialized_catalog, multi, single, eventbus.clone())
			.with_interceptor_builder(self.interceptors)
			.with_runtime(runtime.clone())
			.with_actor_system(actor_system.clone())
			.with_stores(multi_store, single_store)
			.with_cdc_backend(cdc_backend);

		#[cfg(feature = "sub_replication")]
		if self.is_replica {
			database_builder = database_builder.is_replica();
		}

		#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
		{
			let registry = Arc::new(MetricRegistry::new());
			let static_registry = Arc::new(StaticMetricRegistry::new());
			let accumulator = Arc::new(StatementStatsAccumulator::new());

			let metrics_interceptor =
				RequestMetricsInterceptor::new(eventbus.clone(), accumulator.clone(), Clock::Real);
			self.request_interceptors.push(Arc::new(metrics_interceptor));

			let chain = RequestInterceptorChain::new(self.request_interceptors);
			database_builder = database_builder.with_request_interceptor_chain(chain);

			let metric_factory = MetricSubsystemFactory::new(registry, static_registry, accumulator);
			database_builder = database_builder.add_subsystem_factory(Box::new(metric_factory));
		}

		if let Some(configurator) = self.auth_configurator {
			database_builder = database_builder.with_auth(configurator);
		}

		if let Some(source) = self.migrations {
			let migrations = source.resolve()?;
			if !migrations.is_empty() {
				database_builder = database_builder.with_migrations(migrations);
			}
		}

		if !self.bootstrap_configs.is_empty() {
			database_builder = database_builder.with_configs(self.bootstrap_configs);
		}

		if let Some(configurator) = self.routines_configurator {
			database_builder = database_builder.with_routines_configurator(configurator);
		}

		if let Some(configurator) = self.handlers_configurator {
			database_builder = database_builder.with_handlers_configurator(configurator);
		}

		#[cfg(reifydb_target = "native")]
		if let Some(dir) = self.procedure_dir {
			database_builder = database_builder.with_procedure_dir(dir);
		}

		#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel", not(reifydb_single_threaded)))]
		if let Some((otel_configurator, tracing_configurator)) = self.otel_tracing_config {
			use reifydb_sub_api::subsystem::Subsystem;
			use tracing_opentelemetry::layer as otel_layer_fn;

			let otel_config = otel_configurator(OtelConfigurator::new()).configure();
			let mut otel_subsystem = OtelSubsystem::new(otel_config, runtime.clone());
			otel_subsystem.start().expect("Failed to start OpenTelemetry subsystem");

			let tracer =
				otel_subsystem.tracer().expect("Tracer not available after starting OtelSubsystem");

			database_builder = database_builder.with_tracing(move |builder| {
				let otel_layer = otel_layer_fn().with_tracer(tracer);
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

		#[cfg(not(all(feature = "sub_tracing", feature = "sub_server_otel", not(reifydb_single_threaded))))]
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

	#[cfg(all(feature = "sub_replication", not(reifydb_single_threaded)))]
	fn with_replication<F, C>(mut self, configurator: F) -> Self
	where
		F: FnOnce(ReplicationConfigurator) -> C + Send + 'static,
		C: Into<ReplicationConfig> + 'static,
	{
		let config = configurator(ReplicationConfigurator).into();
		self.is_replica = matches!(config, ReplicationConfig::Replica(_));
		self.replication_factory = Some(Box::new(ReplicationSubsystemFactory::from_config(config)));
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
