// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use std::path::PathBuf;
use std::sync::Arc;

use reifydb_auth::service::AuthConfigurator;
use reifydb_catalog::cache::CatalogCache;
use reifydb_core::interface::{auth::AuthenticationProvider, catalog::config::ConfigKey};
#[cfg(feature = "sub_metric_profiler")]
use reifydb_profiler::{
	event::{ProfilerScopeBatchEvent, ProfilerScopeClosedEvent},
	intern::DimInterner,
	layer::ProfilerLayer,
	sink::ProfilerSink,
};
use reifydb_routine_abi::registry::RoutinesConfigurator;
#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
use reifydb_runtime::context::clock::Clock;
#[cfg(feature = "sub_metric_profiler")]
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_runtime::{
	Runtime, RuntimeConfig, fatal::install as install_fatal, pool::PoolConfig, version_epoch::VersionEpoch,
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_store_cdc::{config::CdcCommitConfig, tier::read::CdcReadConfig};
use reifydb_store_multi::tier::{
	commit::buffer::MultiCommitBufferTier, persistent::MultiPersistentTier, point::MultiPointConfig,
	range::MultiRangeConfig,
};
use reifydb_store_operator::tier::{point::OperatorPointConfig, range::OperatorRangeConfig};
use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::builder::FlowConfigurator;
#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
use reifydb_sub_metrics::accumulator::StatementMetricsAccumulator;
#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
use reifydb_sub_metrics::interceptor::RequestMetricsInterceptor;
#[cfg(feature = "sub_metric_profiler")]
use reifydb_sub_metrics::profiler::{
	accumulator::ProfilerAccumulator,
	actor::ProfilerCollectorActor,
	builder::ProfilerConfigurator,
	factory::ProfilerSubsystemFactory,
	instruments::ProfilerInstruments,
	listener::{ProfilerScopeBatchListener, ProfilerScopeClosedListener},
	sink::EventBusSink,
	subsystem::ProfilerSubsystem,
};
#[cfg(feature = "sub_raft")]
use reifydb_sub_raft::config::RaftConfig;
#[cfg(all(feature = "sub_replication", not(reifydb_single_threaded)))]
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
use reifydb_value::value::Value;
#[cfg(feature = "sub_metric_profiler")]
use tracing_subscriber::filter::LevelFilter;

#[cfg(feature = "sub_raft")]
use crate::raft::RaftSubsystemFactory;
use crate::system::raise_fd_limit;

type PoolConfigSources = (
	MultiCommitBufferTier,
	Option<MultiPersistentTier>,
	PoolConfig,
	Option<MultiPointConfig>,
	Option<MultiRangeConfig>,
	Option<OperatorPointConfig>,
	Option<OperatorRangeConfig>,
	u32,
	CdcCommitConfig,
	Option<CdcReadConfig>,
);

fn pool_config_from_sources(factory: &StorageFactory, overrides: &[(ConfigKey, Value)]) -> Result<PoolConfigSources> {
	let multi_commit_buffer = factory.open_multi_commit_buffer();
	let multi_persistent = factory.open_multi_persistent();
	let resolved = resolve_startup_configs(&multi_commit_buffer, multi_persistent.as_ref(), overrides)?;
	if let Some(persistent) = multi_persistent.as_ref() {
		persistent.set_checkpoint_threshold(resolved.multi_wal_autocheckpoint);
	}
	Ok((
		multi_commit_buffer,
		multi_persistent,
		resolved.pools,
		resolved.multi_point,
		resolved.multi_range,
		resolved.operator_point,
		resolved.operator_range,
		resolved.cdc_wal_autocheckpoint,
		resolved.cdc_commit,
		resolved.cdc_read,
	))
}

use super::{DatabaseBuilder, WithInterceptorBuilder, startup::resolve_startup_configs, traits::WithSubsystem};
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
	runtime_config: Option<RuntimeConfig>,
	migrations: Option<MigrationSource>,
	interceptors: InterceptorBuilder,
	#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
	request_interceptors: Vec<Arc<dyn RequestInterceptor>>,
	subsystem_factories: Vec<Box<dyn SubsystemFactory>>,
	routines_configurator: Option<Box<dyn FnOnce(RoutinesConfigurator) -> RoutinesConfigurator + Send + 'static>>,
	handlers_configurator: Option<Box<dyn FnOnce(RoutinesConfigurator) -> RoutinesConfigurator + Send + 'static>>,
	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	procedure_dir: Option<PathBuf>,
	#[cfg(feature = "sub_tracing")]
	tracing_configurator: Option<Box<dyn FnOnce(TracingConfigurator) -> TracingConfigurator + Send + 'static>>,
	#[cfg(feature = "sub_metric_profiler")]
	profiler_configurator: Option<Box<dyn FnOnce(ProfilerConfigurator) -> ProfilerConfigurator + Send + 'static>>,
	#[cfg(feature = "sub_flow")]
	flow_configurator: Option<Box<dyn FnOnce(FlowConfigurator) -> FlowConfigurator + Send + 'static>>,
	#[cfg(feature = "sub_replication")]
	replication_factory: Option<Box<dyn SubsystemFactory>>,
	#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel", not(reifydb_single_threaded)))]
	otel_tracing_config: Option<OtelTracingConfig>,
	auth_configurator: Option<Box<dyn FnOnce(AuthConfigurator) -> AuthConfigurator + Send + 'static>>,
	auth_providers: Vec<Box<dyn AuthenticationProvider>>,
	#[cfg(feature = "sub_replication")]
	is_replica: bool,
	bootstrap_configs: Vec<(ConfigKey, Value)>,
	fast_shutdown: bool,
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
			#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
			procedure_dir: None,
			#[cfg(feature = "sub_tracing")]
			tracing_configurator: None,
			#[cfg(feature = "sub_metric_profiler")]
			profiler_configurator: None,
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
			auth_providers: Vec::new(),
			bootstrap_configs: Vec::new(),
			fast_shutdown: false,
		}
	}

	pub fn with_runtime_config(mut self, config: RuntimeConfig) -> Self {
		self.runtime_config = Some(config);
		self
	}

	pub fn with_fast_shutdown(mut self) -> Self {
		self.fast_shutdown = true;
		self
	}

	pub fn with_auth<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(AuthConfigurator) -> AuthConfigurator + Send + 'static,
	{
		self.auth_configurator = Some(Box::new(configurator));
		self
	}

	pub fn with_auth_provider(mut self, provider: impl AuthenticationProvider + 'static) -> Self {
		self.auth_providers.push(Box::new(provider));
		self
	}

	pub fn with_migrations(mut self, source: impl Into<MigrationSource>) -> Self {
		self.migrations = Some(source.into());
		self
	}

	/// Overwrites any previously persisted value for this key on every `build()`.
	pub fn with_config(mut self, key: ConfigKey, value: Value) -> Self {
		self.bootstrap_configs.push((key, value));
		self
	}

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

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	pub fn with_procedure_dir(mut self, dir: impl Into<PathBuf>) -> Self {
		self.procedure_dir = Some(dir.into());
		self
	}

	#[cfg(all(feature = "sub_server_grpc", not(reifydb_single_threaded)))]
	pub fn with_grpc<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(GrpcConfigurator) -> GrpcConfigurator + Send + 'static,
	{
		let factory = GrpcSubsystemFactory::new(configurator);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	#[cfg(all(feature = "sub_server_http", not(reifydb_single_threaded)))]
	pub fn with_http<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(HttpConfigurator) -> HttpConfigurator + Send + 'static,
	{
		let factory = HttpSubsystemFactory::new(configurator);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	#[cfg(all(feature = "sub_server_ws", not(reifydb_single_threaded)))]
	pub fn with_ws<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(WsConfigurator) -> WsConfigurator + Send + 'static,
	{
		let factory = WsSubsystemFactory::new(configurator);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	#[cfg(all(feature = "sub_server_otel", not(reifydb_single_threaded)))]
	pub fn with_otel<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(OtelConfigurator) -> OtelConfigurator + Send + 'static,
	{
		let factory = OtelSubsystemFactory::new(configurator);
		self.subsystem_factories.push(Box::new(factory));
		self
	}

	/// Pairs the two subsystems so the OpenTelemetry tracer exists before the tracing
	/// subscriber is initialized; configuring them separately cannot guarantee that order.
	#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel", not(reifydb_single_threaded)))]
	pub fn with_tracing_otel<O, F>(mut self, otel_configurator: O, tracing_configurator: F) -> Self
	where
		O: FnOnce(OtelConfigurator) -> OtelConfigurator + Send + 'static,
		F: FnOnce(TracingConfigurator) -> TracingConfigurator + Send + 'static,
	{
		self.otel_tracing_config = Some((Box::new(otel_configurator), Box::new(tracing_configurator)));
		self
	}

	/// Interceptors run in registration order for `pre_execute` and in reverse order
	/// for `post_execute`.
	#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
	pub fn with_request_interceptor<I: RequestInterceptor>(mut self, interceptor: I) -> Self {
		self.request_interceptors.push(Arc::new(interceptor));
		self
	}

	#[cfg(feature = "sub_raft")]
	pub fn with_raft(mut self, config: RaftConfig) -> Self {
		let factory = RaftSubsystemFactory::new(config);
		self.subsystem_factories.push(Box::new(factory));
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
		// Servers accept one file descriptor per connection; raise the soft FD
		// limit to the hard limit before any listener is bound so concurrent load
		// does not exhaust it (`accept error: Too many open files`).
		raise_fd_limit();

		let (
			multi_commit_buffer,
			multi_persistent,
			pool_config,
			multi_point_buffer,
			multi_range_buffer,
			operator_point_buffer,
			operator_range_buffer,
			cdc_wal_autocheckpoint,
			cdc_commit,
			cdc_read,
		) = pool_config_from_sources(&self.storage_factory, &self.bootstrap_configs)?;

		let runtime_config = self.runtime_config.unwrap_or_default();
		install_fatal(runtime_config.fatal);
		let runtime = Runtime::from_config(runtime_config, pool_config);

		let spawner = runtime.spawner();
		let clock = runtime.clock().clone();
		let rng = runtime.rng().clone();

		let (multi_store, single_store, operator_store, cdc_store, transaction_single, eventbus) =
			self.storage_factory.create_with_multi_commit_buffer(
				multi_commit_buffer,
				multi_persistent,
				multi_point_buffer,
				multi_range_buffer,
				operator_point_buffer,
				operator_range_buffer,
				cdc_commit,
				cdc_read,
				cdc_wal_autocheckpoint,
				&spawner,
			);
		let catalog_cache = CatalogCache::new();
		let version_epoch = VersionEpoch::new();
		let (multi, single, eventbus) = transaction(
			(multi_store.clone(), single_store.clone(), transaction_single, eventbus),
			spawner.clone(),
			clock.clone(),
			version_epoch.clone(),
			rng,
			Arc::new(catalog_cache.clone()),
		);

		let mut database_builder =
			DatabaseBuilder::new(catalog_cache, multi, single, eventbus.clone(), version_epoch)
				.with_interceptor_builder(self.interceptors)
				.with_stores(multi_store, single_store, operator_store, cdc_store);

		if self.fast_shutdown {
			database_builder = database_builder.with_fast_shutdown();
		}

		#[cfg(feature = "sub_replication")]
		if self.is_replica {
			database_builder = database_builder.is_replica();
		}

		#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
		{
			let accumulator = Arc::new(StatementMetricsAccumulator::new());

			let metrics_interceptor =
				RequestMetricsInterceptor::new(eventbus.clone(), accumulator.clone(), Clock::Real);
			self.request_interceptors.push(Arc::new(metrics_interceptor));

			let chain = RequestInterceptorChain::new(self.request_interceptors);
			database_builder = database_builder.with_request_interceptor_chain(chain);

			database_builder = database_builder.with_dependency(accumulator);
		}

		if let Some(configurator) = self.auth_configurator {
			database_builder = database_builder.with_auth(configurator);
		}

		for provider in self.auth_providers {
			database_builder = database_builder.with_boxed_auth_provider(provider);
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

		#[cfg(feature = "sub_metric_profiler")]
		#[allow(unused_variables)]
		let profiler_layer: Option<ProfilerLayer> = if let Some(configurator_fn) = self.profiler_configurator.take() {
			let cfg = configurator_fn(ProfilerConfigurator::new());
			let interner = Arc::new(DimInterner::new());
			let instruments = Arc::new(ProfilerInstruments::new());
			let accumulator = Arc::new(RwLock::new(ProfilerAccumulator::new(
				cfg.accumulator_capacity,
				0,
				Arc::new(ProfilerInstruments::new()),
			)));
			let actor = ProfilerCollectorActor::new(
				Arc::clone(&accumulator),
				Arc::clone(&interner),
				Arc::clone(&instruments),
				cfg.accumulator_capacity,
				cfg.min_calls_for_retention,
				clock.clone(),
			);
			let handle = spawner.spawn_coordination("profile-collector", actor);
			let actor_ref = handle.actor_ref().clone();
			eventbus.register::<ProfilerScopeClosedEvent, _>(ProfilerScopeClosedListener::new(
				actor_ref.clone(),
			));
			eventbus.register::<ProfilerScopeBatchEvent, _>(ProfilerScopeBatchListener::new(
				actor_ref.clone(),
			));
			let collector = Some(actor_ref);
			let sink: Arc<dyn ProfilerSink> =
				Arc::new(EventBusSink::new(eventbus.clone(), Arc::clone(&instruments)));
			let subsystem = ProfilerSubsystem::new(
				cfg.categories,
				interner,
				accumulator,
				instruments,
				sink,
				clock.clone(),
				collector,
			);
			let layer = subsystem.layer();
			database_builder = database_builder
				.add_subsystem_factory(Box::new(ProfilerSubsystemFactory::with_subsystem(subsystem)));
			Some(layer)
		} else {
			None
		};

		if let Some(configurator) = self.routines_configurator {
			database_builder = database_builder.with_routines_configurator(configurator);
		}

		if let Some(configurator) = self.handlers_configurator {
			database_builder = database_builder.with_handlers_configurator(configurator);
		}

		#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
		if let Some(dir) = self.procedure_dir {
			database_builder = database_builder.with_procedure_dir(dir);
		}

		#[cfg(all(feature = "sub_tracing", feature = "sub_server_otel", not(reifydb_single_threaded)))]
		if let Some((otel_configurator, tracing_configurator)) = self.otel_tracing_config {
			use tracing_opentelemetry::layer as otel_layer_fn;

			let otel_config = otel_configurator(OtelConfigurator::new()).configure();
			let otel_subsystem = OtelSubsystem::new(otel_config, runtime.tokio())
				.expect("Failed to start OpenTelemetry subsystem");

			let tracer =
				otel_subsystem.tracer().expect("Tracer not available after starting OtelSubsystem");

			#[cfg(feature = "sub_metric_profiler")]
			let profiler_layer_otel = profiler_layer;
			database_builder = database_builder.with_tracing(move |builder| {
				let otel_layer = otel_layer_fn().with_tracer(tracer);
				let mut b = builder.with_layer(otel_layer);
				#[cfg(feature = "sub_metric_profiler")]
				if let Some(layer) = profiler_layer_otel {
					b = b.with_layer(layer).with_layer_filter(LevelFilter::TRACE);
				}
				tracing_configurator(b)
			});

			let factory = OtelSubsystemFactory::with_subsystem(otel_subsystem);
			database_builder = database_builder.add_subsystem_factory(Box::new(factory));
		} else {
			#[cfg(feature = "sub_tracing")]
			{
				let inner = self.tracing_configurator.unwrap_or_else(|| Box::new(|t| t));
				#[cfg(feature = "sub_metric_profiler")]
				let configurator: Box<
					dyn FnOnce(TracingConfigurator) -> TracingConfigurator + Send,
				> = if let Some(layer) = profiler_layer {
					Box::new(move |t| {
						inner(t.with_layer(layer).with_layer_filter(LevelFilter::TRACE))
					})
				} else {
					inner
				};
				#[cfg(not(feature = "sub_metric_profiler"))]
				let configurator = inner;
				database_builder = database_builder.with_tracing(configurator);
			}
		}

		#[cfg(all(
			feature = "sub_tracing",
			not(all(feature = "sub_server_otel", not(reifydb_single_threaded)))
		))]
		{
			let inner = self.tracing_configurator.unwrap_or_else(|| Box::new(|t| t));
			#[cfg(feature = "sub_metric_profiler")]
			let configurator: Box<dyn FnOnce(TracingConfigurator) -> TracingConfigurator + Send> = if let Some(layer) =
				profiler_layer
			{
				Box::new(move |t| inner(t.with_layer(layer).with_layer_filter(LevelFilter::TRACE)))
			} else {
				inner
			};
			#[cfg(not(feature = "sub_metric_profiler"))]
			let configurator = inner;
			database_builder = database_builder.with_tracing(configurator);
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

		database_builder = database_builder.with_runtime(runtime);

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

	#[cfg(feature = "sub_metric_profiler")]
	fn with_profiler<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(ProfilerConfigurator) -> ProfilerConfigurator + Send + 'static,
	{
		self.profiler_configurator = Some(Box::new(configurator));
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
