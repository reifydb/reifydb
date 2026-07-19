// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{path::PathBuf, sync::Arc};

use reifydb_auth::{
	AuthVersion,
	registry::AuthenticationRegistry,
	service::{AuthConfigurator, AuthService, AuthServiceConfig},
};
use reifydb_catalog::{
	CatalogVersion,
	bootstrap::{apply_bootstrap_configs, bootstrap_system_objects, load_catalog_cache, seed_bootstrap_configs},
	cache::CatalogCache,
	catalog::Catalog,
	system::SystemCatalog,
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_cdc::compact::actor::CompactActor;
use reifydb_cdc::{
	CdcVersion,
	consume::wake::CdcWakeRegistry,
	produce::{
		producer::{CdcProducerEventListener, spawn_cdc_producer},
		watermark::CdcProducerWatermark,
	},
	storage::CdcStore,
};
use reifydb_core::{
	CoreVersion,
	actors::cdc::CdcProduceHandle,
	event::{EventBus, transaction::PostCommitEvent},
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		version::{ComponentType, HasVersion, SystemVersion},
	},
	metrics::registry::MetricsRegistry,
	util::ioc::IocContainer,
};
#[cfg(not(reifydb_single_threaded))]
use reifydb_engine::remote::RemoteRegistry;
use reifydb_engine::{EngineVersion, engine::StandardEngine, vm::services::EngineConfig};
#[cfg(reifydb_target = "native")]
use reifydb_extension::procedure::ffi_loader::register_procedures_from_dir;
use reifydb_extension::{
	procedure::wasm_loader::register_wasm_procedures_from_dir,
	transform::registry::{Transforms, TransformsConfigurator},
};
use reifydb_routine::{
	function::default_native_functions,
	monoid::default_native_monoids,
	procedure::default_native_procedures,
	routine::registry::{Routines, RoutinesConfigurator},
};
use reifydb_rql::RqlVersion;
use reifydb_runtime::{Runtime, context::RuntimeContext};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sqlite::SqliteConfig;
use reifydb_store_multi::{MultiStore, MultiStoreVersion, gc::epoch::listener::VersionEpochListener};
use reifydb_store_single::{SingleStore, SingleStoreVersion};
use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::{builder::FlowConfigurator, subsystem::factory::FlowSubsystemFactory};
use reifydb_sub_metrics::factory::MetricsSubsystemFactory;
#[cfg(feature = "sub_metric_profiler")]
use reifydb_sub_metrics::profiler::{builder::ProfilerConfigurator, factory::ProfilerSubsystemFactory};
#[cfg(feature = "sub_replication")]
use reifydb_sub_replication::builder::{ReplicationConfig, ReplicationConfigurator};
#[cfg(all(feature = "sub_replication", not(reifydb_single_threaded)))]
use reifydb_sub_replication::factory::ReplicationSubsystemFactory;
#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
use reifydb_sub_server::interceptor::RequestInterceptorChain;
use reifydb_sub_store::factory::StorageSubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_subscription::subsystem::SubscriptionSubsystemFactory;
#[cfg(not(reifydb_single_threaded))]
use reifydb_sub_task::factory::{TaskConfig, TaskSubsystemFactory};
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingConfigurator;
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::factory::TracingSubsystemFactory;
use reifydb_transaction::{
	TransactionVersion,
	group::{GroupCommitBegin, GroupCommitHandle},
	interceptor::builder::InterceptorBuilder,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
};
use reifydb_value::value::{Value, identity::IdentityId};

#[cfg(not(reifydb_single_threaded))]
use crate::system::tasks::create_system_tasks;
use crate::{
	MigrationStatement, Result, boot::Bootloader, database::Database, health::HealthMonitor, subsystem::Subsystems,
};

/// Backend selection for the CDC store.
///
/// Defaults to `Memory`. Use `Sqlite(config)` for on-disk, restart-safe CDC
/// (non-wasm32 targets only).
#[derive(Default)]
pub enum CdcBackend {
	#[default]
	Memory,
	#[cfg(not(target_arch = "wasm32"))]
	Sqlite(SqliteConfig),
}

pub struct DatabaseBuilder {
	interceptors: InterceptorBuilder,
	factories: Vec<Box<dyn SubsystemFactory>>,
	ioc: IocContainer,
	runtime: Option<Runtime>,
	routines_configurator: Option<Box<dyn FnOnce(RoutinesConfigurator) -> RoutinesConfigurator + Send + 'static>>,
	handlers_configurator: Option<Box<dyn FnOnce(RoutinesConfigurator) -> RoutinesConfigurator + Send + 'static>>,
	#[cfg(reifydb_target = "native")]
	procedure_dir: Option<PathBuf>,
	wasm_procedure_dir: Option<PathBuf>,
	transforms_configurator:
		Option<Box<dyn FnOnce(TransformsConfigurator) -> TransformsConfigurator + Send + 'static>>,
	multi_store: Option<MultiStore>,
	single_store: Option<SingleStore>,
	#[cfg(feature = "sub_tracing")]
	tracing_factory: Option<Box<dyn SubsystemFactory>>,
	#[cfg(feature = "sub_flow")]
	flow_factory: Option<Box<dyn SubsystemFactory>>,
	#[cfg(feature = "sub_replication")]
	replication_factory: Option<Box<dyn SubsystemFactory>>,
	#[cfg(not(reifydb_single_threaded))]
	task_factory: Option<Box<dyn SubsystemFactory>>,
	auth_configurator: Option<Box<dyn FnOnce(AuthConfigurator) -> AuthConfigurator + Send + 'static>>,
	migrations: Vec<MigrationStatement>,
	is_replica: bool,
	bootstrap_configs: Vec<(ConfigKey, Value)>,
	cdc_backend: CdcBackend,
	fast_shutdown: bool,
}

impl DatabaseBuilder {
	#[allow(unused_mut)]
	pub fn new(
		catalog_cache: CatalogCache,
		multi: MultiTransaction,
		single: SingleTransaction,
		eventbus: EventBus,
	) -> Self {
		let ioc = IocContainer::new()
			.register(catalog_cache)
			.register(eventbus)
			.register(multi)
			.register(single)
			.register(MetricsRegistry::new());

		Self {
			interceptors: InterceptorBuilder::new(),
			factories: Vec::new(),
			ioc,
			runtime: None,
			routines_configurator: None,
			handlers_configurator: None,
			#[cfg(reifydb_target = "native")]
			procedure_dir: None,
			wasm_procedure_dir: None,
			transforms_configurator: None,
			multi_store: None,
			single_store: None,
			#[cfg(feature = "sub_tracing")]
			tracing_factory: None,
			#[cfg(feature = "sub_flow")]
			flow_factory: None,
			#[cfg(feature = "sub_replication")]
			replication_factory: None,
			#[cfg(not(reifydb_single_threaded))]
			task_factory: None,
			auth_configurator: None,
			migrations: Vec::new(),
			is_replica: false,
			bootstrap_configs: Vec::new(),
			cdc_backend: CdcBackend::default(),
			fast_shutdown: false,
		}
	}

	/// Select the CDC storage backend. Defaults to `CdcBackend::Memory`.
	pub fn with_cdc_backend(mut self, backend: CdcBackend) -> Self {
		self.cdc_backend = backend;
		self
	}

	pub fn with_fast_shutdown(mut self) -> Self {
		self.fast_shutdown = true;
		self
	}

	/// Store the underlying MultiStore and SingleStore for metrics worker
	pub fn with_stores(mut self, multi: MultiStore, single: SingleStore) -> Self {
		self.multi_store = Some(multi);
		self.single_store = Some(single);
		self
	}

	#[cfg(feature = "sub_tracing")]
	pub fn with_tracing<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(TracingConfigurator) -> TracingConfigurator + Send + 'static,
	{
		self.tracing_factory = Some(Box::new(TracingSubsystemFactory::with_configurator(configurator)));
		self
	}

	#[cfg(feature = "sub_metric_profiler")]
	pub fn with_profiler<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(ProfilerConfigurator) -> ProfilerConfigurator + Send + 'static,
	{
		self.factories.push(Box::new(ProfilerSubsystemFactory::with_configurator(configurator)));
		self
	}

	#[cfg(feature = "sub_flow")]
	pub fn with_flow<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(FlowConfigurator) -> FlowConfigurator + Send + 'static,
	{
		self.flow_factory = Some(Box::new(FlowSubsystemFactory::with_configurator(configurator)));
		self
	}

	#[cfg(all(feature = "sub_replication", not(reifydb_single_threaded)))]
	pub fn with_replication<F, C>(mut self, configurator: F) -> Self
	where
		F: FnOnce(ReplicationConfigurator) -> C + Send + 'static,
		C: Into<ReplicationConfig> + 'static,
	{
		self.replication_factory = Some(Box::new(ReplicationSubsystemFactory::new(configurator)));
		self
	}

	#[cfg(feature = "sub_replication")]
	pub fn add_replication_factory(mut self, factory: Box<dyn SubsystemFactory>) -> Self {
		self.replication_factory = Some(factory);
		self
	}

	pub fn add_subsystem_factory(mut self, factory: Box<dyn SubsystemFactory>) -> Self {
		self.factories.push(factory);
		self
	}

	pub fn with_dependency<T: Clone + Send + Sync + 'static>(self, value: T) -> Self {
		self.ioc.register_service(value);
		self
	}

	pub fn with_interceptor_builder(mut self, builder: InterceptorBuilder) -> Self {
		self.interceptors = builder;
		self
	}

	#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
	pub fn with_request_interceptor_chain(self, chain: RequestInterceptorChain) -> Self {
		self.ioc.register_service(chain);
		self
	}

	pub fn with_routines_configurator<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(RoutinesConfigurator) -> RoutinesConfigurator + Send + 'static,
	{
		self.routines_configurator = Some(Box::new(configurator));
		self
	}

	pub fn with_handlers_configurator<F>(mut self, configurator: F) -> Self
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

	pub fn with_wasm_procedure_dir(mut self, dir: impl Into<PathBuf>) -> Self {
		self.wasm_procedure_dir = Some(dir.into());
		self
	}

	pub fn with_transforms<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(TransformsConfigurator) -> TransformsConfigurator + Send + 'static,
	{
		self.transforms_configurator = Some(Box::new(configurator));
		self
	}

	/// Provide the owned process runtime.
	///
	/// The builder derives the narrow handles (clock, rng, actor spawner, tokio
	/// handle) from it, registers those in the IoC container, then hands the
	/// owned runtime to the `Database`, which shuts it down immediately after
	/// every subsystem has stopped and before the stores are torn down.
	pub fn with_runtime(mut self, runtime: Runtime) -> Self {
		self.runtime = Some(runtime);
		self
	}

	pub fn with_auth<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(AuthConfigurator) -> AuthConfigurator + Send + 'static,
	{
		self.auth_configurator = Some(Box::new(configurator));
		self
	}

	pub fn with_migrations(mut self, migrations: Vec<MigrationStatement>) -> Self {
		self.migrations = migrations;
		self
	}

	/// Set a system configuration value to apply during bootstrap.
	///
	/// Applied on every `build()` after catalog load and system-object bootstrap,
	/// overwriting any previously persisted override for this key. Skipped on replicas.
	pub fn with_config(mut self, key: ConfigKey, value: Value) -> Self {
		self.bootstrap_configs.push((key, value));
		self
	}

	/// Set multiple system configuration values to apply during bootstrap.
	pub fn with_configs(mut self, configs: impl IntoIterator<Item = (ConfigKey, Value)>) -> Self {
		self.bootstrap_configs.extend(configs);
		self
	}

	pub fn is_replica(mut self) -> Self {
		self.is_replica = true;
		self
	}

	pub fn subsystem_count(&self) -> usize {
		self.factories.len()
	}

	pub fn build(mut self) -> Result<Database> {
		#[cfg(reifydb_assertions)]
		self.ioc.resolve::<CatalogCache>()?
			.mark_pending_config_overrides(self.bootstrap_configs.iter().map(|(key, _)| *key));

		// Collect interceptors from all factories
		// Note: We process logging and flow factories separately before adding to self.factories

		#[cfg(feature = "sub_tracing")]
		if let Some(ref factory) = self.tracing_factory {
			self.interceptors = factory.provide_interceptors(self.interceptors, &self.ioc);
		}

		#[cfg(feature = "sub_flow")]
		if let Some(ref factory) = self.flow_factory {
			self.interceptors = factory.provide_interceptors(self.interceptors, &self.ioc);
		}

		#[cfg(feature = "sub_replication")]
		if let Some(ref factory) = self.replication_factory {
			self.interceptors = factory.provide_interceptors(self.interceptors, &self.ioc);
		}

		#[cfg(not(reifydb_single_threaded))]
		if let Some(ref factory) = self.task_factory {
			self.interceptors = factory.provide_interceptors(self.interceptors, &self.ioc);
		}

		for factory in &self.factories {
			self.interceptors = factory.provide_interceptors(self.interceptors, &self.ioc);
		}

		let catalog = self.ioc.resolve::<CatalogCache>()?;
		let multi = self.ioc.resolve::<MultiTransaction>()?;
		let single = self.ioc.resolve::<SingleTransaction>()?;
		let eventbus = self.ioc.resolve::<EventBus>()?;

		load_catalog_cache(&multi, &single, &catalog)?;

		if !self.is_replica {
			seed_bootstrap_configs(&multi, &catalog, &self.bootstrap_configs)?;
		}
		#[cfg(reifydb_assertions)]
		if self.is_replica {
			catalog.clear_pending_config_overrides();
		}

		// Bootstrap complete - clear conflict window so bootstrap entries
		// don't participate in conflict detection.
		multi.bootstrapping_completed();

		let runtime = self.runtime.take().expect("Runtime must be set via with_runtime()");
		let spawner = runtime.spawner();
		let clock = runtime.clock().clone();
		let rng = runtime.rng().clone();
		let runtime_handle = runtime.handle();
		#[cfg(not(reifydb_single_threaded))]
		let tokio_handle = runtime.tokio();

		self.ioc =
			self.ioc.register(spawner.clone())
				.register(clock.clone())
				.register(rng.clone())
				.register(runtime_handle.clone());
		#[cfg(not(reifydb_single_threaded))]
		{
			self.ioc = self.ioc.register(tokio_handle.clone());
		}

		// Create and register CdcStore for CDC storage.
		let cdc_recent_cache_capacity =
			multi.config().get_config_uint8(ConfigKey::CdcRecentCacheCapacity) as usize;
		let cdc_store = match self.cdc_backend {
			CdcBackend::Memory => CdcStore::memory(),
			#[cfg(not(target_arch = "wasm32"))]
			CdcBackend::Sqlite(config) => CdcStore::sqlite(config, cdc_recent_cache_capacity),
		};
		self.ioc = self.ioc.register(cdc_store.clone());

		// Shared CDC producer commit watermark. Producer advances it after
		// processing each PostCommitEvent; the compactor caps its eligible
		// range by it so no in-flight write can land at a version already
		// covered by a packed block. Registered in IoC so consumers can
		// observe "producer caught up to V" - the watermark advances even
		// for commits that produce no CDC row (e.g. ConfigStorage-only
		// commits filtered out by `should_exclude_from_cdc`).
		let cdc_producer_watermark = CdcProducerWatermark::new();
		self.ioc = self.ioc.register(cdc_producer_watermark.clone());

		let cdc_wake_registry = CdcWakeRegistry::new();
		self.ioc = self.ioc.register(cdc_wake_registry.clone());

		// Spawn the CDC compaction actor (sqlite only). Settings come from
		// system config (CDC_COMPACT_INTERVAL etc.) so they can be tuned at
		// runtime via SET CONFIG.
		#[cfg(not(target_arch = "wasm32"))]
		if let CdcStore::Sqlite(ref cached_store) = cdc_store {
			let provider = multi.config();
			let actor = CompactActor::new(
				provider,
				cached_store.inner().clone(),
				cdc_producer_watermark.clone(),
			);
			let cdc_compact_handle = spawner.spawn_coordination("cdc-compact", actor);
			self.ioc = self.ioc.register(cdc_compact_handle.actor_ref().clone());
		}

		// Get the underlying stores for workers
		let multi_store = self.multi_store.clone().expect("MultiStore must be set via with_stores()");
		let single_store = self.single_store.clone().expect("SingleStore must be set via with_stores()");

		self.ioc = self.ioc.register(single_store.clone());
		self.ioc = self.ioc.register(multi_store.clone());

		let metrics_registry = self.ioc.resolve::<MetricsRegistry>()?;
		metrics_registry.register_collectors(multi_store.metrics_collectors());
		metrics_registry.register_collectors(single_store.metrics_collectors());
		metrics_registry.register_collectors(cdc_store.metrics_collectors());

		let transforms = if let Some(configurator) = self.transforms_configurator {
			configurator(Transforms::builder()).configure()
		} else {
			Transforms::empty()
		};

		let routines = {
			let mut routines_builder = Routines::builder();
			routines_builder = default_native_functions(routines_builder);
			routines_builder = default_native_procedures(routines_builder);
			routines_builder = default_native_monoids(routines_builder);

			#[cfg(reifydb_target = "native")]
			if let Some(dir) = &self.procedure_dir {
				routines_builder = register_procedures_from_dir(dir, routines_builder)?;
			}

			if let Some(dir) = &self.wasm_procedure_dir {
				routines_builder = register_wasm_procedures_from_dir(dir, routines_builder)?;
			}

			if let Some(configurator) = self.routines_configurator {
				routines_builder = configurator(routines_builder);
			}

			if let Some(configurator) = self.handlers_configurator {
				routines_builder = configurator(routines_builder);
			}

			routines_builder.configure()
		};

		// Create RemoteRegistry for forwarding queries to remote namespaces
		#[cfg(not(reifydb_single_threaded))]
		let remote_registry = RemoteRegistry::new(tokio_handle.clone());

		// Create engine and CDC producer BEFORE bootstrap so that bootstrap
		// commits produce CDC entries (PostCommitEvent is captured).
		let engine = StandardEngine::new(
			multi.clone(),
			single.clone(),
			eventbus.clone(),
			self.interceptors.build(),
			Catalog::new(catalog.clone()),
			EngineConfig {
				runtime_context: RuntimeContext::new(clock.clone(), rng.clone()),
				routines,
				transforms,
				ioc: self.ioc.clone(),
				#[cfg(not(reifydb_single_threaded))]
				remote_registry: Some(remote_registry),
			},
		);

		self.ioc = self.ioc.register(engine.clone());

		// Create AuthService for token validation
		let auth_service = AuthService::new(
			Arc::new(engine.clone()),
			Arc::new(AuthenticationRegistry::new(clock.clone())),
			rng.clone(),
			clock.clone(),
			match self.auth_configurator {
				Some(configurator) => configurator(AuthConfigurator::new()).configure(),
				None => AuthServiceConfig::default(),
			},
		);
		self.ioc = self.ioc.register(auth_service.clone());

		// Spawn CDC producer and register PostCommitEvent listener BEFORE
		// bootstrap so that bootstrap commits generate CDC entries.
		let cdc_handle = spawn_cdc_producer(
			&spawner,
			cdc_store,
			multi_store,
			engine.clone(),
			eventbus.clone(),
			clock.clone(),
			cdc_producer_watermark,
			cdc_wake_registry,
		);
		eventbus.register::<PostCommitEvent, _>(CdcProducerEventListener::new(
			cdc_handle.actor_ref().clone(),
			clock.clone(),
		));
		eventbus.register::<PostCommitEvent, _>(VersionEpochListener::new(
			engine.version_epoch().clone(),
			clock.clone(),
		));
		self.ioc.register_service::<Arc<CdcProduceHandle>>(Arc::new(cdc_handle));

		// Bootstrap AFTER CDC producer is active so commits are captured.
		if !self.is_replica {
			bootstrap_system_objects(&multi, &single, &catalog, &eventbus)?;
			apply_bootstrap_configs(&multi, &single, &catalog, &eventbus, &self.bootstrap_configs)?;
		}

		let bootloader = Bootloader::new(engine.clone(), spawner.clone());
		bootloader.load()?;
		bootloader.apply_migrations(&self.migrations)?;

		let group_commit = {
			let begin_engine = engine.clone();
			let begin: GroupCommitBegin =
				Arc::new(move || begin_engine.begin_command(IdentityId::system()));
			match engine.catalog().get_config_duration_opt(ConfigKey::CommitGroupLinger) {
				Some(linger) => GroupCommitHandle::spawn(
					&spawner,
					begin,
					linger,
					engine.catalog().get_config_uint8(ConfigKey::CommitGroupMaxEntries) as usize,
				),
				None => GroupCommitHandle::inline(begin),
			}
		};
		self.ioc = self.ioc.register(group_commit);

		// Collect all versions
		let mut all_versions = vec![
			SystemVersion {
				name: "reifydb".to_string(),
				version: env!("CARGO_PKG_VERSION").to_string(),
				description: "ReifyDB Database System".to_string(),
				r#type: ComponentType::Package,
			},
			CoreVersion.version(),
			EngineVersion.version(),
			CatalogVersion.version(),
			MultiStoreVersion.version(),
			SingleStoreVersion.version(),
			TransactionVersion.version(),
			AuthVersion.version(),
			RqlVersion.version(),
			CdcVersion.version(),
		];

		// Create subsystems from factories and collect their versions
		// IMPORTANT: Order matters for shutdown! Subsystems are stopped in REVERSE order.
		// Add logging FIRST so it's stopped LAST and can log shutdown messages from other subsystems.
		let health_monitor = Arc::new(HealthMonitor::new(clock.clone()));
		let mut subsystems = Subsystems::new(Arc::clone(&health_monitor));

		{
			let factory = Box::new(MetricsSubsystemFactory::new());
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		#[cfg(feature = "sub_tracing")]
		if let Some(factory) = self.tracing_factory {
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		#[cfg(feature = "sub_flow")]
		if let Some(factory) = self.flow_factory {
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		#[cfg(feature = "sub_flow")]
		{
			let factory = Box::new(SubscriptionSubsystemFactory);
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		#[cfg(feature = "sub_replication")]
		if let Some(factory) = self.replication_factory {
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		#[cfg(not(reifydb_single_threaded))]
		{
			let factory = self.task_factory.unwrap_or_else(|| {
				Box::new(TaskSubsystemFactory::with_config(TaskConfig::new(create_system_tasks())))
			});
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		{
			let factory: Box<dyn SubsystemFactory> = Box::new(StorageSubsystemFactory::default());
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		for factory in self.factories {
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		if let Some(git_hash) = option_env!("GIT_HASH") {
			all_versions.push(SystemVersion {
				name: "git-hash".to_string(),
				version: git_hash.to_string(),
				description: "Git commit hash at build time".to_string(),
				r#type: ComponentType::Build,
			});
		}

		let system_catalog = SystemCatalog::new(all_versions);
		self.ioc.register(system_catalog);

		Ok(Database::new(
			engine,
			auth_service,
			subsystems,
			health_monitor,
			spawner,
			clock,
			runtime_handle,
			runtime,
		)
		.fast_shutdown_on_drop(self.fast_shutdown))
	}
}
