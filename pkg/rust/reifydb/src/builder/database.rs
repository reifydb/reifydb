// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_auth::AuthVersion;
use reifydb_catalog::{
	CatalogVersion,
	catalog::Catalog,
	materialized::{MaterializedCatalog, load::MaterializedCatalogLoader},
	schema::{SchemaRegistry, load::SchemaRegistryLoader},
	system::SystemCatalog,
};
use reifydb_cdc::{
	CdcVersion,
	produce::{listener::CdcEventListener, worker::CdcWorker},
	storage::CdcStore,
};
use reifydb_core::{
	CoreVersion,
	event::{
		EventBus,
		metric::{CdcStatsDroppedEvent, CdcStatsRecordedEvent, StorageStatsRecordedEvent},
		transaction::PostCommitEvent,
	},
	interface::version::{ComponentType, HasVersion, SystemVersion},
	runtime::SharedRuntime,
	util::ioc::IocContainer,
};
use reifydb_engine::{EngineVersion, engine::StandardEngine};
use reifydb_function::{
	math,
	registry::{Functions, FunctionsBuilder},
	series,
};
use reifydb_metric::worker::{
	CdcStatsDroppedListener, CdcStatsListener, MetricsWorker, MetricsWorkerConfig, StorageStatsListener,
};
use reifydb_rql::RqlVersion;
use reifydb_rqlv2::{self, compiler::Compiler};
use reifydb_store_multi::{MultiStore, MultiStoreVersion};
use reifydb_store_single::{SingleStore, SingleStoreVersion};
use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::{builder::FlowBuilder, subsystem::factory::FlowSubsystemFactory};
use reifydb_sub_task::factory::{TaskConfig, TaskSubsystemFactory};
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingBuilder;
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::factory::TracingSubsystemFactory;
use reifydb_transaction::{
	TransactionVersion, interceptor::builder::StandardInterceptorBuilder, multi::transaction::TransactionMulti,
	single::TransactionSingle, standard::query::StandardQueryTransaction,
};
use tracing::debug;

use crate::{database::Database, health::HealthMonitor, subsystem::Subsystems, system::tasks::create_system_tasks};

pub struct DatabaseBuilder {
	interceptors: StandardInterceptorBuilder,
	factories: Vec<Box<dyn SubsystemFactory>>,
	ioc: IocContainer,
	functions_configurator: Option<Box<dyn FnOnce(FunctionsBuilder) -> FunctionsBuilder + Send + 'static>>,
	multi_store: Option<MultiStore>,
	single_store: Option<SingleStore>,
	#[cfg(feature = "sub_tracing")]
	tracing_factory: Option<Box<dyn SubsystemFactory>>,
	#[cfg(feature = "sub_flow")]
	flow_factory: Option<Box<dyn SubsystemFactory>>,
	task_factory: Option<Box<dyn SubsystemFactory>>,
}

impl DatabaseBuilder {
	#[allow(unused_mut)]
	pub fn new(multi: TransactionMulti, single: TransactionSingle, eventbus: EventBus) -> Self {
		let ioc = IocContainer::new()
			.register(MaterializedCatalog::new())
			.register(SchemaRegistry::new(single.clone()))
			.register(eventbus)
			.register(multi)
			.register(single);

		Self {
			interceptors: StandardInterceptorBuilder::new(),
			factories: Vec::new(),
			ioc,
			functions_configurator: None,
			multi_store: None,
			single_store: None,
			#[cfg(feature = "sub_tracing")]
			tracing_factory: None,
			#[cfg(feature = "sub_flow")]
			flow_factory: None,
			task_factory: None,
		}
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
		F: FnOnce(TracingBuilder) -> TracingBuilder + Send + 'static,
	{
		self.tracing_factory = Some(Box::new(TracingSubsystemFactory::with_configurator(configurator)));
		self
	}

	#[cfg(feature = "sub_flow")]
	pub fn with_flow<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(FlowBuilder) -> FlowBuilder + Send + 'static,
	{
		self.flow_factory = Some(Box::new(FlowSubsystemFactory::with_configurator(configurator)));
		self
	}

	pub fn add_subsystem_factory(mut self, factory: Box<dyn SubsystemFactory>) -> Self {
		self.factories.push(factory);
		self
	}

	pub fn with_interceptor_builder(mut self, builder: StandardInterceptorBuilder) -> Self {
		self.interceptors = builder;
		self
	}

	pub fn with_functions_configurator<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(FunctionsBuilder) -> FunctionsBuilder + Send + 'static,
	{
		self.functions_configurator = Some(Box::new(configurator));
		self
	}

	/// Set the shared runtime for the database.
	///
	/// This registers the runtime in the IoC container so subsystems can resolve it.
	pub fn with_runtime(mut self, runtime: SharedRuntime) -> Self {
		self.ioc = self.ioc.register(runtime);
		self
	}

	pub fn subsystem_count(&self) -> usize {
		self.factories.len()
	}

	pub fn build(mut self) -> crate::Result<Database> {
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

		if let Some(ref factory) = self.task_factory {
			self.interceptors = factory.provide_interceptors(self.interceptors, &self.ioc);
		}

		for factory in &self.factories {
			self.interceptors = factory.provide_interceptors(self.interceptors, &self.ioc);
		}

		let catalog = self.ioc.resolve::<MaterializedCatalog>()?;
		let schema_registry = self.ioc.resolve::<SchemaRegistry>()?;
		let multi = self.ioc.resolve::<TransactionMulti>()?;
		let single = self.ioc.resolve::<TransactionSingle>()?;
		let eventbus = self.ioc.resolve::<EventBus>()?;

		Self::load_materialized_catalog(&multi, &single, &catalog)?;
		Self::load_schema_registry(&multi, &single, &schema_registry)?;

		// Create and register Compiler (requires SharedRuntime to be registered first)
		let runtime = self.ioc.resolve::<SharedRuntime>()?;
		let compiler = Compiler::new(catalog.clone());
		self.ioc = self.ioc.register(compiler);

		// Create and register CdcStore for CDC storage
		let cdc_store = CdcStore::memory();
		self.ioc = self.ioc.register(cdc_store.clone());

		// Get the underlying stores for workers
		let multi_store = self.multi_store.clone().expect("MultiStore must be set via with_stores()");
		let single_store = self.single_store.clone().expect("SingleStore must be set via with_stores()");

		// Create metrics worker and register event listeners
		let metrics_worker = Arc::new(MetricsWorker::new(
			MetricsWorkerConfig::default(),
			single_store.clone(),
			multi_store.clone(),
			eventbus.clone(),
		));
		eventbus.register::<StorageStatsRecordedEvent, _>(StorageStatsListener::new(metrics_worker.sender()));
		eventbus.register::<CdcStatsRecordedEvent, _>(CdcStatsListener::new(metrics_worker.sender()));
		eventbus.register::<CdcStatsDroppedEvent, _>(CdcStatsDroppedListener::new(metrics_worker.sender()));
		self.ioc.register_service::<Arc<MetricsWorker>>(metrics_worker);

		// Register single store in IoC for engine to access
		self.ioc = self.ioc.register(single_store);

		let functions = if let Some(configurator) = self.functions_configurator {
			let default_builder = Functions::builder()
				.register_aggregate("math::sum", math::aggregate::sum::Sum::new)
				.register_aggregate("math::min", math::aggregate::min::Min::new)
				.register_aggregate("math::max", math::aggregate::max::Max::new)
				.register_aggregate("math::avg", math::aggregate::avg::Avg::new)
				.register_aggregate("math::count", math::aggregate::count::Count::new)
				.register_scalar("math::abs", math::scalar::abs::Abs::new)
				.register_scalar("math::avg", math::scalar::avg::Avg::new)
				.register_generator("generate_series", series::GenerateSeries::new);

			Some(configurator(default_builder).build())
		} else {
			None
		};

		// Create engine before CDC worker (CDC worker needs engine for cleanup)
		let engine = StandardEngine::new(
			multi.clone(),
			single.clone(),
			eventbus.clone(),
			Box::new(self.interceptors.build()),
			Catalog::new(catalog, schema_registry),
			functions,
			self.ioc.clone(),
		);

		self.ioc = self.ioc.register(engine.clone());

		// Create CDC worker and register event listener
		// The worker is stored in IoC to keep it alive for the database lifetime
		// Engine is passed for periodic cleanup based on consumer watermarks
		let cdc_worker = Arc::new(CdcWorker::spawn(cdc_store, multi_store, eventbus.clone(), engine.clone()));
		eventbus.register::<PostCommitEvent, _>(CdcEventListener::new(cdc_worker.sender()));
		self.ioc.register_service::<Arc<CdcWorker>>(cdc_worker);

		// Collect all versions
		let mut all_versions = Vec::new();
		all_versions.push(SystemVersion {
			name: "reifydb".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "ReifyDB Database System".to_string(),
			r#type: ComponentType::Package,
		});

		all_versions.push(CoreVersion.version());
		all_versions.push(EngineVersion.version());
		all_versions.push(CatalogVersion.version());
		all_versions.push(MultiStoreVersion.version());
		all_versions.push(SingleStoreVersion.version());
		all_versions.push(TransactionVersion.version());
		all_versions.push(AuthVersion.version());
		all_versions.push(RqlVersion.version());
		all_versions.push(CdcVersion.version());

		// Create subsystems from factories and collect their versions
		// IMPORTANT: Order matters for shutdown! Subsystems are stopped in REVERSE order.
		// Add logging FIRST so it's stopped LAST and can log shutdown messages from other subsystems.
		let health_monitor = Arc::new(HealthMonitor::new());
		let mut subsystems = Subsystems::new(Arc::clone(&health_monitor));

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

		{
			let factory = self.task_factory.unwrap_or_else(|| {
				Box::new(TaskSubsystemFactory::with_config(TaskConfig::new(create_system_tasks())))
			});
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

		Ok(Database::new(engine, subsystems, health_monitor, runtime))
	}

	/// Load the materialized catalog from storage
	fn load_materialized_catalog(
		multi: &TransactionMulti,
		single: &TransactionSingle,
		catalog: &MaterializedCatalog,
	) -> crate::Result<()> {
		let mut qt = StandardQueryTransaction::new(multi.begin_query()?, single.clone());

		debug!("Loading materialized catalog");
		MaterializedCatalogLoader::load_all(&mut qt, catalog)?;

		Ok(())
	}

	/// Load the schema registry from storage
	fn load_schema_registry(
		multi: &TransactionMulti,
		single: &TransactionSingle,
		registry: &SchemaRegistry,
	) -> crate::Result<()> {
		let mut qt = StandardQueryTransaction::new(multi.begin_query()?, single.clone());

		debug!("Loading schema registry");
		SchemaRegistryLoader::load_all(&mut qt, registry)?;

		Ok(())
	}
}
