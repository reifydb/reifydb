// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{path::PathBuf, sync::Arc};

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
	produce::producer::{CdcProducerEventListener, spawn_cdc_producer},
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
	util::ioc::IocContainer,
};
use reifydb_engine::{
	EngineVersion,
	engine::StandardEngine,
	procedure::registry::{Procedures, ProceduresBuilder},
	transform::registry::Transforms,
};
use reifydb_function::registry::{Functions, FunctionsBuilder};
use reifydb_metric::worker::{
	CdcStatsDroppedListener, CdcStatsListener, MetricsWorker, MetricsWorkerConfig, StorageStatsListener,
};
use reifydb_rql::RqlVersion;
use reifydb_runtime::{SharedRuntime, actor::system::ActorSystem};
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
	TransactionVersion,
	interceptor::builder::InterceptorBuilder,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{Transaction, query::QueryTransaction},
};
use tracing::debug;

use crate::{
	Migration, database::Database, health::HealthMonitor, subsystem::Subsystems, system::tasks::create_system_tasks,
};

pub struct DatabaseBuilder {
	interceptors: InterceptorBuilder,
	factories: Vec<Box<dyn SubsystemFactory>>,
	ioc: IocContainer,
	actor_system: Option<ActorSystem>,
	functions_configurator: Option<Box<dyn FnOnce(FunctionsBuilder) -> FunctionsBuilder + Send + 'static>>,
	procedures_configurator: Option<Box<dyn FnOnce(ProceduresBuilder) -> ProceduresBuilder + Send + 'static>>,
	handlers_configurator: Option<Box<dyn FnOnce(ProceduresBuilder) -> ProceduresBuilder + Send + 'static>>,
	#[cfg(reifydb_target = "native")]
	procedure_dir: Option<PathBuf>,
	wasm_procedure_dir: Option<PathBuf>,
	transforms: Option<Transforms>,
	multi_store: Option<MultiStore>,
	single_store: Option<SingleStore>,
	#[cfg(feature = "sub_tracing")]
	tracing_factory: Option<Box<dyn SubsystemFactory>>,
	#[cfg(feature = "sub_flow")]
	flow_factory: Option<Box<dyn SubsystemFactory>>,
	task_factory: Option<Box<dyn SubsystemFactory>>,
	migrations: Vec<Migration>,
}

impl DatabaseBuilder {
	#[allow(unused_mut)]
	pub fn new(multi: MultiTransaction, single: SingleTransaction, eventbus: EventBus) -> Self {
		let ioc = IocContainer::new()
			.register(MaterializedCatalog::new())
			.register(SchemaRegistry::new(single.clone()))
			.register(eventbus)
			.register(multi)
			.register(single);

		Self {
			interceptors: InterceptorBuilder::new(),
			factories: Vec::new(),
			ioc,
			actor_system: None,
			functions_configurator: None,
			procedures_configurator: None,
			handlers_configurator: None,
			#[cfg(reifydb_target = "native")]
			procedure_dir: None,
			wasm_procedure_dir: None,
			transforms: None,
			multi_store: None,
			single_store: None,
			#[cfg(feature = "sub_tracing")]
			tracing_factory: None,
			#[cfg(feature = "sub_flow")]
			flow_factory: None,
			task_factory: None,
			migrations: Vec::new(),
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

	pub fn with_interceptor_builder(mut self, builder: InterceptorBuilder) -> Self {
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

	pub fn with_procedures_configurator<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(ProceduresBuilder) -> ProceduresBuilder + Send + 'static,
	{
		self.procedures_configurator = Some(Box::new(configurator));
		self
	}

	pub fn with_handlers_configurator<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(ProceduresBuilder) -> ProceduresBuilder + Send + 'static,
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

	pub fn with_transforms(mut self, transforms: Transforms) -> Self {
		self.transforms = Some(transforms);
		self
	}

	/// Set the shared runtime for the database.
	///
	/// This registers the runtime in the IoC container so subsystems can resolve it.
	pub fn with_runtime(mut self, runtime: SharedRuntime) -> Self {
		self.ioc = self.ioc.register(runtime);
		self
	}

	pub fn with_actor_system(mut self, system: ActorSystem) -> Self {
		self.actor_system = Some(system);
		self
	}

	pub fn with_migrations(mut self, migrations: Vec<Migration>) -> Self {
		self.migrations = migrations;
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
		let multi = self.ioc.resolve::<MultiTransaction>()?;
		let single = self.ioc.resolve::<SingleTransaction>()?;
		let eventbus = self.ioc.resolve::<EventBus>()?;

		Self::load_materialized_catalog(&multi, &single, &catalog)?;
		Self::load_schema_registry(&multi, &single, &schema_registry)?;

		let runtime = self.ioc.resolve::<SharedRuntime>()?;
		let actor_system = self.actor_system.unwrap_or_else(|| runtime.actor_system().scope());

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

		let default_builder = Functions::defaults();

		let functions = if let Some(configurator) = self.functions_configurator {
			configurator(default_builder).build()
		} else {
			default_builder.build()
		};

		let transforms = self.transforms.unwrap_or_else(Transforms::empty);

		let procedures = {
			let mut procedures_builder = Procedures::builder().with_procedure(
				"identity::inject",
				reifydb_engine::procedure::identity_inject::IdentityInject::new,
			);

			#[cfg(reifydb_target = "native")]
			if let Some(dir) = &self.procedure_dir {
				procedures_builder = reifydb_engine::procedure::loader::register_procedures_from_dir(
					dir,
					procedures_builder,
				)?;
			}

			if let Some(dir) = &self.wasm_procedure_dir {
				procedures_builder =
					reifydb_engine::procedure::wasm_loader::register_wasm_procedures_from_dir(
						dir,
						procedures_builder,
					)?;
			}

			if let Some(configurator) = self.procedures_configurator {
				procedures_builder = configurator(procedures_builder);
			}

			if let Some(configurator) = self.handlers_configurator {
				procedures_builder = configurator(procedures_builder);
			}

			procedures_builder =
				procedures_builder.resolve(&catalog).map_err(|e| reifydb_core::internal_error!(e))?;

			procedures_builder.build()
		};

		// Create engine before CDC worker (CDC worker needs engine for cleanup)
		let engine = StandardEngine::new(
			multi.clone(),
			single.clone(),
			eventbus.clone(),
			self.interceptors.build(),
			Catalog::new(catalog, schema_registry),
			runtime.clock().clone(),
			functions,
			procedures,
			transforms,
			self.ioc.clone(),
		);

		self.ioc = self.ioc.register(engine.clone());

		// Spawn CDC producer actor and register event listener
		// The handle is stored in IoC to keep it alive for the database lifetime
		// Engine is passed for periodic cleanup based on consumer watermarks
		let cdc_handle =
			spawn_cdc_producer(&actor_system, cdc_store, multi_store, engine.clone(), eventbus.clone());
		eventbus.register::<PostCommitEvent, _>(CdcProducerEventListener::new(
			cdc_handle.actor_ref().clone(),
			runtime.clock().clone(),
		));
		self.ioc.register_service::<Arc<reifydb_runtime::actor::system::ActorHandle<reifydb_cdc::produce::producer::CdcProduceMsg>>>(Arc::new(cdc_handle));

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

		Ok(Database::new(engine, subsystems, health_monitor, runtime, actor_system, self.migrations))
	}

	/// Load the materialized catalog from storage
	fn load_materialized_catalog(
		multi: &MultiTransaction,
		single: &SingleTransaction,
		catalog: &MaterializedCatalog,
	) -> crate::Result<()> {
		let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone());

		debug!("Loading materialized catalog");
		MaterializedCatalogLoader::load_all(&mut Transaction::Query(&mut qt), catalog)?;

		Ok(())
	}

	/// Load the schema registry from storage
	fn load_schema_registry(
		multi: &MultiTransaction,
		single: &SingleTransaction,
		registry: &SchemaRegistry,
	) -> crate::Result<()> {
		let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone());

		debug!("Loading schema registry");
		SchemaRegistryLoader::load_all(&mut Transaction::Query(&mut qt), registry)?;

		Ok(())
	}
}
