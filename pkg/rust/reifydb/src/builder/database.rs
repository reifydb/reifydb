// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{path::PathBuf, sync::Arc};

use reifydb_auth::{
	AuthVersion,
	registry::AuthenticationRegistry,
	service::{AuthConfigurator, AuthService, AuthServiceConfig},
};
use reifydb_catalog::{
	CatalogVersion,
	bootstrap::{bootstrap_system_objects, load_materialized_catalog},
	catalog::Catalog,
	materialized::MaterializedCatalog,
	system::SystemCatalog,
};
use reifydb_cdc::{
	CdcVersion,
	produce::producer::{CdcProducerEventListener, spawn_cdc_producer},
	storage::CdcStore,
};
use reifydb_core::{
	CoreVersion,
	actors::cdc::CdcProduceHandle,
	event::{EventBus, transaction::PostCommitEvent},
	interface::version::{ComponentType, HasVersion, SystemVersion},
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
	function::{default_functions, registry::FunctionsConfigurator},
	procedure::{default_procedures, registry::ProceduresConfigurator},
};
use reifydb_rql::RqlVersion;
use reifydb_runtime::{SharedRuntime, actor::system::ActorSystem, context::RuntimeContext};
use reifydb_store_multi::{MultiStore, MultiStoreVersion};
use reifydb_store_single::{SingleStore, SingleStoreVersion};
use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::{builder::FlowConfigurator, subsystem::factory::FlowSubsystemFactory};
#[cfg(feature = "sub_replication")]
use reifydb_sub_replication::builder::{ReplicationConfig, ReplicationConfigurator};
#[cfg(all(feature = "sub_replication", not(reifydb_single_threaded)))]
use reifydb_sub_replication::factory::ReplicationSubsystemFactory;
#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
use reifydb_sub_server::interceptor::RequestInterceptorChain;
#[cfg(feature = "sub_flow")]
use reifydb_sub_subscription::subsystem::SubscriptionSubsystemFactory;
#[cfg(not(reifydb_single_threaded))]
use reifydb_sub_task::factory::{TaskConfig, TaskSubsystemFactory};
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingConfigurator;
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::factory::TracingSubsystemFactory;
use reifydb_transaction::{
	TransactionVersion, interceptor::builder::InterceptorBuilder, multi::transaction::MultiTransaction,
	single::SingleTransaction,
};

#[cfg(not(reifydb_single_threaded))]
use crate::system::tasks::create_system_tasks;
use crate::{Migration, Result, database::Database, health::HealthMonitor, subsystem::Subsystems};

pub struct DatabaseBuilder {
	interceptors: InterceptorBuilder,
	factories: Vec<Box<dyn SubsystemFactory>>,
	ioc: IocContainer,
	actor_system: Option<ActorSystem>,
	functions_configurator:
		Option<Box<dyn FnOnce(FunctionsConfigurator) -> FunctionsConfigurator + Send + 'static>>,
	procedures_configurator:
		Option<Box<dyn FnOnce(ProceduresConfigurator) -> ProceduresConfigurator + Send + 'static>>,
	handlers_configurator:
		Option<Box<dyn FnOnce(ProceduresConfigurator) -> ProceduresConfigurator + Send + 'static>>,
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
	migrations: Vec<Migration>,
	is_replica: bool,
}

impl DatabaseBuilder {
	#[allow(unused_mut)]
	pub fn new(
		materialized_catalog: MaterializedCatalog,
		multi: MultiTransaction,
		single: SingleTransaction,
		eventbus: EventBus,
	) -> Self {
		let ioc = IocContainer::new()
			.register(materialized_catalog)
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
		F: FnOnce(TracingConfigurator) -> TracingConfigurator + Send + 'static,
	{
		self.tracing_factory = Some(Box::new(TracingSubsystemFactory::with_configurator(configurator)));
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

	pub fn with_interceptor_builder(mut self, builder: InterceptorBuilder) -> Self {
		self.interceptors = builder;
		self
	}

	#[cfg(all(feature = "sub_server", not(reifydb_single_threaded)))]
	pub fn with_request_interceptor_chain(self, chain: RequestInterceptorChain) -> Self {
		self.ioc.register_service(chain);
		self
	}

	pub fn with_functions_configurator<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(FunctionsConfigurator) -> FunctionsConfigurator + Send + 'static,
	{
		self.functions_configurator = Some(Box::new(configurator));
		self
	}

	pub fn with_procedures_configurator<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(ProceduresConfigurator) -> ProceduresConfigurator + Send + 'static,
	{
		self.procedures_configurator = Some(Box::new(configurator));
		self
	}

	pub fn with_handlers_configurator<F>(mut self, configurator: F) -> Self
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

	pub fn is_replica(mut self) -> Self {
		self.is_replica = true;
		self
	}

	pub fn subsystem_count(&self) -> usize {
		self.factories.len()
	}

	pub fn build(mut self) -> Result<Database> {
		let default_builder = default_functions();
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

		let catalog = self.ioc.resolve::<MaterializedCatalog>()?;
		let multi = self.ioc.resolve::<MultiTransaction>()?;
		let single = self.ioc.resolve::<SingleTransaction>()?;
		let eventbus = self.ioc.resolve::<EventBus>()?;

		load_materialized_catalog(&multi, &single, &catalog)?;

		// Bootstrap complete — clear conflict window so bootstrap entries
		// don't participate in conflict detection.
		multi.bootstrapping_completed();

		let runtime = self.ioc.resolve::<SharedRuntime>()?;
		let actor_system = self.actor_system.unwrap_or_else(|| runtime.actor_system().scope());

		// Create and register CdcStore for CDC storage
		let cdc_store = CdcStore::memory();
		self.ioc = self.ioc.register(cdc_store.clone());

		// Get the underlying stores for workers
		let multi_store = self.multi_store.clone().expect("MultiStore must be set via with_stores()");
		let single_store = self.single_store.clone().expect("SingleStore must be set via with_stores()");

		self.ioc = self.ioc.register(single_store);
		self.ioc = self.ioc.register(multi_store.clone());

		let functions = if let Some(configurator) = self.functions_configurator {
			configurator(default_builder).configure()
		} else {
			default_builder.configure()
		};

		let transforms = if let Some(configurator) = self.transforms_configurator {
			configurator(Transforms::builder()).configure()
		} else {
			Transforms::empty()
		};

		let procedures = {
			let mut procedures_builder = default_procedures();

			#[cfg(reifydb_target = "native")]
			if let Some(dir) = &self.procedure_dir {
				procedures_builder = register_procedures_from_dir(dir, procedures_builder)?;
			}

			if let Some(dir) = &self.wasm_procedure_dir {
				procedures_builder = register_wasm_procedures_from_dir(dir, procedures_builder)?;
			}

			if let Some(configurator) = self.procedures_configurator {
				procedures_builder = configurator(procedures_builder);
			}

			if let Some(configurator) = self.handlers_configurator {
				procedures_builder = configurator(procedures_builder);
			}

			procedures_builder.configure()
		};

		// Create RemoteRegistry for forwarding queries to remote namespaces
		#[cfg(not(reifydb_single_threaded))]
		let remote_registry = RemoteRegistry::new(runtime.clone());

		// Create engine and CDC producer BEFORE bootstrap so that bootstrap
		// commits produce CDC entries (PostCommitEvent is captured).
		let engine = StandardEngine::new(
			multi.clone(),
			single.clone(),
			eventbus.clone(),
			self.interceptors.build(),
			Catalog::new(catalog.clone()),
			EngineConfig {
				runtime_context: RuntimeContext::new(runtime.clock().clone(), runtime.rng().clone()),
				functions,
				procedures,
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
			Arc::new(AuthenticationRegistry::new(runtime.clock().clone())),
			runtime.rng().clone(),
			runtime.clock().clone(),
			match self.auth_configurator {
				Some(configurator) => configurator(AuthConfigurator::new()).configure(),
				None => AuthServiceConfig::default(),
			},
		);
		self.ioc = self.ioc.register(auth_service.clone());

		// Spawn CDC producer and register PostCommitEvent listener BEFORE
		// bootstrap so that bootstrap commits generate CDC entries.
		let cdc_handle = spawn_cdc_producer(
			&actor_system,
			cdc_store,
			multi_store,
			engine.clone(),
			eventbus.clone(),
			runtime.clock().clone(),
		);
		eventbus.register::<PostCommitEvent, _>(CdcProducerEventListener::new(
			cdc_handle.actor_ref().clone(),
			runtime.clock().clone(),
		));
		self.ioc.register_service::<Arc<CdcProduceHandle>>(Arc::new(cdc_handle));

		// Bootstrap AFTER CDC producer is active so commits are captured.
		if !self.is_replica {
			bootstrap_system_objects(&multi, &single, &catalog, &eventbus)?;
		}

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
		let health_monitor = Arc::new(HealthMonitor::new(runtime.clock().clone()));
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
			runtime,
			actor_system,
			self.migrations,
		))
	}
}
