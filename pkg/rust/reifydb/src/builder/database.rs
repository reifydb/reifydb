// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crate::{database::Database, health::HealthMonitor, subsystem::Subsystems};
use reifydb_auth::AuthVersion;
use reifydb_catalog::{Catalog, CatalogVersion, MaterializedCatalog, MaterializedCatalogLoader, system::SystemCatalog};
use reifydb_cdc::{CdcEventListener, CdcStore, CdcVersion, CdcWorker};
use reifydb_core::event::transaction::PostCommitEvent;
use reifydb_core::{
	CoreVersion, SharedRuntime,
	event::EventBus,
	interface::version::{ComponentType, HasVersion, SystemVersion},
	ioc::IocContainer,
};
use reifydb_engine::{EngineVersion, StandardEngine, StandardQueryTransaction};
use reifydb_function::{Functions, FunctionsBuilder, math, series};
use reifydb_rql::RqlVersion;
use reifydb_rqlv2;
use reifydb_rqlv2::Compiler;
use reifydb_store_transaction::{StorageResolver, TransactionStoreVersion};
use reifydb_sub_api::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::{FlowBuilder, subsystem::FlowSubsystemFactory};
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::{TracingBuilder, TracingSubsystemFactory};
use reifydb_transaction::{
	TransactionVersion, interceptor::StandardInterceptorBuilder,
	multi::TransactionMultiVersion, single::TransactionSingle,
};
use tracing::debug;

pub struct DatabaseBuilder {
	interceptors: StandardInterceptorBuilder,
	factories: Vec<Box<dyn SubsystemFactory>>,
	ioc: IocContainer,
	functions_configurator: Option<Box<dyn FnOnce(FunctionsBuilder) -> FunctionsBuilder + Send + 'static>>,
	#[cfg(feature = "sub_tracing")]
	tracing_factory: Option<Box<dyn SubsystemFactory>>,
	#[cfg(feature = "sub_flow")]
	flow_factory: Option<Box<dyn SubsystemFactory>>,
}

impl DatabaseBuilder {
	#[allow(unused_mut)]
	pub fn new(
		multi: TransactionMultiVersion,
		single: TransactionSingle,
		eventbus: EventBus,
	) -> Self {
		let ioc = IocContainer::new()
			.register(MaterializedCatalog::new())
			.register(eventbus)
			.register(multi)
			.register(single);

		Self {
			interceptors: StandardInterceptorBuilder::new(),
			factories: Vec::new(),
			ioc,
			functions_configurator: None,
			#[cfg(feature = "sub_tracing")]
			tracing_factory: None,
			#[cfg(feature = "sub_flow")]
			flow_factory: None,
		}
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

		for factory in &self.factories {
			self.interceptors = factory.provide_interceptors(self.interceptors, &self.ioc);
		}

		let catalog = self.ioc.resolve::<MaterializedCatalog>()?;
		let multi = self.ioc.resolve::<TransactionMultiVersion>()?;
		let single = self.ioc.resolve::<TransactionSingle>()?;
		let eventbus = self.ioc.resolve::<EventBus>()?;

		Self::load_materialized_catalog(&multi, &single, &catalog)?;

		// Create and register Compiler (requires SharedRuntime to be registered first)
		let runtime = self.ioc.resolve::<SharedRuntime>()?;
		let compiler = Compiler::new(catalog.clone());
		self.ioc = self.ioc.register(compiler);

		// Create and register CdcStore for CDC storage
		let cdc_store = CdcStore::memory();
		self.ioc = self.ioc.register(cdc_store.clone());

		// Create CDC worker with stats tracking and register event listener
		// The worker is stored in IoC to keep it alive for the database lifetime
		let stats_worker = multi.store().stats_worker().clone();
		let hot_storage = multi.store().hot().cloned().expect("hot tier required for CDC");
		let resolver = Arc::new(StorageResolver::new(hot_storage));
		let cdc_worker = Arc::new(CdcWorker::spawn(cdc_store, resolver, Some(stats_worker)));
		eventbus.register::<PostCommitEvent, _>(CdcEventListener::new(cdc_worker.sender()));
		self.ioc.register_service::<Arc<CdcWorker>>(cdc_worker);

		let functions = if let Some(configurator) = self.functions_configurator {
			let default_builder = Functions::builder()
				.register_aggregate("math::sum", math::aggregate::Sum::new)
				.register_aggregate("math::min", math::aggregate::Min::new)
				.register_aggregate("math::max", math::aggregate::Max::new)
				.register_aggregate("math::avg", math::aggregate::Avg::new)
				.register_aggregate("math::count", math::aggregate::Count::new)
				.register_scalar("math::abs", math::scalar::Abs::new)
				.register_scalar("math::avg", math::scalar::Avg::new)
				.register_generator("generate_series", series::GenerateSeries::new);

			Some(configurator(default_builder).build())
		} else {
			None
		};

		let engine = StandardEngine::new(
			multi.clone(),
			single.clone(),
			eventbus.clone(),
			Box::new(self.interceptors.build()),
			Catalog::new(catalog),
			functions,
			self.ioc.clone(),
		);

		self.ioc = self.ioc.register(engine.clone());

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
		all_versions.push(TransactionStoreVersion.version());
		all_versions.push(TransactionVersion.version());
		all_versions.push(AuthVersion.version());
		all_versions.push(RqlVersion.version());
		all_versions.push(CdcVersion.version());

		// Create subsystems from factories and collect their versions
		// IMPORTANT: Order matters for shutdown! Subsystems are stopped in REVERSE order.
		// Add logging FIRST so it's stopped LAST and can log shutdown messages from other subsystems.
		let health_monitor = Arc::new(HealthMonitor::new());
		let mut subsystems = Subsystems::new(Arc::clone(&health_monitor));

		// 1. Add tracing subsystem first (stopped last during shutdown)
		#[cfg(feature = "sub_tracing")]
		if let Some(factory) = self.tracing_factory {
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		// 3. Add flow subsystem third
		#[cfg(feature = "sub_flow")]
		if let Some(factory) = self.flow_factory {
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		// 4. Add other custom subsystems last (stopped first during shutdown)
		for factory in self.factories {
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		// Add git hash if available
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
		multi: &TransactionMultiVersion,
		single: &TransactionSingle,
		catalog: &MaterializedCatalog,
	) -> crate::Result<()> {
		let mut qt = StandardQueryTransaction::new(multi.begin_query()?, single.clone());

		debug!("Loading materialized catalog");
		MaterializedCatalogLoader::load_all(&mut qt, catalog)?;

		Ok(())
	}
}
