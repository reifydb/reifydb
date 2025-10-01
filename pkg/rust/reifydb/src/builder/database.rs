// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{sync::Arc, time::Duration};

use reifydb_auth::AuthVersion;
use reifydb_catalog::{CatalogVersion, MaterializedCatalog, MaterializedCatalogLoader, system::SystemCatalog};
use reifydb_cdc::CdcVersion;
use reifydb_core::{
	CoreVersion,
	event::EventBus,
	interceptor::StandardInterceptorBuilder,
	interface::{
		CdcTransaction, MultiVersionTransaction, SingleVersionTransaction,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	ioc::IocContainer,
	log_timed_debug,
};
use reifydb_engine::{
	EngineTransaction, EngineVersion, StandardCommandTransaction, StandardEngine, StandardQueryTransaction,
};
use reifydb_network::NetworkVersion;
use reifydb_rql::RqlVersion;
use reifydb_store_row::RowStoreVersion;
use reifydb_sub_api::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::{FlowBuilder, FlowSubsystemFactory};
#[cfg(feature = "sub_logging")]
use reifydb_sub_logging::{LoggingBuilder, LoggingSubsystemFactory};
#[cfg(feature = "sub_worker")]
use reifydb_sub_worker::{WorkerBuilder, WorkerSubsystem, WorkerSubsystemFactory};
use reifydb_transaction::TransactionVersion;

use crate::{
	database::{Database, DatabaseConfig},
	health::HealthMonitor,
	subsystem::Subsystems,
};

pub struct DatabaseBuilder<MVT: MultiVersionTransaction, SVT: SingleVersionTransaction, C: CdcTransaction> {
	config: DatabaseConfig,
	interceptors: StandardInterceptorBuilder<StandardCommandTransaction<EngineTransaction<MVT, SVT, C>>>,
	subsystems: Vec<Box<dyn SubsystemFactory<StandardCommandTransaction<EngineTransaction<MVT, SVT, C>>>>>,
	ioc: IocContainer,
	#[cfg(feature = "sub_logging")]
	logging_factory: Option<Box<dyn SubsystemFactory<StandardCommandTransaction<EngineTransaction<MVT, SVT, C>>>>>,
	#[cfg(feature = "sub_worker")]
	worker_factory: Option<Box<dyn SubsystemFactory<StandardCommandTransaction<EngineTransaction<MVT, SVT, C>>>>>,
	#[cfg(feature = "sub_flow")]
	flow_factory: Option<Box<dyn SubsystemFactory<StandardCommandTransaction<EngineTransaction<MVT, SVT, C>>>>>,
}

impl<MVT: MultiVersionTransaction, SVT: SingleVersionTransaction, C: CdcTransaction> DatabaseBuilder<MVT, SVT, C> {
	#[allow(unused_mut)]
	pub fn new(multi: MVT, single: SVT, cdc: C, eventbus: EventBus) -> Self {
		let ioc = IocContainer::new()
			.register(MaterializedCatalog::new())
			.register(eventbus)
			.register(multi)
			.register(single)
			.register(cdc);

		Self {
			config: DatabaseConfig::default(),
			interceptors: StandardInterceptorBuilder::new(),
			subsystems: Vec::new(),
			ioc,
			#[cfg(feature = "sub_logging")]
			logging_factory: None,
			#[cfg(feature = "sub_worker")]
			worker_factory: None,
			#[cfg(feature = "sub_flow")]
			flow_factory: None,
		}
	}

	pub fn with_graceful_shutdown_timeout(mut self, timeout: Duration) -> Self {
		self.config = self.config.with_graceful_shutdown_timeout(timeout);
		self
	}

	pub fn with_health_check_interval(mut self, interval: Duration) -> Self {
		self.config = self.config.with_health_check_interval(interval);
		self
	}

	pub fn with_max_startup_time(mut self, timeout: Duration) -> Self {
		self.config = self.config.with_max_startup_time(timeout);
		self
	}

	pub fn with_config(mut self, config: DatabaseConfig) -> Self {
		self.config = config;
		self
	}

	#[cfg(feature = "sub_logging")]
	pub fn with_logging<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(LoggingBuilder) -> LoggingBuilder + Send + 'static,
	{
		self.logging_factory = Some(Box::new(LoggingSubsystemFactory::with_configurator(configurator)));
		self
	}

	#[cfg(feature = "sub_worker")]
	pub fn with_worker<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(WorkerBuilder) -> WorkerBuilder + Send + 'static,
	{
		self.worker_factory = Some(Box::new(WorkerSubsystemFactory::with_configurator(configurator)));
		self
	}

	#[cfg(feature = "sub_flow")]
	pub fn with_flow<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(
				FlowBuilder<EngineTransaction<MVT, SVT, C>>,
			) -> FlowBuilder<EngineTransaction<MVT, SVT, C>>
			+ Send
			+ 'static,
	{
		self.flow_factory = Some(Box::new(FlowSubsystemFactory::with_configurator(configurator)));
		self
	}

	pub fn add_subsystem_factory(
		mut self,
		factory: Box<dyn SubsystemFactory<StandardCommandTransaction<EngineTransaction<MVT, SVT, C>>>>,
	) -> Self {
		self.subsystems.push(factory);
		self
	}

	pub fn with_interceptor_builder(
		mut self,
		builder: StandardInterceptorBuilder<StandardCommandTransaction<EngineTransaction<MVT, SVT, C>>>,
	) -> Self {
		self.interceptors = builder;
		self
	}

	pub fn config(&self) -> &DatabaseConfig {
		&self.config
	}

	pub fn subsystem_count(&self) -> usize {
		self.subsystems.len()
	}

	pub fn build(mut self) -> crate::Result<Database<MVT, SVT, C>> {
		// Add configured or default subsystems
		#[cfg(feature = "sub_logging")]
		self.subsystems.push(self.logging_factory.unwrap_or_else(|| Box::new(LoggingSubsystemFactory::new())));

		#[cfg(feature = "sub_worker")]
		self.subsystems.push(self
			.worker_factory
			.unwrap_or_else(|| Box::new(WorkerSubsystemFactory::<EngineTransaction<MVT, SVT, C>>::new())));

		#[cfg(feature = "sub_flow")]
		self.subsystems.push(self
			.flow_factory
			.unwrap_or_else(|| Box::new(FlowSubsystemFactory::<EngineTransaction<MVT, SVT, C>>::new())));

		// Collect interceptors from all factories
		for factory in &self.subsystems {
			self.interceptors = factory.provide_interceptors(self.interceptors, &self.ioc);
		}

		let catalog = self.ioc.resolve::<MaterializedCatalog>()?;
		let multi = self.ioc.resolve::<MVT>()?;
		let single = self.ioc.resolve::<SVT>()?;
		let cdc = self.ioc.resolve::<C>()?;
		let eventbus = self.ioc.resolve::<EventBus>()?;

		Self::load_materialized_catalog(&multi, &single, &cdc, &catalog)?;

		// First create the engine (needed by subsystems)
		let engine = StandardEngine::new(
			multi.clone(),
			single.clone(),
			cdc.clone(),
			eventbus.clone(),
			Box::new(self.interceptors.build()),
			catalog.clone(),
		);

		self.ioc = self.ioc.register(engine.clone());

		// Collect all versions
		let mut all_versions = Vec::new();

		// Add core component versions using the version structs from
		// each crate
		all_versions.push(SystemVersion {
			name: "reifydb".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "ReifyDB Database System".to_string(),
			r#type: ComponentType::Package,
		});

		all_versions.push(CoreVersion.version());
		all_versions.push(EngineVersion.version());
		all_versions.push(CatalogVersion.version());
		all_versions.push(RowStoreVersion.version());
		all_versions.push(TransactionVersion.version());
		all_versions.push(AuthVersion.version());
		all_versions.push(RqlVersion.version());
		all_versions.push(CdcVersion.version());
		all_versions.push(NetworkVersion.version());

		// Create subsystems from factories and collect their versions
		let health_monitor = Arc::new(HealthMonitor::new());
		let mut subsystems = Subsystems::new(Arc::clone(&health_monitor));

		for factory in self.subsystems {
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		// Get the scheduler - it must exist when feature is enabled
		#[cfg(feature = "sub_worker")]
		let scheduler = subsystems
			.get::<WorkerSubsystem<EngineTransaction<MVT, SVT, C>>>()
			.map(|w| w.get_scheduler())
			.expect("Worker subsystem should always be created when feature is enabled");

		// Add git hash if available
		if let Some(git_hash) = option_env!("GIT_HASH") {
			all_versions.push(SystemVersion {
				name: "git-hash".to_string(),
				version: git_hash.to_string(),
				description: "Git commit hash at build time".to_string(),
				r#type: ComponentType::Build,
			});
		}

		// Create SystemCatalog with all versions and set it in
		// MaterializedCatalog This is done after engine creation but
		// versions will be available via the catalog
		let system_catalog = SystemCatalog::new(all_versions);
		catalog.set_system_catalog(system_catalog);

		Ok(Database::new(
			engine,
			subsystems,
			self.config,
			health_monitor,
			#[cfg(feature = "sub_worker")]
			scheduler,
		))
	}

	/// Load the materialized catalog from storage
	fn load_materialized_catalog(
		multi: &MVT,
		single: &SVT,
		cdc: &C,
		catalog: &MaterializedCatalog,
	) -> crate::Result<()> {
		let mut qt: StandardQueryTransaction<EngineTransaction<MVT, SVT, C>> = StandardQueryTransaction::new(
			multi.begin_query()?,
			single.clone(),
			cdc.clone(),
			catalog.clone(),
		);

		log_timed_debug!("Loading materialized catalog", {
			MaterializedCatalogLoader::load_all(&mut qt, catalog)?;
		});

		Ok(())
	}
}

impl<MVT: MultiVersionTransaction, SVT: SingleVersionTransaction, C: CdcTransaction> DatabaseBuilder<MVT, SVT, C> {
	pub fn development_config(self) -> Self {
		self.with_graceful_shutdown_timeout(Duration::from_secs(10))
			.with_health_check_interval(Duration::from_secs(2))
			.with_max_startup_time(Duration::from_secs(30))
	}

	pub fn production_config(self) -> Self {
		self.with_graceful_shutdown_timeout(Duration::from_secs(60))
			.with_health_check_interval(Duration::from_secs(10))
			.with_max_startup_time(Duration::from_secs(120))
	}

	pub fn testing_config(self) -> Self {
		self.with_graceful_shutdown_timeout(Duration::from_secs(5))
			.with_health_check_interval(Duration::from_secs(1))
			.with_max_startup_time(Duration::from_secs(10))
	}
}
