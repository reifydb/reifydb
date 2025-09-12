// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{sync::Arc, time::Duration};

use reifydb_auth::AuthVersion;
use reifydb_catalog::{
	CatalogVersion, MaterializedCatalog, MaterializedCatalogLoader,
	system::SystemCatalog,
};
use reifydb_cdc::CdcVersion;
use reifydb_core::{
	CoreVersion,
	event::EventBus,
	interceptor::StandardInterceptorBuilder,
	interface::{
		CdcTransaction, UnversionedTransaction, VersionedTransaction,
		subsystem::SubsystemFactory,
		version::{ComponentType, HasVersion, SystemVersion},
	},
	ioc::IocContainer,
	log_timed_debug,
};
use reifydb_engine::{
	EngineTransaction, EngineVersion, StandardCommandTransaction,
	StandardEngine, StandardQueryTransaction,
};
use reifydb_network::NetworkVersion;
use reifydb_rql::RqlVersion;
use reifydb_storage::StorageVersion;
#[cfg(feature = "sub_logging")]
use reifydb_sub_logging::LoggingSubsystemFactory;
#[cfg(feature = "sub_workerpool")]
use reifydb_sub_workerpool::WorkerPoolSubsystemFactory;
use reifydb_transaction::TransactionVersion;

use crate::{
	database::{Database, DatabaseConfig},
	health::HealthMonitor,
	subsystem::Subsystems,
};

pub struct DatabaseBuilder<
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
> {
	config: DatabaseConfig,
	interceptors: StandardInterceptorBuilder<
		StandardCommandTransaction<EngineTransaction<VT, UT, C>>,
	>,
	subsystems: Vec<
		Box<
			dyn SubsystemFactory<
				StandardCommandTransaction<
					EngineTransaction<VT, UT, C>,
				>,
			>,
		>,
	>,
	ioc: IocContainer,
}

impl<VT: VersionedTransaction, UT: UnversionedTransaction, C: CdcTransaction>
	DatabaseBuilder<VT, UT, C>
{
	#[allow(unused_mut)]
	pub fn new(
		versioned: VT,
		unversioned: UT,
		cdc: C,
		eventbus: EventBus,
	) -> Self {
		let ioc = IocContainer::new()
			.register(MaterializedCatalog::new())
			.register(eventbus.clone())
			.register(versioned.clone())
			.register(unversioned.clone())
			.register(cdc.clone());

		let result = Self {
			config: DatabaseConfig::default(),
			interceptors: StandardInterceptorBuilder::new(),
			subsystems: Vec::new(),
			ioc,
		};

		result
	}

	/// Add default subsystems that are always required
	#[allow(unused_mut)]
	pub fn with_default_subsystems(mut self) -> Self {
		// Add default logging subsystem first so it's initialized
		// before other subsystems Note: This can be overridden by
		// adding a custom logging factory before calling this
		#[cfg(feature = "sub_logging")]
		if self.subsystems.is_empty() {
			self = self.add_subsystem_factory(Box::new(
				LoggingSubsystemFactory::new(),
			));
		}

		// Add worker pool subsystem if feature enabled
		#[cfg(feature = "sub_workerpool")]
		{
			self = self.add_subsystem_factory(Box::new(
				WorkerPoolSubsystemFactory::new(),
			));
		}

		self
	}

	pub fn with_graceful_shutdown_timeout(
		mut self,
		timeout: Duration,
	) -> Self {
		self.config =
			self.config.with_graceful_shutdown_timeout(timeout);
		self
	}

	pub fn with_health_check_interval(
		mut self,
		interval: Duration,
	) -> Self {
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

	pub fn add_subsystem_factory(
		mut self,
		factory: Box<
			dyn SubsystemFactory<
				StandardCommandTransaction<
					EngineTransaction<VT, UT, C>,
				>,
			>,
		>,
	) -> Self {
		self.subsystems.push(factory);
		self
	}

	pub fn with_interceptor_builder(
		mut self,
		builder: StandardInterceptorBuilder<
			StandardCommandTransaction<
				EngineTransaction<VT, UT, C>,
			>,
		>,
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

	pub fn build(mut self) -> crate::Result<Database<VT, UT, C>> {
		// Collect interceptors from all factories
		for factory in &self.subsystems {
			self.interceptors = factory.provide_interceptors(
				self.interceptors,
				&self.ioc,
			);
		}

		let catalog = self.ioc.resolve::<MaterializedCatalog>()?;
		let versioned = self.ioc.resolve::<VT>()?;
		let unversioned = self.ioc.resolve::<UT>()?;
		let cdc = self.ioc.resolve::<C>()?;
		let eventbus = self.ioc.resolve::<EventBus>()?;

		Self::load_materialized_catalog(
			&versioned,
			&unversioned,
			&cdc,
			&catalog,
		)?;

		// First create the engine (needed by subsystems)
		let engine = StandardEngine::new(
			versioned.clone(),
			unversioned.clone(),
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
		all_versions.push(StorageVersion.version());
		all_versions.push(TransactionVersion.version());
		all_versions.push(AuthVersion.version());
		all_versions.push(RqlVersion.version());
		all_versions.push(CdcVersion.version());
		all_versions.push(NetworkVersion.version());

		// Create subsystems from factories and collect their versions
		let health_monitor = Arc::new(HealthMonitor::new());
		let mut subsystems =
			Subsystems::new(Arc::clone(&health_monitor));

		for factory in self.subsystems {
			let subsystem = factory.create(&self.ioc)?;
			all_versions.push(subsystem.version());
			subsystems.add_subsystem(subsystem);
		}

		// Add git hash if available
		if let Some(git_hash) = option_env!("GIT_HASH") {
			all_versions.push(SystemVersion {
				name: "git-hash".to_string(),
				version: git_hash.to_string(),
				description: "Git commit hash at build time"
					.to_string(),
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
		))
	}

	/// Load the materialized catalog from storage
	fn load_materialized_catalog(
		versioned: &VT,
		unversioned: &UT,
		cdc: &C,
		catalog: &MaterializedCatalog,
	) -> crate::Result<()> {
		let mut qt: StandardQueryTransaction<
			EngineTransaction<VT, UT, C>,
		> = StandardQueryTransaction::new(
			versioned.begin_query()?,
			unversioned.clone(),
			cdc.clone(),
			catalog.clone(),
		);

		log_timed_debug!("Loading materialized catalog", {
			MaterializedCatalogLoader::load_all(&mut qt, catalog)?;
		});

		Ok(())
	}
}

impl<VT: VersionedTransaction, UT: UnversionedTransaction, C: CdcTransaction>
	DatabaseBuilder<VT, UT, C>
{
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
