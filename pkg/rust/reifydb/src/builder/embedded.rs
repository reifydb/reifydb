// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{path::PathBuf, sync::Arc};

use reifydb_auth::service::AuthConfigurator;
use reifydb_catalog::materialized::MaterializedCatalog;
use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_extension::transform::registry::TransformsConfigurator;
use reifydb_routine::routine::registry::RoutinesConfigurator;
use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig};
use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::builder::FlowConfigurator;
#[cfg(feature = "sub_replication")]
use reifydb_sub_replication::builder::{ReplicationConfig, ReplicationConfigurator};
#[cfg(all(feature = "sub_replication", not(reifydb_single_threaded)))]
use reifydb_sub_replication::factory::ReplicationSubsystemFactory;
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingConfigurator;
use reifydb_transaction::interceptor::builder::InterceptorBuilder;
use reifydb_type::value::Value;

use super::{DatabaseBuilder, WithInterceptorBuilder, database::CdcBackend, traits::WithSubsystem};
use crate::{
	Database, MigrationSource, Result,
	api::{StorageFactory, transaction},
};

pub struct EmbeddedBuilder {
	storage_factory: StorageFactory,
	runtime: Option<SharedRuntime>,
	runtime_config: Option<SharedRuntimeConfig>,
	interceptors: InterceptorBuilder,
	subsystem_factories: Vec<Box<dyn SubsystemFactory>>,
	routines_configurator: Option<Box<dyn FnOnce(RoutinesConfigurator) -> RoutinesConfigurator + Send + 'static>>,
	handlers_configurator: Option<Box<dyn FnOnce(RoutinesConfigurator) -> RoutinesConfigurator + Send + 'static>>,
	#[cfg(reifydb_target = "native")]
	procedure_dir: Option<PathBuf>,
	wasm_procedure_dir: Option<PathBuf>,
	transforms_configurator:
		Option<Box<dyn FnOnce(TransformsConfigurator) -> TransformsConfigurator + Send + 'static>>,
	#[cfg(feature = "sub_tracing")]
	tracing_configurator: Option<Box<dyn FnOnce(TracingConfigurator) -> TracingConfigurator + Send + 'static>>,
	#[cfg(feature = "sub_flow")]
	flow_configurator: Option<Box<dyn FnOnce(FlowConfigurator) -> FlowConfigurator + Send + 'static>>,
	#[cfg(feature = "sub_replication")]
	replication_factory: Option<Box<dyn SubsystemFactory>>,
	auth_configurator: Option<Box<dyn FnOnce(AuthConfigurator) -> AuthConfigurator + Send + 'static>>,
	migrations: Option<MigrationSource>,
	bootstrap_configs: Vec<(ConfigKey, Value)>,
}

impl EmbeddedBuilder {
	pub fn new(storage_factory: StorageFactory) -> Self {
		Self {
			storage_factory,
			runtime: None,
			runtime_config: None,
			interceptors: InterceptorBuilder::new(),
			subsystem_factories: Vec::new(),
			routines_configurator: None,
			handlers_configurator: None,
			#[cfg(reifydb_target = "native")]
			procedure_dir: None,
			wasm_procedure_dir: None,
			transforms_configurator: None,
			#[cfg(feature = "sub_tracing")]
			tracing_configurator: None,
			#[cfg(feature = "sub_flow")]
			flow_configurator: None,
			#[cfg(feature = "sub_replication")]
			replication_factory: None,
			auth_configurator: None,
			migrations: None,
			bootstrap_configs: Vec::new(),
		}
	}

	/// Provide a pre-built shared runtime.
	///
	/// When set, this runtime is used directly and `with_runtime_config` is ignored.
	pub fn with_runtime(mut self, runtime: SharedRuntime) -> Self {
		self.runtime = Some(runtime);
		self
	}

	/// Configure the shared runtime.
	///
	/// If not set, a default configuration will be used.
	/// Ignored if `with_runtime` was called.
	pub fn with_runtime_config(mut self, config: SharedRuntimeConfig) -> Self {
		self.runtime_config = Some(config);
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

	/// Register migrations to be applied during `Database::start()`.
	///
	/// Migrations are stored in the database on first encounter and
	/// applied in name order. Already-applied migrations are skipped.
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

	pub fn build(self) -> Result<Database> {
		let runtime = match self.runtime {
			Some(rt) => rt,
			None => SharedRuntime::from_config(self.runtime_config.unwrap_or_default()),
		};

		let actor_system = runtime.actor_system().scope();
		let (multi_store, single_store, transaction_single, eventbus) =
			self.storage_factory.create(&actor_system);
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

		let mut builder = DatabaseBuilder::new(materialized_catalog, multi, single, eventbus)
			.with_interceptor_builder(self.interceptors)
			.with_runtime(runtime.clone())
			.with_actor_system(actor_system)
			.with_stores(multi_store, single_store)
			.with_cdc_backend(cdc_backend);

		if let Some(configurator) = self.auth_configurator {
			builder = builder.with_auth(configurator);
		}

		if let Some(configurator) = self.routines_configurator {
			builder = builder.with_routines_configurator(configurator);
		}

		if let Some(configurator) = self.handlers_configurator {
			builder = builder.with_handlers_configurator(configurator);
		}

		#[cfg(reifydb_target = "native")]
		if let Some(dir) = self.procedure_dir {
			builder = builder.with_procedure_dir(dir);
		}

		if let Some(dir) = self.wasm_procedure_dir {
			builder = builder.with_wasm_procedure_dir(dir);
		}

		if let Some(configurator) = self.transforms_configurator {
			builder = builder.with_transforms(configurator);
		}

		#[cfg(feature = "sub_tracing")]
		{
			let configurator = self.tracing_configurator.unwrap_or_else(|| Box::new(|t| t));
			builder = builder.with_tracing(configurator);
		}

		#[cfg(feature = "sub_flow")]
		if let Some(configurator) = self.flow_configurator {
			builder = builder.with_flow(configurator);
		}

		#[cfg(feature = "sub_replication")]
		if let Some(factory) = self.replication_factory {
			builder = builder.add_replication_factory(factory);
		}

		for factory in self.subsystem_factories {
			builder = builder.add_subsystem_factory(factory);
		}

		if let Some(source) = self.migrations {
			let migrations = source.resolve()?;
			if !migrations.is_empty() {
				builder = builder.with_migrations(migrations);
			}
		}

		if !self.bootstrap_configs.is_empty() {
			builder = builder.with_configs(self.bootstrap_configs);
		}

		builder.build()
	}
}

impl WithSubsystem for EmbeddedBuilder {
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
		self.replication_factory = Some(Box::new(ReplicationSubsystemFactory::new(configurator)));
		self
	}

	fn with_subsystem(mut self, factory: Box<dyn SubsystemFactory>) -> Self {
		self.subsystem_factories.push(factory);
		self
	}
}

impl WithInterceptorBuilder for EmbeddedBuilder {
	fn interceptor_builder_mut(&mut self) -> &mut InterceptorBuilder {
		&mut self.interceptors
	}
}
