// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{path::PathBuf, sync::Arc};

use reifydb_auth::service::AuthConfigurator;
use reifydb_catalog::{bootstrap::read_configs, cache::CatalogCache};
use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_extension::transform::registry::TransformsConfigurator;
use reifydb_routine::routine::registry::RoutinesConfigurator;
use reifydb_runtime::{Runtime, RuntimeConfig, pool::PoolConfig};
use reifydb_store_multi::tier::commit::buffer::MultiCommitBufferTier;
use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::builder::FlowConfigurator;
#[cfg(feature = "sub_metric_profiler")]
use reifydb_sub_metrics::profiler::{builder::ProfilerConfigurator, factory::ProfilerSubsystemFactory};
#[cfg(feature = "sub_replication")]
use reifydb_sub_replication::builder::{ReplicationConfig, ReplicationConfigurator};
#[cfg(all(feature = "sub_replication", not(reifydb_single_threaded)))]
use reifydb_sub_replication::factory::ReplicationSubsystemFactory;
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingConfigurator;
use reifydb_transaction::interceptor::builder::InterceptorBuilder;
use reifydb_value::value::Value;

fn pool_config_from_sources(
	factory: &StorageFactory,
	overrides: &[(ConfigKey, Value)],
) -> Result<(MultiCommitBufferTier, PoolConfig)> {
	let multi_commit_buffer = factory.open_multi_commit_buffer();
	let persisted = read_configs(
		Some(&multi_commit_buffer),
		None,
		&[
			ConfigKey::ThreadsAsync,
			ConfigKey::ThreadsCoordination,
			ConfigKey::ThreadsFlow,
			ConfigKey::ThreadsTask,
			ConfigKey::ThreadsCompute,
		],
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
		coordination_threads: resolve(ConfigKey::ThreadsCoordination),
		flow_threads: resolve(ConfigKey::ThreadsFlow),
		task_threads: resolve(ConfigKey::ThreadsTask),
		compute_threads: resolve(ConfigKey::ThreadsCompute),
		async_threads: resolve(ConfigKey::ThreadsAsync),
	};
	Ok((multi_commit_buffer, pools))
}

use super::{DatabaseBuilder, WithInterceptorBuilder, database::CdcBackend, traits::WithSubsystem};
use crate::{
	Database, MigrationSource, Result,
	api::{StorageFactory, transaction},
};

pub struct EmbeddedBuilder {
	storage_factory: StorageFactory,
	runtime_config: Option<RuntimeConfig>,
	interceptors: InterceptorBuilder,
	subsystem_factories: Vec<Box<dyn SubsystemFactory>>,
	dependencies: Vec<Box<dyn FnOnce(DatabaseBuilder) -> DatabaseBuilder + Send>>,
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
	fast_shutdown: bool,
}

impl EmbeddedBuilder {
	pub fn new(storage_factory: StorageFactory) -> Self {
		Self {
			storage_factory,
			runtime_config: None,
			interceptors: InterceptorBuilder::new(),
			subsystem_factories: Vec::new(),
			dependencies: Vec::new(),
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
			fast_shutdown: false,
		}
	}

	pub fn with_fast_shutdown(mut self) -> Self {
		self.fast_shutdown = true;
		self
	}

	pub fn with_dependency<T: Clone + Send + Sync + 'static>(mut self, value: T) -> Self {
		self.dependencies.push(Box::new(move |builder| builder.with_dependency(value)));
		self
	}

	/// Configure the process runtime (clock + rng).
	///
	/// If not set, a default configuration will be used.
	pub fn with_runtime_config(mut self, config: RuntimeConfig) -> Self {
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
		let (multi_commit_buffer, pool_config) =
			pool_config_from_sources(&self.storage_factory, &self.bootstrap_configs)?;
		let runtime = Runtime::from_config(self.runtime_config.unwrap_or_default(), pool_config);

		let spawner = runtime.spawner();
		let clock = runtime.clock().clone();
		let rng = runtime.rng().clone();

		let (multi_store, single_store, transaction_single, eventbus) =
			self.storage_factory.create_with_multi_commit_buffer(multi_commit_buffer, &spawner);
		let catalog_cache = CatalogCache::new();
		let (multi, single, eventbus) = transaction(
			(multi_store.clone(), single_store.clone(), transaction_single, eventbus),
			spawner,
			clock,
			rng,
			Arc::new(catalog_cache.clone()),
		);

		let cdc_backend = match &self.storage_factory {
			StorageFactory::Memory => CdcBackend::Memory,
			#[cfg(not(target_arch = "wasm32"))]
			StorageFactory::Sqlite(config) => CdcBackend::Sqlite(config.clone()),
			#[cfg(not(target_arch = "wasm32"))]
			StorageFactory::SqliteWithoutBuffer(config) => CdcBackend::Sqlite(config.clone()),
		};

		let mut builder = DatabaseBuilder::new(catalog_cache, multi, single, eventbus)
			.with_interceptor_builder(self.interceptors)
			.with_runtime(runtime)
			.with_stores(multi_store, single_store)
			.with_cdc_backend(cdc_backend);

		for dependency in self.dependencies {
			builder = dependency(builder);
		}

		if self.fast_shutdown {
			builder = builder.with_fast_shutdown();
		}

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

	#[cfg(feature = "sub_metric_profiler")]
	fn with_profiler<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(ProfilerConfigurator) -> ProfilerConfigurator + Send + 'static,
	{
		self.subsystem_factories.push(Box::new(ProfilerSubsystemFactory::with_configurator(configurator)));
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
