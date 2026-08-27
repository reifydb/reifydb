// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{path::PathBuf, sync::Arc};

use reifydb_auth::service::AuthConfigurator;
use reifydb_catalog::cache::CatalogCache;
use reifydb_core::interface::{auth::AuthenticationProvider, catalog::config::ConfigKey};
use reifydb_extension::transform::registry::TransformsConfigurator;
use reifydb_routine_abi::registry::RoutinesConfigurator;
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
#[cfg(feature = "sub_metric_profiler")]
use reifydb_sub_metrics::profiler::{builder::ProfilerConfigurator, factory::ProfilerSubsystemFactory};
#[cfg(all(feature = "sub_replication", not(reifydb_single_threaded)))]
use reifydb_sub_replication::builder::{ReplicationConfig, ReplicationConfigurator};
#[cfg(all(feature = "sub_replication", not(reifydb_single_threaded)))]
use reifydb_sub_replication::factory::ReplicationSubsystemFactory;
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingConfigurator;
use reifydb_transaction::interceptor::builder::InterceptorBuilder;
use reifydb_value::{byte_size::ByteSize, value::{Value, duration::Duration}};

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
	u32,
	Duration,
	ByteSize,
	ByteSize,
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
		resolved.operator_wal_autocheckpoint,
		resolved.operator_flush_interval,
		resolved.operator_flush_budget,
		resolved.multi_flush_budget,
	))
}

use super::{DatabaseBuilder, WithInterceptorBuilder, startup::resolve_startup_configs, traits::WithSubsystem};
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
	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
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
	auth_providers: Vec<Box<dyn AuthenticationProvider>>,
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
			#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
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
			auth_providers: Vec::new(),
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

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
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

	/// Migrations are recorded in the database on first encounter and applied in registration
	/// order, not name order; already-applied ones are skipped.
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

	pub fn build(self) -> Result<Database> {
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
			operator_wal_autocheckpoint,
			operator_flush_interval,
			operator_flush_budget,
			multi_flush_budget,
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
				operator_wal_autocheckpoint,
				operator_flush_interval,
				operator_flush_budget,
				multi_flush_budget,
				&spawner,
			);
		let catalog_cache = CatalogCache::new();
		let version_epoch = VersionEpoch::new();
		let (multi, single, eventbus) = transaction(
			(multi_store.clone(), single_store.clone(), transaction_single, eventbus),
			spawner,
			clock,
			version_epoch.clone(),
			rng,
			Arc::new(catalog_cache.clone()),
		);

		let mut builder = DatabaseBuilder::new(catalog_cache, multi, single, eventbus, version_epoch)
			.with_interceptor_builder(self.interceptors)
			.with_runtime(runtime)
			.with_stores(multi_store, single_store, operator_store, cdc_store);

		for dependency in self.dependencies {
			builder = dependency(builder);
		}

		if self.fast_shutdown {
			builder = builder.with_fast_shutdown();
		}

		if let Some(configurator) = self.auth_configurator {
			builder = builder.with_auth(configurator);
		}

		for provider in self.auth_providers {
			builder = builder.with_boxed_auth_provider(provider);
		}

		if let Some(configurator) = self.routines_configurator {
			builder = builder.with_routines_configurator(configurator);
		}

		if let Some(configurator) = self.handlers_configurator {
			builder = builder.with_handlers_configurator(configurator);
		}

		#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
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
