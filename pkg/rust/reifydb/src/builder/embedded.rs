// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{path::PathBuf, sync::Arc};

use reifydb_auth::service::AuthConfigurator;
use reifydb_catalog::cache::CatalogCache;
use reifydb_core::{
	common::CommitVersion,
	interface::{auth::AuthenticationProvider, catalog::config::ConfigKey},
};
use reifydb_extension::transform::registry::TransformsConfigurator;
use reifydb_routine_abi::registry::RoutinesConfigurator;
use reifydb_runtime::{
	Runtime, RuntimeConfig,
	context::{RuntimeContext, clock::Clock, rng::Rng, transaction::TransactionContext},
	fatal::install as install_fatal,
	io::fs::Fs,
	pool::PoolConfig,
	version_epoch::VersionEpoch,
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_store_commit::store::CommitStore;
use reifydb_store_multi::tier::{persistent::MultiPersistentTier, point::MultiPointConfig, range::MultiRangeConfig};
use reifydb_store_operator::{persistent::PersistentTier, range::OperatorRangeConfig, resident::ResidentLimits};
use reifydb_sub_api::subsystem::SubsystemFactory;
#[cfg(feature = "sub_flow")]
use reifydb_sub_flow::builder::FlowConfigurator;
#[cfg(feature = "sub_metric_profiler")]
use reifydb_sub_metrics::profiler::{builder::ProfilerConfigurator, factory::ProfilerSubsystemFactory};
#[cfg(feature = "sub_tracing")]
use reifydb_sub_tracing::builder::TracingConfigurator;
use reifydb_transaction::{
	interceptor::builder::InterceptorBuilder,
	wal::{CommitWal, Recovery, WalConfigurator, replay},
};
use reifydb_value::value::{Value, duration::Duration};
use reifydb_wal::sync::WalMode;

type PoolConfigSources = (
	CommitStore,
	Option<MultiPersistentTier>,
	PoolConfig,
	Option<MultiPointConfig>,
	Option<MultiRangeConfig>,
	Option<OperatorRangeConfig>,
	u32,
	ResidentLimits,
	Duration,
	CommitWal,
	Recovery,
);

fn pool_config_from_sources(
	ctx: &TransactionContext,
	factory: &StorageFactory,
	overrides: &[(ConfigKey, Value)],
	wal_mode: WalMode,
	wal_fs: Option<(Fs, PathBuf)>,
	clock: &Clock,
) -> Result<PoolConfigSources> {
	let multi_commit_store = factory.open_multi_commit_store();
	let multi_persistent = factory.open_multi_persistent();
	let (saved_through, persisted_high) = match multi_persistent.as_ref() {
		Some(persistent) => (persistent.saved_through()?, persistent.install_floor()?),
		None => (CommitVersion(0), CommitVersion(0)),
	};
	let wal = commit_wal(factory, clock, wal_mode, wal_fs);
	let recovery = replay(&wal, &multi_commit_store, saved_through, persisted_high);
	let resolved = resolve_startup_configs(ctx, &multi_commit_store, multi_persistent.as_ref(), overrides)?;
	if let Some(persistent) = multi_persistent.as_ref() {
		persistent.set_checkpoint_threshold(resolved.multi_wal_autocheckpoint);
	}
	Ok((
		multi_commit_store,
		multi_persistent,
		resolved.pools,
		resolved.multi_point,
		resolved.multi_range,
		resolved.operator_range,
		resolved.operator_wal_autocheckpoint,
		resolved.operator_resident,
		resolved.operator_flush_interval,
		wal,
		recovery,
	))
}

#[cfg(feature = "column")]
use reifydb_extension::block_encoding::registry::BlockEncodingsConfigurator;
#[cfg(feature = "column")]
use reifydb_store_column::store::ColumnStore;
#[cfg(feature = "column")]
use reifydb_sub_store::subsystem::StorageConfig;

use super::{DatabaseBuilder, WithInterceptorBuilder, startup::resolve_startup_configs, traits::WithSubsystem};
#[cfg(all(feature = "column", reifydb_target = "host"))]
use crate::api;
use crate::{
	Database, MigrationSource, Result,
	api::{StorageFactory, commit_wal, transaction},
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
	auth_configurator: Option<Box<dyn FnOnce(AuthConfigurator) -> AuthConfigurator + Send + 'static>>,
	wal_configurator: Option<Box<dyn FnOnce(WalConfigurator) -> WalConfigurator + Send + 'static>>,
	auth_providers: Vec<Box<dyn AuthenticationProvider>>,
	migrations: Option<MigrationSource>,
	bootstrap_configs: Vec<(ConfigKey, Value)>,
	fast_shutdown: bool,
	operator_persistent: Option<PersistentTier>,
	wal_fs: Option<(Fs, PathBuf)>,
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
			auth_configurator: None,
			wal_configurator: None,
			auth_providers: Vec::new(),
			migrations: None,
			bootstrap_configs: Vec::new(),
			fast_shutdown: false,
			operator_persistent: None,
			wal_fs: None,
		}
	}

	pub fn with_fast_shutdown(mut self) -> Self {
		self.fast_shutdown = true;
		self
	}

	#[cfg(feature = "testing")]
	pub fn with_wal_fs(mut self, fs: Fs, dir: impl Into<PathBuf>) -> Self {
		self.wal_fs = Some((fs, dir.into()));
		self
	}

	pub fn with_operator_persistent(mut self, persistent: PersistentTier) -> Self {
		self.operator_persistent = Some(persistent);
		self
	}

	pub fn with_dependency<T: Clone + Send + Sync + 'static>(mut self, value: T) -> Self {
		self.dependencies.push(Box::new(move |builder| builder.with_dependency(value)));
		self
	}

	#[cfg(feature = "column")]
	pub fn with_storage_config(mut self, config: StorageConfig) -> Self {
		self.dependencies.push(Box::new(move |builder| builder.with_storage_config(config)));
		self
	}

	#[cfg(feature = "column")]
	pub fn with_column_store(mut self, store: ColumnStore) -> Self {
		self.dependencies.push(Box::new(move |builder| builder.with_column_store(store)));
		self
	}

	#[cfg(feature = "column")]
	pub fn with_block_encodings<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(BlockEncodingsConfigurator) -> BlockEncodingsConfigurator + Send + 'static,
	{
		self.dependencies.push(Box::new(move |builder| builder.with_block_encodings(configurator)));
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

	pub fn with_wal<F>(mut self, configurator: F) -> Self
	where
		F: FnOnce(WalConfigurator) -> WalConfigurator + Send + 'static,
	{
		self.wal_configurator = Some(Box::new(configurator));
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
		let boot_clock = self.runtime_config.as_ref().map_or(Clock::Real, |config| config.clock.clone());
		let boot_ctx = TransactionContext::system(RuntimeContext::new(
			boot_clock.clone(),
			self.runtime_config.as_ref().map_or(Rng::Os, |config| config.rng.fork(b"startup")),
			VersionEpoch::new(),
		));
		let wal_mode = self
			.wal_configurator
			.map_or_else(WalConfigurator::new, |configurator| configurator(WalConfigurator::new()))
			.configure();
		let (
			multi_commit_store,
			multi_persistent,
			pool_config,
			multi_point_buffer,
			multi_range_buffer,
			operator_range_buffer,
			operator_wal_autocheckpoint,
			operator_resident,
			operator_flush_interval,
			wal,
			recovery,
		) = pool_config_from_sources(
			&boot_ctx,
			&self.storage_factory,
			&self.bootstrap_configs,
			wal_mode,
			self.wal_fs,
			&boot_clock,
		)?;
		#[cfg(all(feature = "column", reifydb_target = "host"))]
		let column_dir = match &self.storage_factory {
			StorageFactory::Sqlite(config) => Some(api::column_dir(config)),
			StorageFactory::Memory => None,
		};
		let runtime_config = self.runtime_config.unwrap_or_default();
		install_fatal(runtime_config.fatal);
		let runtime = Runtime::from_config(runtime_config, pool_config);

		let spawner = runtime.spawner();
		let clock = runtime.clock().clone();
		let rng = runtime.rng().clone();

		let (multi_store, single_store, operator_store, transaction_single, eventbus) =
			self.storage_factory.create_with_multi_commit_store(
				multi_commit_store,
				multi_persistent,
				multi_point_buffer,
				multi_range_buffer,
				operator_range_buffer,
				operator_wal_autocheckpoint,
				operator_resident,
				operator_flush_interval,
				self.operator_persistent,
				&wal,
				wal_mode != WalMode::Off,
				&clock,
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
			wal,
			recovery,
		);

		let mut builder = DatabaseBuilder::new(catalog_cache, multi, single, eventbus, version_epoch)
			.with_interceptor_builder(self.interceptors)
			.with_runtime(runtime)
			.with_stores(multi_store, single_store, operator_store);

		#[cfg(all(feature = "column", reifydb_target = "host"))]
		{
			builder = builder.with_column_dir(column_dir);
		}

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
