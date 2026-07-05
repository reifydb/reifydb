// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(feature = "column")]
use std::sync::Arc;

#[cfg(feature = "column")]
use reifydb_column::compress::{CompressConfig, Compressor};
use reifydb_core::util::ioc::IocContainer;
#[cfg(feature = "column")]
use reifydb_engine::engine::StandardEngine;
#[cfg(feature = "column")]
use reifydb_runtime::actor::system::ActorSpawner;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteConfig;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::Result;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::column::persistent::sqlite::SqliteColumnStore;
#[cfg(feature = "column")]
use crate::column::{
	actor::{series::SeriesMaterializationActor, table::TableMaterializationActor},
	block_store::ColumnBlockStore,
};
use crate::subsystem::{StorageConfig, StorageSubsystem};

pub struct StorageSubsystemFactory {
	#[cfg_attr(not(feature = "column"), allow(dead_code))]
	config: StorageConfig,
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	column_sqlite: Option<SqliteConfig>,
}

impl StorageSubsystemFactory {
	pub fn new(config: StorageConfig) -> Self {
		Self {
			config,
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			column_sqlite: None,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn with_column_sqlite(mut self, config: Option<SqliteConfig>) -> Self {
		self.column_sqlite = config;
		self
	}
}

impl Default for StorageSubsystemFactory {
	fn default() -> Self {
		Self::new(StorageConfig::default())
	}
}

impl SubsystemFactory for StorageSubsystemFactory {
	#[cfg(feature = "column")]
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let spawner = ioc.resolve::<ActorSpawner>()?;
		let engine = ioc.resolve::<StandardEngine>()?;

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let block_store = {
			let tier = self.column_sqlite.clone().map(|cfg| Arc::new(SqliteColumnStore::new(cfg)));
			let store = ColumnBlockStore::with_persistent(tier);
			store.warm()?;
			store
		};
		#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
		let block_store = ColumnBlockStore::new();

		ioc.register_service::<Arc<ColumnBlockStore>>(Arc::new(block_store.clone()));

		let table_actor = TableMaterializationActor::new(
			engine.clone(),
			block_store.clone(),
			Compressor::new(CompressConfig::default()),
			self.config.table_tick_interval,
		);
		let table_handle = spawner.spawn_system("storage-materialize-table", table_actor);
		let table_ref = table_handle.actor_ref().clone();

		let series_actor = SeriesMaterializationActor::new(
			engine,
			block_store.clone(),
			Compressor::new(CompressConfig::default()),
			self.config.series_tick_interval,
			self.config.series_bucket_width,
			self.config.series_grace,
		);
		let series_handle = spawner.spawn_system("storage-materialize-series", series_actor);
		let series_ref = series_handle.actor_ref().clone();

		Ok(Box::new(StorageSubsystem::new(block_store, table_ref, series_ref)))
	}

	#[cfg(not(feature = "column"))]
	fn create(self: Box<Self>, _ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		Ok(Box::new(StorageSubsystem::new()))
	}
}
