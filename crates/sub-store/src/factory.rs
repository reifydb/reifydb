// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_target = "host")]
use std::path::PathBuf;
#[cfg(feature = "column")]
use std::sync::Arc;

#[cfg(feature = "column")]
use reifydb_core::event::{EventBus, transaction::PostCommitEvent};
use reifydb_core::util::ioc::IocContainer;
#[cfg(feature = "column")]
use reifydb_engine::engine::StandardEngine;
#[cfg(feature = "column")]
use reifydb_runtime::actor::system::ActorSpawner;
#[cfg(feature = "column")]
use reifydb_store_column::{compress::Compressor, store::ColumnStore};
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_value::Result;

#[cfg(feature = "column")]
use crate::column::actor::{
	series::SeriesMaterializationActor,
	table::{TableChanges, TableMaterializationActor},
};
use crate::subsystem::{StorageConfig, StorageSubsystem};

pub struct StorageSubsystemFactory {
	#[cfg_attr(not(feature = "column"), allow(dead_code))]
	config: StorageConfig,
	#[cfg(reifydb_target = "host")]
	column_dir: Option<PathBuf>,
	#[cfg(feature = "column")]
	column_store: Option<ColumnStore>,
}

impl StorageSubsystemFactory {
	pub fn new(config: StorageConfig) -> Self {
		Self {
			config,
			#[cfg(reifydb_target = "host")]
			column_dir: None,
			#[cfg(feature = "column")]
			column_store: None,
		}
	}

	#[cfg(reifydb_target = "host")]
	pub fn with_column_dir(mut self, dir: Option<PathBuf>) -> Self {
		self.column_dir = dir;
		self
	}

	#[cfg(feature = "column")]
	pub fn with_column_store(mut self, store: ColumnStore) -> Self {
		self.column_store = Some(store);
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
		let event_bus = ioc.resolve::<EventBus>()?;

		let block_store = match self.column_store.clone() {
			Some(store) => store,
			#[cfg(reifydb_target = "host")]
			None => match self.column_dir.clone() {
				Some(dir) => ColumnStore::host(dir)?,
				None => ColumnStore::memory()?,
			},
			#[cfg(not(reifydb_target = "host"))]
			None => ColumnStore::memory()?,
		};

		ioc.register_service::<Arc<ColumnStore>>(Arc::new(block_store.clone()));

		let compressor = || Compressor::new(block_store.session().clone());

		let table_changes = TableChanges::new();
		event_bus.register::<PostCommitEvent, _>(table_changes.clone());

		let table_actor = TableMaterializationActor::new(
			engine.clone(),
			block_store.clone(),
			compressor(),
			self.config.table_tick_interval,
			table_changes,
		);
		let table_handle = spawner.spawn_coordination("storage-materialize-table", table_actor);
		let table_ref = table_handle.actor_ref().clone();

		let series_actor = SeriesMaterializationActor::new(
			engine,
			block_store.clone(),
			compressor(),
			self.config.series_tick_interval,
			self.config.series_bucket_width,
			self.config.series_grace,
		);
		let series_handle = spawner.spawn_coordination("storage-materialize-series", series_actor);
		let series_ref = series_handle.actor_ref().clone();

		Ok(Box::new(StorageSubsystem::new(block_store, table_ref, series_ref)))
	}

	#[cfg(not(feature = "column"))]
	fn create(self: Box<Self>, _ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		Ok(Box::new(StorageSubsystem::new()))
	}
}
