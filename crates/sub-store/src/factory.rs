// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#[cfg(feature = "column")]
use std::sync::Arc;

#[cfg(feature = "column")]
use reifydb_column::compress::{CompressConfig, Compressor};
use reifydb_core::util::ioc::IocContainer;
#[cfg(feature = "column")]
use reifydb_engine::engine::StandardEngine;
#[cfg(feature = "column")]
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_type::Result;

#[cfg(feature = "column")]
use crate::column::{
	actor::{series::SeriesMaterializationActor, table::TableMaterializationActor},
	block_store::ColumnBlockStore,
};
use crate::subsystem::{StorageConfig, StorageSubsystem};

pub struct StorageSubsystemFactory {
	#[cfg_attr(not(feature = "column"), allow(dead_code))]
	config: StorageConfig,
}

impl StorageSubsystemFactory {
	pub fn new(config: StorageConfig) -> Self {
		Self {
			config,
		}
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
		let runtime = ioc.resolve::<SharedRuntime>()?;
		let engine = ioc.resolve::<StandardEngine>()?;
		let actor_system = runtime.actor_system();
		let block_store = ColumnBlockStore::new();
		ioc.register_service::<Arc<ColumnBlockStore>>(Arc::new(block_store.clone()));

		let table_actor = TableMaterializationActor::new(
			engine.clone(),
			block_store.clone(),
			Compressor::new(CompressConfig::default()),
			self.config.table_tick_interval,
		);
		let table_handle = actor_system.spawn_system("storage-materialize-table", table_actor);
		let table_ref = table_handle.actor_ref().clone();

		let series_actor = SeriesMaterializationActor::new(
			engine,
			block_store.clone(),
			Compressor::new(CompressConfig::default()),
			self.config.series_tick_interval,
			self.config.series_bucket_width,
			self.config.series_grace,
		);
		let series_handle = actor_system.spawn_system("storage-materialize-series", series_actor);
		let series_ref = series_handle.actor_ref().clone();

		Ok(Box::new(StorageSubsystem::new(block_store, table_ref, series_ref)))
	}

	#[cfg(not(feature = "column"))]
	fn create(self: Box<Self>, _ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		Ok(Box::new(StorageSubsystem::new()))
	}
}
