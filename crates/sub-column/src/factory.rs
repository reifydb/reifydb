// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_column::{
	compress::{CompressConfig, Compressor},
	registry::SnapshotRegistry,
};
use reifydb_core::util::ioc::IocContainer;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_type::Result;

use crate::{
	actor::{series::SeriesMaterializationActor, table::TableMaterializationActor},
	subsystem::{StorageConfig, StorageSubsystem},
};

pub struct StorageSubsystemFactory {
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
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let runtime = ioc.resolve::<SharedRuntime>()?;
		let engine = ioc.resolve::<StandardEngine>()?;
		let actor_system = runtime.actor_system();
		let registry = SnapshotRegistry::new();

		let table_actor = TableMaterializationActor::new(
			engine.clone(),
			registry.clone(),
			Compressor::new(CompressConfig::default()),
			self.config.table_tick_interval,
		);
		let table_handle = actor_system.spawn_system("storage-materialize-table", table_actor);
		let table_ref = table_handle.actor_ref().clone();

		let series_actor = SeriesMaterializationActor::new(
			engine,
			registry.clone(),
			Compressor::new(CompressConfig::default()),
			self.config.series_tick_interval,
			self.config.series_bucket_width,
			self.config.series_grace,
		);
		let series_handle = actor_system.spawn_system("storage-materialize-series", series_actor);
		let series_ref = series_handle.actor_ref().clone();

		Ok(Box::new(StorageSubsystem::new(registry, table_ref, series_ref)))
	}
}
