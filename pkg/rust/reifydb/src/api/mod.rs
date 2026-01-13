// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_core::event::EventBus;
use reifydb_core::runtime::ComputePool;
use reifydb_store_transaction::{
	HotConfig, TransactionStore, TransactionStoreConfig, hot::HotStorage, sqlite::SqliteConfig,
};
use reifydb_transaction::{multi::TransactionMultiVersion, single::TransactionSingle};

pub mod embedded;
pub mod server;

/// Storage factory enum for deferred storage creation.
///
/// This allows the builder to create storage with the appropriate `ComputePool`
/// during the `build()` phase, rather than requiring users to provide it upfront.
#[derive(Clone)]
pub enum StorageFactory {
	/// In-memory storage (non-persistent)
	Memory,
	/// SQLite-based persistent storage
	Sqlite(SqliteConfig),
}

impl StorageFactory {
	/// Create the storage with the given compute pool.
	pub(crate) fn create(
		&self,
		compute_pool: ComputePool,
	) -> (TransactionStore, TransactionSingle, EventBus) {
		match self {
			StorageFactory::Memory => create_memory_store(compute_pool),
			StorageFactory::Sqlite(config) => create_sqlite_store(config.clone()),
		}
	}
}

/// Internal: Create in-memory storage with the given compute pool.
fn create_memory_store(
	compute_pool: ComputePool,
) -> (TransactionStore, TransactionSingle, EventBus) {
	let eventbus = EventBus::new();
	let storage = HotStorage::memory(compute_pool);
	let store = TransactionStore::standard(TransactionStoreConfig {
		hot: Some(HotConfig {
			storage,
			retention_period: Duration::from_millis(200),
		}),
		warm: None,
		cold: None,
		retention: Default::default(),
		merge_config: Default::default(),
		stats: Default::default(),
		event_bus: eventbus.clone(),
	});

	(store.clone(), TransactionSingle::svl(store.clone(), eventbus.clone()), eventbus)
}

/// Internal: Create SQLite storage with the given configuration.
fn create_sqlite_store(config: SqliteConfig) -> (TransactionStore, TransactionSingle, EventBus) {
	let eventbus = EventBus::new();
	let storage = HotStorage::sqlite(config);
	let store = TransactionStore::standard(TransactionStoreConfig {
		hot: Some(HotConfig {
			storage,
			retention_period: Duration::from_millis(200),
		}),
		warm: None,
		cold: None,
		retention: Default::default(),
		merge_config: Default::default(),
		stats: Default::default(),
		event_bus: eventbus.clone(),
	});

	(store.clone(), TransactionSingle::svl(store.clone(), eventbus.clone()), eventbus)
}

/// Convenience function to create a transaction layer
pub(crate) fn transaction(
	input: (TransactionStore, TransactionSingle, EventBus),
) -> (TransactionMultiVersion, TransactionSingle, EventBus) {
	let multi = TransactionMultiVersion::new(input.0, input.1.clone(), input.2.clone()).unwrap();
	(multi, input.1, input.2)
}
