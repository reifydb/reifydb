// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_core::event::EventBus;
use reifydb_core::runtime::ComputePool;
use reifydb_store_multi::{
	HotConfig as MultiHotConfig, MultiStore, MultiStoreConfig, hot::HotStorage,
	hot::sqlite::DbPath,
	sqlite::SqliteConfig,
};
use reifydb_store_single::{
	HotConfig as SingleHotConfig, SingleStore, SingleStoreConfig,
	sqlite::SqliteConfig as SingleSqliteConfig,
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
	) -> (MultiStore, SingleStore, TransactionSingle, EventBus) {
		match self {
			StorageFactory::Memory => create_memory_store(compute_pool),
			StorageFactory::Sqlite(config) => create_sqlite_store(config.clone()),
		}
	}
}

/// Internal: Create in-memory storage with the given compute pool.
fn create_memory_store(
	compute_pool: ComputePool,
) -> (MultiStore, SingleStore, TransactionSingle, EventBus) {
	let eventbus = EventBus::new();

	// Create multi-version store
	let multi_storage = HotStorage::memory(compute_pool.clone());
	let multi_store = MultiStore::standard(MultiStoreConfig {
		hot: Some(MultiHotConfig {
			storage: multi_storage,
			retention_period: Duration::from_millis(200),
		}),
		warm: None,
		cold: None,
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus: eventbus.clone(),
	});

	// Create single-version store
	let single_storage = reifydb_store_single::hot::HotTier::memory(compute_pool);
	let single_store = SingleStore::standard(SingleStoreConfig {
		hot: Some(SingleHotConfig { storage: single_storage }),
		event_bus: eventbus.clone(),
	});

	let transaction_single = TransactionSingle::svl(single_store.clone(), eventbus.clone());
	(multi_store, single_store, transaction_single, eventbus)
}

/// Internal: Create SQLite storage with the given configuration.
fn create_sqlite_store(config: SqliteConfig) -> (MultiStore, SingleStore, TransactionSingle, EventBus) {
	let eventbus = EventBus::new();

	// Modify config to use multi.db in a directory named after the UUID
	let multi_path = match &config.path {
		DbPath::File(p) => DbPath::File(p.with_extension("").join("multi.db")),
		DbPath::Memory(p) => DbPath::Memory(p.with_extension("").join("multi.db")),
		DbPath::Tmpfs(p) => DbPath::Tmpfs(p.with_extension("").join("multi.db")),
	};
	let multi_config = SqliteConfig {
		path: multi_path,
		..config.clone()
	};

	// Create multi-version store
	let multi_storage = HotStorage::sqlite(multi_config);
	let multi_store = MultiStore::standard(MultiStoreConfig {
		hot: Some(MultiHotConfig {
			storage: multi_storage,
			retention_period: Duration::from_millis(200),
		}),
		warm: None,
		cold: None,
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus: eventbus.clone(),
	});

	// Create single-version config with single.db in same directory
	let single_path = match &config.path {
		DbPath::File(p) => p.with_extension("").join("single.db"),
		DbPath::Memory(p) => p.with_extension("").join("single.db"),
		DbPath::Tmpfs(p) => p.with_extension("").join("single.db"),
	};
	let single_config = SingleSqliteConfig::new(single_path);
	let single_storage = reifydb_store_single::hot::HotTier::sqlite(single_config);
	let single_store = SingleStore::standard(SingleStoreConfig {
		hot: Some(SingleHotConfig { storage: single_storage }),
		event_bus: eventbus.clone(),
	});

	let transaction_single = TransactionSingle::svl(single_store.clone(), eventbus.clone());
	(multi_store, single_store, transaction_single, eventbus)
}

/// Convenience function to create a transaction layer
pub(crate) fn transaction(
	input: (MultiStore, SingleStore, TransactionSingle, EventBus),
) -> (TransactionMultiVersion, TransactionSingle, EventBus) {
	let multi = TransactionMultiVersion::new(input.0, input.2.clone(), input.3.clone()).unwrap();
	(multi, input.2, input.3)
}
