// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{event::EventBus, interface::catalog::config::GetConfig};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::{clock::Clock, rng::Rng},
};
use reifydb_sqlite::{DbPath, SqliteConfig};
use reifydb_store_multi::{
	MultiStore,
	buffer::storage::BufferStorage,
	config::{BufferConfig as MultiBufferConfig, MultiStoreConfig},
};
use reifydb_store_single::{
	SingleStore,
	buffer::tier::BufferTier,
	config::{BufferConfig as SingleBufferConfig, SingleStoreConfig},
};
use reifydb_transaction::{multi::transaction::MultiTransaction, single::SingleTransaction};

pub mod embedded;
pub mod migration;
pub mod server;

/// Storage factory enum for deferred storage creation.
///
/// This allows the builder to create storage during the `build()` phase,
/// rather than requiring users to provide it upfront.
#[derive(Clone)]
pub enum StorageFactory {
	/// In-memory storage (non-persistent)
	Memory,
	/// SQLite-based persistent storage
	Sqlite(SqliteConfig),
}

impl StorageFactory {
	pub(crate) fn open_multi_buffer(&self) -> BufferStorage {
		match self {
			StorageFactory::Memory => BufferStorage::memory(),
			StorageFactory::Sqlite(config) => BufferStorage::sqlite(SqliteConfig {
				path: multi_db_path(&config.path),
				..config.clone()
			}),
		}
	}

	pub(crate) fn create_with_multi_buffer(
		&self,
		multi_buffer: BufferStorage,
		actor_system: &ActorSystem,
	) -> (MultiStore, SingleStore, SingleTransaction, EventBus) {
		match self {
			StorageFactory::Memory => create_memory_store_with(multi_buffer, actor_system),
			StorageFactory::Sqlite(config) => {
				create_sqlite_store_with(multi_buffer, config.clone(), actor_system)
			}
		}
	}
}

fn multi_db_path(path: &DbPath) -> DbPath {
	match path {
		DbPath::File(p) => DbPath::File(p.with_extension("").join("multi.db")),
		DbPath::Memory(p) => DbPath::Memory(p.with_extension("").join("multi.db")),
		DbPath::Tmpfs(p) => DbPath::Tmpfs(p.with_extension("").join("multi.db")),
	}
}

fn create_memory_store_with(
	multi_buffer: BufferStorage,
	actor_system: &ActorSystem,
) -> (MultiStore, SingleStore, SingleTransaction, EventBus) {
	let eventbus = EventBus::new(actor_system);

	let multi_store = MultiStore::standard(MultiStoreConfig {
		buffer: Some(MultiBufferConfig {
			storage: multi_buffer,
		}),
		persistent: None,
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus: eventbus.clone(),
		actor_system: actor_system.clone(),
		clock: Clock::Real,
	});

	let single_storage = BufferTier::memory();
	let single_store = SingleStore::standard(SingleStoreConfig {
		buffer: Some(SingleBufferConfig {
			storage: single_storage,
		}),
		event_bus: eventbus.clone(),
	});

	let transaction_single = SingleTransaction::new(single_store.clone(), eventbus.clone());
	(multi_store, single_store, transaction_single, eventbus)
}

fn create_sqlite_store_with(
	multi_buffer: BufferStorage,
	config: SqliteConfig,
	actor_system: &ActorSystem,
) -> (MultiStore, SingleStore, SingleTransaction, EventBus) {
	let eventbus = EventBus::new(actor_system);

	let multi_store = MultiStore::standard(MultiStoreConfig {
		buffer: Some(MultiBufferConfig {
			storage: multi_buffer,
		}),
		persistent: None,
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus: eventbus.clone(),
		actor_system: actor_system.clone(),
		clock: Clock::Real,
	});

	let single_path = match &config.path {
		DbPath::File(p) => DbPath::File(p.with_extension("").join("single.db")),
		DbPath::Memory(p) => DbPath::Memory(p.with_extension("").join("single.db")),
		DbPath::Tmpfs(p) => DbPath::Tmpfs(p.with_extension("").join("single.db")),
	};
	let single_config = SqliteConfig {
		path: single_path,
		..config.clone()
	};
	let single_storage = BufferTier::sqlite(single_config);
	let single_store = SingleStore::standard(SingleStoreConfig {
		buffer: Some(SingleBufferConfig {
			storage: single_storage,
		}),
		event_bus: eventbus.clone(),
	});

	let transaction_single = SingleTransaction::new(single_store.clone(), eventbus.clone());
	(multi_store, single_store, transaction_single, eventbus)
}

/// Convenience function to create a transaction layer
pub(crate) fn transaction(
	input: (MultiStore, SingleStore, SingleTransaction, EventBus),
	actor_system: ActorSystem,
	clock: Clock,
	rng: Rng,
	config: Arc<dyn GetConfig>,
) -> (MultiTransaction, SingleTransaction, EventBus) {
	let multi = MultiTransaction::new(input.0, input.2.clone(), input.3.clone(), actor_system, clock, rng, config)
		.unwrap();
	(multi, input.2, input.3)
}
