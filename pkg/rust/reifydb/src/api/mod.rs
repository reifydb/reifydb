// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{event::EventBus, interface::catalog::config::GetConfig};
use reifydb_runtime::{
	actor::system::ActorSpawner,
	context::{clock::Clock, rng::Rng},
};
use reifydb_sqlite::{DbPath, SqliteConfig};
use reifydb_store_multi::{
	MultiStore,
	config::{
		CommitBufferConfig as MultiCommitBufferConfig, MultiStoreConfig,
		PersistentConfig as MultiPersistentConfig,
	},
	tier::commit::buffer::MultiCommitBufferTier,
};
use reifydb_store_single::{
	SingleStore,
	buffer::tier::SingleBufferTier,
	config::{BufferConfig as SingleBufferConfig, PersistentConfig as SinglePersistentConfig, SingleStoreConfig},
};
use reifydb_transaction::{multi::transaction::MultiTransaction, single::SingleTransaction};

pub mod embedded;
mod export;
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
	/// SQLite-based persistent storage with no in-memory buffer
	SqliteWithoutBuffer(SqliteConfig),
}

impl StorageFactory {
	pub(crate) fn open_multi_commit_buffer(&self) -> MultiCommitBufferTier {
		MultiCommitBufferTier::memory()
	}

	pub(crate) fn create_with_multi_commit_buffer(
		&self,
		multi_commit_buffer: MultiCommitBufferTier,
		spawner: &ActorSpawner,
	) -> (MultiStore, SingleStore, SingleTransaction, EventBus) {
		match self {
			StorageFactory::Memory => create_memory_store_with(multi_commit_buffer, spawner),
			StorageFactory::Sqlite(config) => {
				create_sqlite_store_with(multi_commit_buffer, config.clone(), spawner)
			}
			StorageFactory::SqliteWithoutBuffer(config) => {
				create_sqlite_without_buffer_store_with(config.clone(), spawner)
			}
		}
	}
}

fn create_memory_store_with(
	multi_commit_buffer: MultiCommitBufferTier,
	spawner: &ActorSpawner,
) -> (MultiStore, SingleStore, SingleTransaction, EventBus) {
	let eventbus = EventBus::new(spawner);

	let multi_store = MultiStore::standard(MultiStoreConfig {
		commit: Some(MultiCommitBufferConfig {
			storage: multi_commit_buffer,
		}),
		persistent: None,
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus: eventbus.clone(),
		spawner: spawner.clone(),
		clock: Clock::Real,
	});

	let single_store = SingleStore::standard(SingleStoreConfig {
		buffer: Some(SingleBufferConfig {
			storage: SingleBufferTier::memory(),
		}),
		persistent: None,
		spawner: spawner.clone(),
		clock: Clock::Real,
	});

	let transaction_single = SingleTransaction::new(single_store.clone(), eventbus.clone());
	(multi_store, single_store, transaction_single, eventbus)
}

fn create_sqlite_store_with(
	multi_commit_buffer: MultiCommitBufferTier,
	config: SqliteConfig,
	spawner: &ActorSpawner,
) -> (MultiStore, SingleStore, SingleTransaction, EventBus) {
	let eventbus = EventBus::new(spawner);

	let multi_path = match &config.path {
		DbPath::File(p) => DbPath::File(p.with_extension("").join("multi.db")),
		DbPath::Memory(p) => DbPath::Memory(p.with_extension("").join("multi.db")),
		DbPath::Tmpfs(p) => DbPath::Tmpfs(p.with_extension("").join("multi.db")),
	};
	let multi_config = SqliteConfig {
		path: multi_path,
		..config.clone()
	};

	let multi_store = MultiStore::standard(MultiStoreConfig {
		commit: Some(MultiCommitBufferConfig {
			storage: multi_commit_buffer,
		}),
		persistent: Some(MultiPersistentConfig::sqlite(multi_config)),
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus: eventbus.clone(),
		spawner: spawner.clone(),
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
	let single_store = SingleStore::standard(SingleStoreConfig {
		buffer: Some(SingleBufferConfig {
			storage: SingleBufferTier::memory(),
		}),
		persistent: Some(SinglePersistentConfig::sqlite(single_config)),
		spawner: spawner.clone(),
		clock: Clock::Real,
	});

	let transaction_single = SingleTransaction::new(single_store.clone(), eventbus.clone());
	(multi_store, single_store, transaction_single, eventbus)
}

fn create_sqlite_without_buffer_store_with(
	config: SqliteConfig,
	spawner: &ActorSpawner,
) -> (MultiStore, SingleStore, SingleTransaction, EventBus) {
	let eventbus = EventBus::new(spawner);

	let multi_path = match &config.path {
		DbPath::File(p) => DbPath::File(p.with_extension("").join("multi.db")),
		DbPath::Memory(p) => DbPath::Memory(p.with_extension("").join("multi.db")),
		DbPath::Tmpfs(p) => DbPath::Tmpfs(p.with_extension("").join("multi.db")),
	};
	let multi_config = SqliteConfig {
		path: multi_path,
		..config.clone()
	};

	let multi_store = MultiStore::standard(MultiStoreConfig {
		commit: None,
		persistent: Some(MultiPersistentConfig::sqlite(multi_config)),
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus: eventbus.clone(),
		spawner: spawner.clone(),
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
	let single_store = SingleStore::standard(SingleStoreConfig {
		buffer: None,
		persistent: Some(SinglePersistentConfig::sqlite(single_config)),
		spawner: spawner.clone(),
		clock: Clock::Real,
	});

	let transaction_single = SingleTransaction::new(single_store.clone(), eventbus.clone());
	(multi_store, single_store, transaction_single, eventbus)
}

/// Convenience function to create a transaction layer
pub(crate) fn transaction(
	input: (MultiStore, SingleStore, SingleTransaction, EventBus),
	spawner: ActorSpawner,
	clock: Clock,
	rng: Rng,
	config: Arc<dyn GetConfig>,
) -> (MultiTransaction, SingleTransaction, EventBus) {
	let multi =
		MultiTransaction::new(input.0, input.2.clone(), input.3.clone(), spawner, clock, rng, config).unwrap();
	(multi, input.2, input.3)
}
