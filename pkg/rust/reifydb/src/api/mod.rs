// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{event::EventBus, interface::catalog::config::GetConfig};
use reifydb_runtime::{
	actor::system::ActorSpawner,
	context::{clock::Clock, rng::Rng},
	version_epoch::VersionEpoch,
};
use reifydb_sqlite::{DbPath, SqliteConfig};
use reifydb_store_multi::{
	MultiStore,
	config::{
		CommitBufferConfig as MultiCommitBufferConfig, MultiStoreConfig,
		PersistentConfig as MultiPersistentConfig,
	},
	tier::{commit::buffer::MultiCommitBufferTier, persistent::MultiPersistentTier, read::ReadBufferConfig},
};
use reifydb_store_operator::store::OperatorStore;
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

#[derive(Clone)]
pub enum StorageFactory {
	Memory,
	Sqlite(SqliteConfig),
}

impl StorageFactory {
	pub(crate) fn open_multi_commit_buffer(&self) -> MultiCommitBufferTier {
		MultiCommitBufferTier::memory()
	}

	pub(crate) fn open_multi_persistent(&self) -> Option<MultiPersistentTier> {
		match self {
			StorageFactory::Memory => None,
			StorageFactory::Sqlite(config) => Some(MultiPersistentTier::sqlite(multi_sqlite_config(config))),
		}
	}

	pub(crate) fn create_with_multi_commit_buffer(
		&self,
		multi_commit_buffer: MultiCommitBufferTier,
		multi_persistent: Option<MultiPersistentTier>,
		read: Option<ReadBufferConfig>,
		spawner: &ActorSpawner,
	) -> (MultiStore, SingleStore, OperatorStore, SingleTransaction, EventBus) {
		match self {
			StorageFactory::Memory => create_memory_store_with(multi_commit_buffer, spawner),
			StorageFactory::Sqlite(config) => create_sqlite_store_with(
				multi_commit_buffer,
				multi_persistent.expect("sqlite storage must supply an opened persistent tier"),
				read,
				config.clone(),
				spawner,
			),
		}
	}
}

fn multi_sqlite_config(config: &SqliteConfig) -> SqliteConfig {
	let path = match &config.path {
		DbPath::File(p) => DbPath::File(p.with_extension("").join("multi.db")),
		DbPath::Memory(p) => DbPath::Memory(p.with_extension("").join("multi.db")),
		DbPath::Tmpfs(p) => DbPath::Tmpfs(p.with_extension("").join("multi.db")),
	};
	SqliteConfig {
		path,
		..config.clone()
	}
}

fn create_memory_store_with(
	multi_commit_buffer: MultiCommitBufferTier,
	spawner: &ActorSpawner,
) -> (MultiStore, SingleStore, OperatorStore, SingleTransaction, EventBus) {
	let eventbus = EventBus::new(spawner);

	let multi_store = MultiStore::standard(MultiStoreConfig {
		commit: MultiCommitBufferConfig {
			storage: multi_commit_buffer,
		},
		persistent: None,
		read: None,
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

	let operator_store = OperatorStore::memory();

	let transaction_single = SingleTransaction::new(single_store.clone(), eventbus.clone());
	(multi_store, single_store, operator_store, transaction_single, eventbus)
}

fn create_sqlite_store_with(
	multi_commit_buffer: MultiCommitBufferTier,
	multi_persistent: MultiPersistentTier,
	read: Option<ReadBufferConfig>,
	config: SqliteConfig,
	spawner: &ActorSpawner,
) -> (MultiStore, SingleStore, OperatorStore, SingleTransaction, EventBus) {
	let eventbus = EventBus::new(spawner);

	let multi_store = MultiStore::standard(MultiStoreConfig {
		commit: MultiCommitBufferConfig {
			storage: multi_commit_buffer,
		},
		persistent: Some(MultiPersistentConfig::opened(multi_persistent)),
		read,
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

	let operator_path = match &config.path {
		DbPath::File(p) => DbPath::File(p.with_extension("").join("operator.db")),
		DbPath::Memory(p) => DbPath::Memory(p.with_extension("").join("operator.db")),
		DbPath::Tmpfs(p) => DbPath::Tmpfs(p.with_extension("").join("operator.db")),
	};
	let operator_store = OperatorStore::sqlite(SqliteConfig {
		path: operator_path,
		..config.clone()
	});

	let transaction_single = SingleTransaction::new(single_store.clone(), eventbus.clone());
	(multi_store, single_store, operator_store, transaction_single, eventbus)
}

pub(crate) fn transaction(
	input: (MultiStore, SingleStore, SingleTransaction, EventBus),
	spawner: ActorSpawner,
	clock: Clock,
	version_epoch: VersionEpoch,
	rng: Rng,
	config: Arc<dyn GetConfig>,
) -> (MultiTransaction, SingleTransaction, EventBus) {
	let multi = MultiTransaction::new(
		input.0,
		input.2.clone(),
		input.3.clone(),
		spawner,
		clock,
		version_epoch,
		rng,
		config,
	)
	.unwrap();
	(multi, input.2, input.3)
}
