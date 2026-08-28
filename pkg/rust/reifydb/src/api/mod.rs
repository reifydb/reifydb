// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{event::EventBus, interface::catalog::config::GetConfig};
use reifydb_runtime::{
	actor::system::ActorSpawner,
	context::{clock::Clock, rng::Rng},
	version_epoch::VersionEpoch,
};
use reifydb_sqlite::{DbPath, JournalMode, SqliteConfig};
use reifydb_store_cdc::{
	config::{CdcCommitConfig, CdcPersistentConfig, CdcStoreConfig},
	store::CdcStore,
	tier::read::CdcReadConfig,
};
use reifydb_store_multi::{
	MultiStore,
	config::{
		CommitBufferConfig as MultiCommitBufferConfig, MultiStoreConfig,
		PersistentConfig as MultiPersistentConfig,
	},
	tier::{
		commit::buffer::MultiCommitBufferTier, persistent::MultiPersistentTier, point::MultiPointConfig,
		range::MultiRangeConfig,
	},
};
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorResidentStateConfig, OperatorStoreConfig},
	store::OperatorStore,
	tier::{
		persistent::OperatorPersistentTier, point::OperatorPointConfig, range::OperatorRangeConfig,
		resident::OperatorResidentState,
	},
};
use reifydb_store_single::{
	SingleStore,
	config::{
		CommitBufferConfig as SingleCommitBufferConfig, PersistentConfig as SinglePersistentConfig,
		SingleStoreConfig,
	},
	tier::commit::buffer::SingleCommitBufferTier,
};
use reifydb_transaction::{multi::transaction::MultiTransaction, single::SingleTransaction};
use reifydb_value::byte_size::ByteSize;

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
			StorageFactory::Sqlite(config) => {
				Some(MultiPersistentTier::sqlite(multi_sqlite_config(config)))
			}
		}
	}

	#[allow(clippy::too_many_arguments)]
	pub(crate) fn create_with_multi_commit_buffer(
		&self,
		multi_commit_buffer: MultiCommitBufferTier,
		multi_persistent: Option<MultiPersistentTier>,
		multi_point: Option<MultiPointConfig>,
		multi_range: Option<MultiRangeConfig>,
		operator_point: Option<OperatorPointConfig>,
		operator_range: Option<OperatorRangeConfig>,
		cdc_commit: CdcCommitConfig,
		cdc_read: Option<CdcReadConfig>,
		cdc_wal_autocheckpoint: u32,
		operator_wal_autocheckpoint: u32,
		operator_flush_budget: ByteSize,
		spawner: &ActorSpawner,
	) -> (MultiStore, SingleStore, OperatorStore, CdcStore, SingleTransaction, EventBus) {
		match self {
			StorageFactory::Memory => {
				create_memory_store_with(multi_commit_buffer, cdc_commit, cdc_read, spawner)
			}
			StorageFactory::Sqlite(config) => create_sqlite_store_with(
				multi_commit_buffer,
				multi_persistent.expect("sqlite storage must supply an opened persistent tier"),
				multi_point,
				multi_range,
				operator_point,
				operator_range,
				cdc_commit,
				cdc_read,
				cdc_wal_autocheckpoint,
				operator_wal_autocheckpoint,
				operator_flush_budget,
				config.clone(),
				spawner,
			),
		}
	}
}

/// The catalog key is the single source for this pragma, so an inherited value must not reach open.
pub(crate) fn multi_sqlite_config(config: &SqliteConfig) -> SqliteConfig {
	let path = match &config.path {
		DbPath::File(p) => DbPath::File(p.with_extension("").join("multi.db")),
		DbPath::Memory(p) => DbPath::Memory(p.with_extension("").join("multi.db")),
		DbPath::Tmpfs(p) => DbPath::Tmpfs(p.with_extension("").join("multi.db")),
	};
	SqliteConfig {
		path,
		wal_autocheckpoint: None,
		..config.clone()
	}
}

/// One connection behind a mutex never contends with itself, so an inherited write-ahead log buys nothing
/// and leaves a `-wal` companion no checkpointer here ever truncates.
pub(crate) fn single_sqlite_config(config: &SqliteConfig) -> SqliteConfig {
	let path = match &config.path {
		DbPath::File(p) => DbPath::File(p.with_extension("").join("single.db")),
		DbPath::Memory(p) => DbPath::Memory(p.with_extension("").join("single.db")),
		DbPath::Tmpfs(p) => DbPath::Tmpfs(p.with_extension("").join("single.db")),
	};
	SqliteConfig {
		path,
		journal_mode: Some(JournalMode::Persist),
		wal_autocheckpoint: None,
		..config.clone()
	}
}

/// CDC is written on the commit path, so this is the sole control over how often cdc.db's write-ahead log
/// is folded back and therefore how often a commit pays an inline checkpoint.
pub(crate) fn cdc_sqlite_config(config: &SqliteConfig, wal_autocheckpoint: u32) -> SqliteConfig {
	let path = match &config.path {
		DbPath::File(p) => DbPath::File(p.with_extension("").join("cdc.db")),
		DbPath::Memory(p) => DbPath::Memory(p.with_extension("").join("cdc.db")),
		DbPath::Tmpfs(p) => DbPath::Tmpfs(p.with_extension("").join("cdc.db")),
	};
	SqliteConfig {
		path,
		wal_autocheckpoint: Some(wal_autocheckpoint),
		..config.clone()
	}
}

fn create_memory_store_with(
	multi_commit_buffer: MultiCommitBufferTier,
	cdc_commit: CdcCommitConfig,
	cdc_read: Option<CdcReadConfig>,
	spawner: &ActorSpawner,
) -> (MultiStore, SingleStore, OperatorStore, CdcStore, SingleTransaction, EventBus) {
	let eventbus = EventBus::new(spawner);

	let multi_store = MultiStore::standard(MultiStoreConfig {
		commit: MultiCommitBufferConfig {
			storage: multi_commit_buffer,
		},
		persistent: None,
		point: None,
		range: None,
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus: eventbus.clone(),
		spawner: spawner.clone(),
		clock: Clock::Real,
	});

	let single_store = SingleStore::standard(SingleStoreConfig {
		commit: Some(SingleCommitBufferConfig {
			storage: SingleCommitBufferTier::memory(),
		}),
		persistent: None,
		spawner: spawner.clone(),
		clock: Clock::Real,
	});

	let operator_store = OperatorStore::standard(OperatorStoreConfig::memory(spawner.clone(), Clock::Real));

	let cdc_store = CdcStore::new(CdcStoreConfig {
		commit: cdc_commit,
		persistent: CdcPersistentConfig::memory(),
		read: cdc_read,
		spawner: spawner.clone(),
		clock: Clock::Real,
	});

	let transaction_single = SingleTransaction::new(single_store.clone(), eventbus.clone());
	(multi_store, single_store, operator_store, cdc_store, transaction_single, eventbus)
}

#[allow(clippy::too_many_arguments)]
fn create_sqlite_store_with(
	multi_commit_buffer: MultiCommitBufferTier,
	multi_persistent: MultiPersistentTier,
	multi_point: Option<MultiPointConfig>,
	multi_range: Option<MultiRangeConfig>,
	operator_point: Option<OperatorPointConfig>,
	operator_range: Option<OperatorRangeConfig>,
	cdc_commit: CdcCommitConfig,
	cdc_read: Option<CdcReadConfig>,
	cdc_wal_autocheckpoint: u32,
	operator_wal_autocheckpoint: u32,
	operator_flush_budget: ByteSize,
	config: SqliteConfig,
	spawner: &ActorSpawner,
) -> (MultiStore, SingleStore, OperatorStore, CdcStore, SingleTransaction, EventBus) {
	let eventbus = EventBus::new(spawner);

	let multi_store = MultiStore::standard(MultiStoreConfig {
		commit: MultiCommitBufferConfig {
			storage: multi_commit_buffer,
		},
		persistent: Some(MultiPersistentConfig::opened(multi_persistent)),
		point: multi_point,
		range: multi_range,
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus: eventbus.clone(),
		spawner: spawner.clone(),
		clock: Clock::Real,
	});

	let single_config = single_sqlite_config(&config);
	let single_store = SingleStore::standard(SingleStoreConfig {
		commit: Some(SingleCommitBufferConfig {
			storage: SingleCommitBufferTier::memory(),
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
	let operator_persistent = OperatorPersistentTier::sqlite(SqliteConfig {
		path: operator_path,
		wal_autocheckpoint: None,
		..config.clone()
	});
	operator_persistent.set_checkpoint_threshold(operator_wal_autocheckpoint);
	let operator_store = OperatorStore::standard(OperatorStoreConfig {
		point: operator_point,
		range: operator_range,
		resident: OperatorResidentStateConfig {
			storage: OperatorResidentState::with_budget(operator_flush_budget),
		},
		..OperatorStoreConfig::sqlite(
			OperatorPersistentConfig::opened(operator_persistent),
			spawner.clone(),
			Clock::Real,
		)
	});

	let cdc_store = CdcStore::new(CdcStoreConfig {
		commit: cdc_commit,
		persistent: CdcPersistentConfig::sqlite(cdc_sqlite_config(&config, cdc_wal_autocheckpoint)),
		read: cdc_read,
		spawner: spawner.clone(),
		clock: Clock::Real,
	});

	let transaction_single = SingleTransaction::new(single_store.clone(), eventbus.clone());
	(multi_store, single_store, operator_store, cdc_store, transaction_single, eventbus)
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
