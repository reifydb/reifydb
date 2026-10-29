// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{path::PathBuf, sync::Arc};

use reifydb_core::{event::EventBus, interface::catalog::config::GetConfig};
use reifydb_runtime::{
	actor::system::ActorSpawner,
	context::{clock::Clock, rng::Rng},
	io::fs::{Fs, ReadDir, host::HostFs},
	version_epoch::VersionEpoch,
};
use reifydb_sqlite::{DbPath, SqliteConfig, SynchronousMode};
use reifydb_store_commit::store::CommitStore;
use reifydb_store_multi::{
	MultiStore,
	config::{
		CommitStoreConfig as MultiCommitStoreConfig, MultiStoreConfig,
		PersistentConfig as MultiPersistentConfig,
	},
	tier::{persistent::MultiPersistentTier, point::MultiPointConfig, range::MultiRangeConfig},
};
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig, ResidentConfig},
	persistent::PersistentTier,
	range::OperatorRangeConfig,
	resident::{Resident, ResidentLimits},
	store::OperatorStore,
};
use reifydb_store_single::{
	SingleStore,
	config::{
		CommitBufferConfig as SingleCommitBufferConfig, PersistentConfig as SinglePersistentConfig,
		SingleStoreConfig,
	},
	tier::commit::buffer::SingleCommitBufferTier,
};
use reifydb_transaction::{
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	wal::{
		CheckpointedStore, CommitWal, Recovery, floor_checkpoint, memory_wal, nop_wal, open_wal,
		replay_operator, replay_single,
	},
};
use reifydb_value::value::duration::Duration;

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
	pub(crate) fn open_multi_commit_store(&self) -> CommitStore {
		CommitStore::new()
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
	pub(crate) fn create_with_multi_commit_store(
		&self,
		multi_commit_store: CommitStore,
		multi_persistent: Option<MultiPersistentTier>,
		multi_point: Option<MultiPointConfig>,
		multi_range: Option<MultiRangeConfig>,
		operator_range: Option<OperatorRangeConfig>,
		operator_wal_autocheckpoint: u32,
		operator_resident: ResidentLimits,
		operator_flush_interval: Duration,
		operator_persistent: Option<PersistentTier>,
		wal: &CommitWal,
		wal_enabled: bool,
		clock: &Clock,
		spawner: &ActorSpawner,
	) -> (MultiStore, SingleStore, OperatorStore, SingleTransaction, EventBus) {
		match self {
			StorageFactory::Memory => create_memory_store_with(
				multi_commit_store,
				operator_resident,
				operator_flush_interval,
				operator_persistent,
				wal,
				wal_enabled,
				clock,
				spawner,
			),
			StorageFactory::Sqlite(config) => create_sqlite_store_with(
				multi_commit_store,
				multi_persistent.expect("sqlite storage must supply an opened persistent tier"),
				multi_point,
				multi_range,
				operator_range,
				operator_wal_autocheckpoint,
				operator_resident,
				operator_flush_interval,
				operator_persistent,
				config.clone(),
				wal,
				wal_enabled,
				clock,
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
		synchronous_mode: Some(SynchronousMode::Full),
		wal_autocheckpoint: None,
		..config.clone()
	}
}

pub(crate) fn single_sqlite_config(config: &SqliteConfig) -> SqliteConfig {
	let path = match &config.path {
		DbPath::File(p) => DbPath::File(p.with_extension("").join("single.db")),
		DbPath::Memory(p) => DbPath::Memory(p.with_extension("").join("single.db")),
		DbPath::Tmpfs(p) => DbPath::Tmpfs(p.with_extension("").join("single.db")),
	};
	SqliteConfig {
		path,
		synchronous_mode: Some(SynchronousMode::Full),
		wal_autocheckpoint: None,
		..config.clone()
	}
}

#[cfg(all(feature = "column", reifydb_target = "host"))]
pub(crate) fn column_dir(config: &SqliteConfig) -> PathBuf {
	match &config.path {
		DbPath::File(p) | DbPath::Memory(p) | DbPath::Tmpfs(p) => p.with_extension("").join("column"),
	}
}

pub(crate) fn wal_dir(config: &SqliteConfig) -> PathBuf {
	match &config.path {
		DbPath::File(p) | DbPath::Memory(p) | DbPath::Tmpfs(p) => p.with_extension("").join("wal"),
	}
}

pub(crate) fn commit_wal(factory: &StorageFactory, clock: &Clock, enabled: bool) -> CommitWal {
	if !enabled {
		if let StorageFactory::Sqlite(config) = factory {
			let dir = wal_dir(config);
			let parent =
				dir.parent().unwrap_or_else(|| panic!("commit log at {} has no parent", dir.display()));
			let entries = Fs::Host(HostFs::new()).read_dir(parent).unwrap_or_else(|err| {
				panic!("{} could not be listed at boot: {err}", parent.display())
			});
			if entries.iter().any(|entry| entry == &dir) {
				panic!(
					"commit log at {} exists but the wal is disabled: delete it, or boot once with \
					 the wal enabled to replay it",
					dir.display()
				);
			}
		}
		return nop_wal().unwrap_or_else(|err| panic!("no-op commit log failed to open: {err}"));
	}
	match factory {
		StorageFactory::Memory => memory_wal(clock.clone())
			.unwrap_or_else(|err| panic!("in-memory commit log failed to open: {err}")),
		StorageFactory::Sqlite(config) => {
			let dir = wal_dir(config);
			open_wal(Fs::Host(HostFs::new()), clock.clone(), &dir)
				.unwrap_or_else(|err| panic!("commit log at {} failed to open: {err}", dir.display()))
		}
	}
}

fn create_memory_store_with(
	multi_commit_store: CommitStore,
	operator_resident: ResidentLimits,
	operator_flush_interval: Duration,
	operator_persistent: Option<PersistentTier>,
	wal: &CommitWal,
	wal_enabled: bool,
	clock: &Clock,
	spawner: &ActorSpawner,
) -> (MultiStore, SingleStore, OperatorStore, SingleTransaction, EventBus) {
	let eventbus = EventBus::new(spawner);

	let multi_store = MultiStore::standard(MultiStoreConfig {
		commit: MultiCommitStoreConfig {
			storage: multi_commit_store,
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

	let operator_store = match operator_persistent {
		None => OperatorStore::standard(OperatorStoreConfig::memory(spawner.clone())),
		Some(persistent) => OperatorStore::standard(OperatorStoreConfig {
			resident: ResidentConfig {
				storage: Resident::with_limits(operator_resident),
				flush_interval: operator_flush_interval,
			},
			persistent: Some(OperatorPersistentConfig::opened(persistent)),
			..OperatorStoreConfig::memory(spawner.clone())
		}),
	};

	let saved_lsn = single_store
		.saved_lsn()
		.unwrap_or_else(|err| panic!("single.db saved lsn could not be read at boot: {err}"));
	replay_single(wal, &single_store, saved_lsn, wal_enabled);
	let operator_saved_lsn = operator_store
		.saved_lsn()
		.unwrap_or_else(|err| panic!("operator.db saved lsn could not be read at boot: {err}"));
	replay_operator(wal, &operator_store, operator_saved_lsn, wal_enabled);
	let transaction_single =
		SingleTransaction::new(single_store.clone(), eventbus.clone(), wal.clone(), clock.clone());
	(multi_store, single_store, operator_store, transaction_single, eventbus)
}

#[allow(clippy::too_many_arguments)]
fn create_sqlite_store_with(
	multi_commit_store: CommitStore,
	multi_persistent: MultiPersistentTier,
	multi_point: Option<MultiPointConfig>,
	multi_range: Option<MultiRangeConfig>,
	operator_range: Option<OperatorRangeConfig>,
	operator_wal_autocheckpoint: u32,
	operator_resident: ResidentLimits,
	operator_flush_interval: Duration,
	operator_persistent: Option<PersistentTier>,
	config: SqliteConfig,
	wal: &CommitWal,
	wal_enabled: bool,
	clock: &Clock,
	spawner: &ActorSpawner,
) -> (MultiStore, SingleStore, OperatorStore, SingleTransaction, EventBus) {
	let eventbus = EventBus::new(spawner);

	let multi_store = MultiStore::standard(MultiStoreConfig {
		commit: MultiCommitStoreConfig {
			storage: multi_commit_store,
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
		persistent: Some(SinglePersistentConfig::sqlite(
			single_config,
			floor_checkpoint(wal, CheckpointedStore::Single),
		)),
		spawner: spawner.clone(),
		clock: Clock::Real,
	});

	let operator_path = match &config.path {
		DbPath::File(p) => DbPath::File(p.with_extension("").join("operator.db")),
		DbPath::Memory(p) => DbPath::Memory(p.with_extension("").join("operator.db")),
		DbPath::Tmpfs(p) => DbPath::Tmpfs(p.with_extension("").join("operator.db")),
	};
	let operator_persistent = operator_persistent.unwrap_or_else(|| {
		let sqlite = PersistentTier::sqlite(SqliteConfig {
			path: operator_path,
			wal_autocheckpoint: None,
			..config.clone()
		});
		sqlite.set_checkpoint_threshold(operator_wal_autocheckpoint);
		sqlite
	});
	let operator_persistent = match operator_persistent {
		PersistentTier::Sqlite(_) => OperatorPersistentConfig::opened_with_checkpoint(
			operator_persistent,
			floor_checkpoint(wal, CheckpointedStore::Operator),
		),
		storage => OperatorPersistentConfig::opened(storage),
	};
	let operator_store = OperatorStore::standard(OperatorStoreConfig {
		range: operator_range,
		resident: ResidentConfig {
			storage: Resident::with_limits(operator_resident),
			flush_interval: operator_flush_interval,
		},
		..OperatorStoreConfig::sqlite(operator_persistent, spawner.clone())
	});

	let saved_lsn = single_store
		.saved_lsn()
		.unwrap_or_else(|err| panic!("single.db saved lsn could not be read at boot: {err}"));
	replay_single(wal, &single_store, saved_lsn, wal_enabled);
	let operator_saved_lsn = operator_store
		.saved_lsn()
		.unwrap_or_else(|err| panic!("operator.db saved lsn could not be read at boot: {err}"));
	replay_operator(wal, &operator_store, operator_saved_lsn, wal_enabled);
	let transaction_single =
		SingleTransaction::new(single_store.clone(), eventbus.clone(), wal.clone(), clock.clone());
	(multi_store, single_store, operator_store, transaction_single, eventbus)
}

pub(crate) fn transaction(
	input: (MultiStore, SingleStore, SingleTransaction, EventBus),
	spawner: ActorSpawner,
	clock: Clock,
	version_epoch: VersionEpoch,
	rng: Rng,
	config: Arc<dyn GetConfig>,
	wal: CommitWal,
	recovery: Recovery,
) -> (MultiTransaction, SingleTransaction, EventBus) {
	let multi = MultiTransaction::new(
		input.0,
		input.3.clone(),
		spawner,
		clock,
		version_epoch,
		rng,
		config,
		wal,
		recovery,
	)
	.unwrap();
	let checkpoint = multi.store_checkpoint(CheckpointedStore::Multi).unwrap();
	multi.store().set_wal_checkpoint(checkpoint);
	(multi, input.2, input.3)
}
