// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::store::SingleWalCheckpoint;
use reifydb_runtime::{actor::system::ActorSpawner, context::clock::Clock};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_value::value::duration::Duration;

use crate::tier::{commit::buffer::SingleCommitBufferTier, persistent::SinglePersistentTier};

#[derive(Clone)]
pub struct SingleStoreConfig {
	pub commit: Option<CommitBufferConfig>,
	pub persistent: Option<PersistentConfig>,
	pub spawner: ActorSpawner,
	pub clock: Clock,
}

impl SingleStoreConfig {
	pub fn memory(spawner: ActorSpawner, clock: Clock) -> Self {
		Self {
			commit: Some(CommitBufferConfig {
				storage: SingleCommitBufferTier::memory(),
			}),
			persistent: None,
			spawner,
			clock,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(persistent: PersistentConfig, spawner: ActorSpawner, clock: Clock) -> Self {
		Self {
			commit: Some(CommitBufferConfig {
				storage: SingleCommitBufferTier::memory(),
			}),
			persistent: Some(persistent),
			spawner,
			clock,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_unbuffered(persistent: PersistentConfig, spawner: ActorSpawner, clock: Clock) -> Self {
		Self {
			commit: None,
			persistent: Some(persistent),
			spawner,
			clock,
		}
	}
}

#[derive(Clone)]
pub struct CommitBufferConfig {
	pub storage: SingleCommitBufferTier,
}

#[derive(Clone)]
pub struct PersistentConfig {
	pub storage: SinglePersistentTier,
	pub flush_interval: Duration,
	pub checkpoint: Arc<dyn SingleWalCheckpoint>,
}

impl PersistentConfig {
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(sqlite_config: SqliteConfig, checkpoint: Arc<dyn SingleWalCheckpoint>) -> Self {
		Self {
			storage: SinglePersistentTier::sqlite(sqlite_config),
			flush_interval: Duration::from_seconds(5).unwrap(),
			checkpoint,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_in_memory(checkpoint: Arc<dyn SingleWalCheckpoint>) -> (Self, SqliteTempPathGuard) {
		let (storage, guard) = SinglePersistentTier::sqlite_in_memory();
		(
			Self {
				storage,
				flush_interval: Duration::from_seconds(5).unwrap(),
				checkpoint,
			},
			guard,
		)
	}

	pub fn flush_interval(mut self, interval: Duration) -> Self {
		self.flush_interval = interval;
		self
	}
}
