// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::event::EventBus;
use reifydb_runtime::{actor::system::ActorSpawner, context::clock::Clock};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_value::value::duration::Duration;

use crate::tier::{commit::buffer::MultiCommitBufferTier, persistent::MultiPersistentTier};

#[derive(Clone)]
pub struct MultiStoreConfig {
	pub commit: Option<CommitBufferConfig>,
	pub persistent: Option<PersistentConfig>,
	pub retention: RetentionConfig,
	pub merge_config: MergeConfig,
	pub event_bus: EventBus,
	pub spawner: ActorSpawner,
	pub clock: Clock,
}

impl MultiStoreConfig {
	pub fn memory(spawner: ActorSpawner, clock: Clock, event_bus: EventBus) -> Self {
		Self {
			commit: Some(CommitBufferConfig {
				storage: MultiCommitBufferTier::memory(),
			}),
			persistent: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			spawner,
			clock,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(persistent: PersistentConfig, spawner: ActorSpawner, clock: Clock, event_bus: EventBus) -> Self {
		Self {
			commit: Some(CommitBufferConfig {
				storage: MultiCommitBufferTier::memory(),
			}),
			persistent: Some(persistent),
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			spawner,
			clock,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_unbuffered(
		persistent: PersistentConfig,
		spawner: ActorSpawner,
		clock: Clock,
		event_bus: EventBus,
	) -> Self {
		Self {
			commit: None,
			persistent: Some(persistent),
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			spawner,
			clock,
		}
	}
}

#[derive(Clone)]
pub struct CommitBufferConfig {
	pub storage: MultiCommitBufferTier,
}

#[derive(Clone)]
pub struct PersistentConfig {
	pub storage: MultiPersistentTier,
	pub flush_interval: Duration,
}

impl PersistentConfig {
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(sqlite_config: SqliteConfig) -> Self {
		Self {
			storage: MultiPersistentTier::sqlite(sqlite_config),
			flush_interval: Duration::from_seconds(5).unwrap(),
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_in_memory() -> (Self, SqliteTempPathGuard) {
		let (storage, guard) = MultiPersistentTier::sqlite_in_memory();
		(
			Self {
				storage,
				flush_interval: Duration::from_seconds(5).unwrap(),
			},
			guard,
		)
	}

	pub fn flush_interval(mut self, interval: Duration) -> Self {
		self.flush_interval = interval;
		self
	}
}

#[derive(Clone, Debug)]
pub struct RetentionConfig {
	pub buffer: Duration,
	pub persistent: Duration,
}

#[derive(Clone, Debug)]
pub struct MergeConfig {
	pub merge_threshold_rows: usize,
	pub merge_batch_size: usize,
	pub enable_auto_eviction: bool,
}

impl Default for RetentionConfig {
	fn default() -> Self {
		Self {
			buffer: Duration::from_seconds(300).unwrap(),
			persistent: Duration::from_seconds(3600).unwrap(),
		}
	}
}

impl Default for MergeConfig {
	fn default() -> Self {
		Self {
			merge_threshold_rows: 100_000,
			merge_batch_size: 10_000,
			enable_auto_eviction: true,
		}
	}
}
