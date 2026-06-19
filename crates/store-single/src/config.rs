// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::{actor::system::ActorSpawner, context::clock::Clock};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_value::value::duration::Duration;

use crate::{buffer::tier::SingleBufferTier, persistent::SinglePersistentTier};

#[derive(Clone)]
pub struct SingleStoreConfig {
	pub buffer: Option<BufferConfig>,
	pub persistent: Option<PersistentConfig>,
	pub spawner: ActorSpawner,
	pub clock: Clock,
}

impl SingleStoreConfig {
	pub fn memory(spawner: ActorSpawner, clock: Clock) -> Self {
		Self {
			buffer: Some(BufferConfig {
				storage: SingleBufferTier::memory(),
			}),
			persistent: None,
			spawner,
			clock,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(persistent: PersistentConfig, spawner: ActorSpawner, clock: Clock) -> Self {
		Self {
			buffer: Some(BufferConfig {
				storage: SingleBufferTier::memory(),
			}),
			persistent: Some(persistent),
			spawner,
			clock,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_unbuffered(persistent: PersistentConfig, spawner: ActorSpawner, clock: Clock) -> Self {
		Self {
			buffer: None,
			persistent: Some(persistent),
			spawner,
			clock,
		}
	}
}

#[derive(Clone)]
pub struct BufferConfig {
	pub storage: SingleBufferTier,
}

#[derive(Clone)]
pub struct PersistentConfig {
	pub storage: SinglePersistentTier,
	pub flush_interval: Duration,
}

impl PersistentConfig {
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(sqlite_config: SqliteConfig) -> Self {
		Self {
			storage: SinglePersistentTier::sqlite(sqlite_config),
			flush_interval: Duration::from_seconds(5).unwrap(),
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_in_memory() -> (Self, SqliteTempPathGuard) {
		let (storage, guard) = SinglePersistentTier::sqlite_in_memory();
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
