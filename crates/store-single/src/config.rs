// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteConfig;

use crate::{buffer::tier::SingleBufferTier, persistent::SinglePersistentTier};

#[derive(Clone)]
pub struct SingleStoreConfig {
	pub buffer: Option<BufferConfig>,
	pub persistent: Option<PersistentConfig>,
	pub actor_system: ActorSystem,
	pub clock: Clock,
}

impl SingleStoreConfig {
	pub fn memory(actor_system: ActorSystem, clock: Clock) -> Self {
		Self {
			buffer: Some(BufferConfig {
				storage: SingleBufferTier::memory(),
			}),
			persistent: None,
			actor_system,
			clock,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(persistent: PersistentConfig, actor_system: ActorSystem, clock: Clock) -> Self {
		Self {
			buffer: Some(BufferConfig {
				storage: SingleBufferTier::memory(),
			}),
			persistent: Some(persistent),
			actor_system,
			clock,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_unbuffered(persistent: PersistentConfig, actor_system: ActorSystem, clock: Clock) -> Self {
		Self {
			buffer: None,
			persistent: Some(persistent),
			actor_system,
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
			flush_interval: Duration::from_secs(5),
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_in_memory() -> Self {
		Self {
			storage: SinglePersistentTier::sqlite_in_memory(),
			flush_interval: Duration::from_secs(5),
		}
	}

	pub fn flush_interval(mut self, interval: Duration) -> Self {
		self.flush_interval = interval;
		self
	}
}
