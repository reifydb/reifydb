// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_core::event::EventBus;
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteConfig;

use crate::{buffer::tier::BufferTier, persistent::PersistentTier};

#[derive(Clone)]
pub struct SingleStoreConfig {
	pub buffer: Option<BufferConfig>,
	pub persistent: Option<PersistentConfig>,
	pub event_bus: EventBus,
	pub actor_system: ActorSystem,
	pub clock: Clock,
}

#[derive(Clone)]
pub struct BufferConfig {
	pub storage: BufferTier,
}

#[derive(Clone)]
pub struct PersistentConfig {
	pub storage: PersistentTier,
	pub flush_interval: Duration,
}

impl PersistentConfig {
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(sqlite_config: SqliteConfig) -> Self {
		Self {
			storage: PersistentTier::sqlite(sqlite_config),
			flush_interval: Duration::from_secs(5),
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_in_memory() -> Self {
		Self {
			storage: PersistentTier::sqlite_in_memory(),
			flush_interval: Duration::from_secs(5),
		}
	}

	pub fn flush_interval(mut self, interval: Duration) -> Self {
		self.flush_interval = interval;
		self
	}
}
