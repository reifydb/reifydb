// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_core::event::EventBus;
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteConfig;

use crate::{hot::storage::HotStorage, warm::WarmStorage};

#[derive(Clone)]
pub struct MultiStoreConfig {
	pub hot: Option<HotConfig>,
	pub warm: Option<WarmConfig>,
	pub cold: Option<ColdConfig>,
	pub retention: RetentionConfig,
	pub merge_config: MergeConfig,
	pub event_bus: EventBus,
	pub actor_system: ActorSystem,
	pub clock: Clock,
}

#[derive(Clone)]
pub struct HotConfig {
	pub storage: HotStorage,
}

#[derive(Clone)]
pub struct WarmConfig {
	pub storage: WarmStorage,
	pub flush_interval: Duration,
}

impl WarmConfig {
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(sqlite_config: SqliteConfig) -> Self {
		Self {
			storage: WarmStorage::sqlite(sqlite_config),
			flush_interval: Duration::from_secs(5),
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_in_memory() -> Self {
		Self {
			storage: WarmStorage::sqlite_in_memory(),
			flush_interval: Duration::from_secs(5),
		}
	}

	pub fn flush_interval(mut self, interval: Duration) -> Self {
		self.flush_interval = interval;
		self
	}
}

#[derive(Clone, Default)]
pub struct ColdConfig;

#[derive(Clone, Debug)]
pub struct RetentionConfig {
	pub hot: Duration,
	pub warm: Duration,
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
			hot: Duration::from_secs(300),
			warm: Duration::from_secs(3600),
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
