// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::actor::system::ActorSpawner;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_value::value::duration::Duration;

use crate::{
	persistent::PersistentTier,
	range::OperatorRangeConfig,
	resident::{FLUSH_INTERVAL, Resident},
};

#[derive(Debug, Clone)]
pub struct ResidentConfig {
	pub storage: Resident,
	pub flush_interval: Duration,
}

impl Default for ResidentConfig {
	fn default() -> Self {
		Self {
			storage: Resident::default(),
			flush_interval: FLUSH_INTERVAL,
		}
	}
}

#[derive(Clone)]
pub struct OperatorPersistentConfig {
	pub storage: PersistentTier,
}

impl OperatorPersistentConfig {
	pub fn opened(storage: PersistentTier) -> Self {
		Self {
			storage,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_in_memory() -> (Self, SqliteTempPathGuard) {
		let (storage, guard) = PersistentTier::sqlite_in_memory();
		(Self::opened(storage), guard)
	}
}

#[derive(Clone)]
pub struct OperatorStoreConfig {
	pub resident: ResidentConfig,
	pub persistent: Option<OperatorPersistentConfig>,
	pub range: Option<OperatorRangeConfig>,
	pub spawner: ActorSpawner,
}

impl OperatorStoreConfig {
	pub fn memory(spawner: ActorSpawner) -> Self {
		Self {
			resident: ResidentConfig::default(),
			persistent: None,
			range: None,
			spawner,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(persistent: OperatorPersistentConfig, spawner: ActorSpawner) -> Self {
		Self {
			resident: ResidentConfig::default(),
			persistent: Some(persistent),
			range: None,
			spawner,
		}
	}
}
