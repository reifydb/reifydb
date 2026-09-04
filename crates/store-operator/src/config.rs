// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::{actor::system::ActorSpawner, context::clock::Clock};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_value::value::duration::Duration;

use crate::tier::{
	persistent::OperatorPersistentTier,
	range::OperatorRangeConfig,
	resident::{FLUSH_INTERVAL, OperatorResidentState},
};

#[derive(Debug, Clone)]
pub struct OperatorResidentStateConfig {
	pub storage: OperatorResidentState,
	pub flush_interval: Duration,
}

impl Default for OperatorResidentStateConfig {
	fn default() -> Self {
		Self {
			storage: OperatorResidentState::default(),
			flush_interval: FLUSH_INTERVAL,
		}
	}
}

#[derive(Clone)]
pub struct OperatorPersistentConfig {
	pub storage: OperatorPersistentTier,
}

impl OperatorPersistentConfig {
	pub fn opened(storage: OperatorPersistentTier) -> Self {
		Self {
			storage,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::opened(OperatorPersistentTier::sqlite(config))
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_in_memory() -> (Self, SqliteTempPathGuard) {
		let (storage, guard) = OperatorPersistentTier::sqlite_in_memory();
		(Self::opened(storage), guard)
	}
}

#[derive(Clone)]
pub struct OperatorStoreConfig {
	pub resident: OperatorResidentStateConfig,
	pub persistent: Option<OperatorPersistentConfig>,
	pub range: Option<OperatorRangeConfig>,
	pub spawner: ActorSpawner,
	pub clock: Clock,
}

impl OperatorStoreConfig {
	pub fn memory(spawner: ActorSpawner, clock: Clock) -> Self {
		Self {
			resident: OperatorResidentStateConfig::default(),
			persistent: None,
			range: None,
			spawner,
			clock,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(persistent: OperatorPersistentConfig, spawner: ActorSpawner, clock: Clock) -> Self {
		Self {
			resident: OperatorResidentStateConfig::default(),
			persistent: Some(persistent),
			range: None,
			spawner,
			clock,
		}
	}
}
