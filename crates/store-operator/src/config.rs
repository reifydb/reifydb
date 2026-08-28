// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::{actor::system::ActorSpawner, context::clock::Clock};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};

use crate::tier::{
	commit::OperatorCommitBuffer, persistent::OperatorPersistentTier, point::OperatorPointConfig,
	range::OperatorRangeConfig,
};

#[derive(Debug, Clone, Default)]
pub struct OperatorCommitConfig {
	pub storage: OperatorCommitBuffer,
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
	pub commit: OperatorCommitConfig,
	pub persistent: Option<OperatorPersistentConfig>,
	pub point: Option<OperatorPointConfig>,
	pub range: Option<OperatorRangeConfig>,
	pub spawner: ActorSpawner,
	pub clock: Clock,
}

impl OperatorStoreConfig {
	pub fn memory(spawner: ActorSpawner, clock: Clock) -> Self {
		Self {
			commit: OperatorCommitConfig::default(),
			persistent: None,
			point: None,
			range: None,
			spawner,
			clock,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(persistent: OperatorPersistentConfig, spawner: ActorSpawner, clock: Clock) -> Self {
		Self {
			commit: OperatorCommitConfig::default(),
			persistent: Some(persistent),
			point: None,
			range: None,
			spawner,
			clock,
		}
	}
}
