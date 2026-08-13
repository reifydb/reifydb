// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::buffer::tier::OperatorBufferTier;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::persistent::OperatorPersistentTier;

#[derive(Clone)]
pub struct OperatorBufferConfig {
	pub storage: OperatorBufferTier,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
#[derive(Clone)]
pub struct OperatorPersistentConfig {
	pub storage: OperatorPersistentTier,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl OperatorPersistentConfig {
	pub fn opened(storage: OperatorPersistentTier) -> Self {
		Self {
			storage,
		}
	}

	pub fn sqlite(config: reifydb_sqlite::SqliteConfig) -> Self {
		Self {
			storage: OperatorPersistentTier::sqlite(config),
		}
	}

	pub fn sqlite_in_memory() -> (Self, reifydb_sqlite::SqliteTempPathGuard) {
		let (storage, guard) = OperatorPersistentTier::sqlite_in_memory();
		(
			Self {
				storage,
			},
			guard,
		)
	}
}

#[derive(Clone)]
pub struct OperatorStoreConfig {
	pub buffer: Option<OperatorBufferConfig>,
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub persistent: Option<OperatorPersistentConfig>,
}

impl OperatorStoreConfig {
	pub fn memory() -> Self {
		Self {
			buffer: Some(OperatorBufferConfig {
				storage: OperatorBufferTier::memory(),
			}),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			persistent: None,
		}
	}
}
