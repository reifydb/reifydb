// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::EncodedOperatorRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId, key::operator_state::GroupId, metrics::collect::MetricsCollector,
};
use reifydb_runtime::shutdown::Shutdown;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::{
	buffer::{memory::MemoryOperatorStorage, tier::OperatorBufferTier},
	config::OperatorStoreConfig,
	types::{OperatorBatch, OperatorSealAnchor, OperatorSealAnchorCensus, OperatorStateCensus, OperatorWrite},
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::{
	config::OperatorPersistentConfig,
	persistent::{OperatorPersistentTier, sqlite::storage::SqliteOperatorStorage},
};

#[derive(Clone)]
enum OperatorTier {
	Memory(MemoryOperatorStorage),
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	Sqlite(SqliteOperatorStorage),
}

#[derive(Clone)]
pub struct OperatorStore {
	tier: Arc<OperatorTier>,
}

impl OperatorStore {
	pub fn standard(config: OperatorStoreConfig) -> Self {
		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		if let Some(persistent) = config.persistent {
			let OperatorPersistentTier::Sqlite(storage) = persistent.storage;
			return Self {
				tier: Arc::new(OperatorTier::Sqlite(storage)),
			};
		}

		let buffer = config.buffer.expect("an operator store needs either a buffer or a persistent tier");
		let OperatorBufferTier::Memory(storage) = buffer.storage;
		Self {
			tier: Arc::new(OperatorTier::Memory(storage)),
		}
	}

	pub fn testing_memory() -> Self {
		Self::standard(OperatorStoreConfig::memory())
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite() -> (Self, SqliteTempPathGuard) {
		let (persistent, guard) = OperatorPersistentConfig::sqlite_in_memory();
		(
			Self::standard(OperatorStoreConfig {
				buffer: None,
				persistent: Some(persistent),
			}),
			guard,
		)
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::standard(OperatorStoreConfig {
			buffer: None,
			persistent: Some(OperatorPersistentConfig::sqlite(config)),
		})
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		match self.tier.as_ref() {
			OperatorTier::Memory(_) => Vec::new(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.metrics_collectors(),
		}
	}

	pub fn set(&self, operator: OperatorId, key: EncodedKey, row: EncodedOperatorRow) {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.set(operator, key, row),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.set(operator, key, row),
		}
	}

	pub fn remove(&self, operator: OperatorId, key: &EncodedKey) {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.remove(operator, key),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.remove(operator, key),
		}
	}

	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.apply_batch(writes),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.apply_batch(writes),
		}
	}

	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedOperatorRow> {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.get(operator, key),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.get(operator, key),
		}
	}

	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.contains(operator, key),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.contains(operator, key),
		}
	}

	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.range_batch(operator, range, batch_size),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.range_batch(operator, range, batch_size),
		}
	}

	pub fn bytes(&self, operator: OperatorId) -> u64 {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.bytes(operator),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.bytes(operator),
		}
	}

	pub fn total_bytes(&self) -> u64 {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.total_bytes(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.total_bytes(),
		}
	}

	pub fn census(&self) -> Vec<OperatorStateCensus> {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.census(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.census(),
		}
	}

	pub fn anchor_get(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Option<DateTime> {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.anchor_get(operator, group, side, row_number),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.anchor_get(operator, group, side, row_number),
		}
	}

	pub fn anchors_by_expiry(&self, operator: OperatorId, group: GroupId, limit: u64) -> Vec<OperatorSealAnchor> {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.anchors_by_expiry(operator, group, limit),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.anchors_by_expiry(operator, group, limit),
		}
	}

	pub fn anchors_due(
		&self,
		operator: OperatorId,
		group: GroupId,
		at: DateTime,
		limit: u64,
	) -> Vec<OperatorSealAnchor> {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.anchors_due(operator, group, at, limit),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.anchors_due(operator, group, at, limit),
		}
	}

	pub fn anchor_census(&self) -> Vec<OperatorSealAnchorCensus> {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.anchor_census(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.anchor_census(),
		}
	}

	pub fn anchor_set(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
		expiry: DateTime,
	) {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.anchor_set(operator, group, side, row_number, expiry),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.anchor_set(operator, group, side, row_number, expiry),
		}
	}

	pub fn anchor_remove(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.anchor_remove(operator, group, side, row_number),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.anchor_remove(operator, group, side, row_number),
		}
	}

	pub fn anchors_remove_group(&self, operator: OperatorId, group: GroupId) {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.anchors_remove_group(operator, group),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.anchors_remove_group(operator, group),
		}
	}

	pub fn anchors_drop_operator(&self, operator: OperatorId) {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.anchors_drop_operator(operator),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.anchors_drop_operator(operator),
		}
	}

	pub fn drop_operator_state(&self, operator: OperatorId) {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.drop_operator_state(operator),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.drop_operator_state(operator),
		}
	}
}

impl Shutdown for OperatorStore {
	fn shutdown(&self) {
		match self.tier.as_ref() {
			OperatorTier::Memory(storage) => storage.shutdown(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			OperatorTier::Sqlite(storage) => storage.shutdown(),
		}
	}
}
