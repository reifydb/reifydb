// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator_state::GroupId,
	metrics::collect::MetricsCollector,
};
use reifydb_runtime::shutdown::Shutdown;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_value::{
	byte_size::ByteSize,
	value::{datetime::DateTime, row_number::RowNumber},
};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::sqlite::SqliteOperatorStorage;
use crate::{
	commit::batch::FlushBatch,
	filter::OperatorKeyFilter,
	types::{OperatorBatch, OperatorSealAnchor, OperatorSealAnchorCensus, OperatorStateCensus},
};

#[derive(Clone)]
#[cfg_attr(all(feature = "sqlite", not(target_arch = "wasm32")), repr(u8))]
pub enum OperatorPersistentTier {
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	Sqlite(SqliteOperatorStorage) = 0,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl OperatorPersistentTier {
	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::Sqlite(SqliteOperatorStorage::new(config))
	}

	pub fn sqlite_in_memory() -> (Self, SqliteTempPathGuard) {
		let (storage, guard) = SqliteOperatorStorage::in_memory();
		(Self::Sqlite(storage), guard)
	}

	pub fn sqlite_storage(&self) -> &SqliteOperatorStorage {
		match self {
			Self::Sqlite(storage) => storage,
		}
	}

	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedPodRow> {
		match self {
			Self::Sqlite(storage) => storage.get(operator, key),
		}
	}

	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		match self {
			Self::Sqlite(storage) => storage.contains(operator, key),
		}
	}

	pub fn filter(&self) -> &OperatorKeyFilter {
		match self {
			Self::Sqlite(storage) => storage.filter(),
		}
	}

	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		match self {
			Self::Sqlite(storage) => storage.range_batch(operator, range, batch_size),
		}
	}

	pub fn checkpoint_get(&self, flow: FlowId) -> Option<CommitVersion> {
		match self {
			Self::Sqlite(storage) => storage.checkpoint_get(flow),
		}
	}

	pub fn checkpoint_floor(&self) -> Option<CommitVersion> {
		match self {
			Self::Sqlite(storage) => storage.checkpoint_floor(),
		}
	}

	pub fn checkpoint_list(&self) -> Vec<FlowId> {
		match self {
			Self::Sqlite(storage) => storage.checkpoint_list(),
		}
	}

	pub fn bytes(&self, operator: OperatorId) -> ByteSize {
		match self {
			Self::Sqlite(storage) => storage.bytes(operator),
		}
	}

	pub fn total_bytes(&self) -> ByteSize {
		match self {
			Self::Sqlite(storage) => storage.total_bytes(),
		}
	}

	pub fn census(&self) -> Vec<OperatorStateCensus> {
		match self {
			Self::Sqlite(storage) => storage.census(),
		}
	}

	pub fn anchor_census(&self) -> Vec<OperatorSealAnchorCensus> {
		match self {
			Self::Sqlite(storage) => storage.anchor_census(),
		}
	}

	pub fn anchor_get(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Option<DateTime> {
		match self {
			Self::Sqlite(storage) => storage.anchor_get(operator, group, side, row_number),
		}
	}

	pub fn anchors_by_expiry(&self, operator: OperatorId, group: GroupId, limit: u64) -> Vec<OperatorSealAnchor> {
		match self {
			Self::Sqlite(storage) => storage.anchors_by_expiry(operator, group, limit),
		}
	}

	pub fn anchors_due(
		&self,
		operator: OperatorId,
		group: GroupId,
		at: DateTime,
		limit: u64,
	) -> Vec<OperatorSealAnchor> {
		match self {
			Self::Sqlite(storage) => storage.anchors_due(operator, group, at, limit),
		}
	}

	pub fn flush_batch(&self, batch: &FlushBatch) {
		match self {
			Self::Sqlite(storage) => storage.flush_batch(batch),
		}
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		match self {
			Self::Sqlite(storage) => storage.metrics_collectors(),
		}
	}
}

#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
impl OperatorPersistentTier {
	pub fn get(&self, _operator: OperatorId, _key: &EncodedKey) -> Option<EncodedPodRow> {
		match *self {}
	}

	pub fn contains(&self, _operator: OperatorId, _key: &EncodedKey) -> bool {
		match *self {}
	}

	pub fn filter(&self) -> &OperatorKeyFilter {
		match *self {}
	}

	pub fn range_batch(&self, _operator: OperatorId, _range: EncodedKeyRange, _batch_size: u64) -> OperatorBatch {
		match *self {}
	}

	pub fn checkpoint_get(&self, _flow: FlowId) -> Option<CommitVersion> {
		match *self {}
	}

	pub fn checkpoint_floor(&self) -> Option<CommitVersion> {
		match *self {}
	}

	pub fn checkpoint_list(&self) -> Vec<FlowId> {
		match *self {}
	}

	pub fn bytes(&self, _operator: OperatorId) -> ByteSize {
		match *self {}
	}

	pub fn total_bytes(&self) -> ByteSize {
		match *self {}
	}

	pub fn census(&self) -> Vec<OperatorStateCensus> {
		match *self {}
	}

	pub fn anchor_census(&self) -> Vec<OperatorSealAnchorCensus> {
		match *self {}
	}

	pub fn anchor_get(
		&self,
		_operator: OperatorId,
		_group: GroupId,
		_side: u8,
		_row_number: RowNumber,
	) -> Option<DateTime> {
		match *self {}
	}

	pub fn anchors_by_expiry(
		&self,
		_operator: OperatorId,
		_group: GroupId,
		_limit: u64,
	) -> Vec<OperatorSealAnchor> {
		match *self {}
	}

	pub fn anchors_due(
		&self,
		_operator: OperatorId,
		_group: GroupId,
		_at: DateTime,
		_limit: u64,
	) -> Vec<OperatorSealAnchor> {
		match *self {}
	}

	pub fn flush_batch(&self, _batch: &FlushBatch) {
		match *self {}
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		match *self {}
	}
}

impl Shutdown for OperatorPersistentTier {
	fn shutdown(&self) {
		match self {
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.shutdown(),
			#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
			_ => {}
		}
	}
}
