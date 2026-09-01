// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod filter;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod sqlite;

use std::{collections::HashMap, sync::Arc};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::GroupId,
	metrics::collect::MetricsCollector,
};
use reifydb_runtime::shutdown::Shutdown;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store::{filter::KeyFilter, metrics::PageCacheMetrics};
use reifydb_value::{
	byte_size::ByteSize,
	value::{datetime::DateTime, row_number::RowNumber},
};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::tier::persistent::sqlite::SqliteOperatorStorage;
use crate::{
	tier::{persistent::filter::JoinExpiryKeys, resident::batch::FlushBatch},
	types::{OperatorBatch, OperatorStateCensus, StoredJoinRowExpiry, StoredJoinRowExpiryCensus},
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

	pub fn get_many(&self, operator: OperatorId, keys: &[EncodedKey]) -> HashMap<EncodedKey, EncodedPodRow> {
		match self {
			Self::Sqlite(storage) => storage.get_many(operator, keys),
		}
	}

	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		match self {
			Self::Sqlite(storage) => storage.contains(operator, key),
		}
	}

	pub fn join_expiry_filter(&self) -> &KeyFilter<JoinExpiryKeys> {
		match self {
			Self::Sqlite(storage) => storage.join_expiry_filter(),
		}
	}

	pub fn join_expiries_out_of_band(&self) -> bool {
		match self {
			Self::Sqlite(storage) => storage.join_expiries_out_of_band(),
		}
	}

	pub fn state_sizes(&self, operator: OperatorId, keys: &[EncodedKey]) -> HashMap<EncodedKey, ByteSize> {
		match self {
			Self::Sqlite(storage) => storage.state_sizes(operator, keys),
		}
	}

	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		match self {
			Self::Sqlite(storage) => storage.range_batch(operator, range, batch_size),
		}
	}

	pub fn last_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		match self {
			Self::Sqlite(storage) => storage.last_batch(operator, range, batch_size),
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

	pub fn join_expiry_census(&self) -> Vec<StoredJoinRowExpiryCensus> {
		match self {
			Self::Sqlite(storage) => storage.join_expiry_census(),
		}
	}

	pub fn join_expiry_get(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Option<DateTime> {
		match self {
			Self::Sqlite(storage) => storage.join_expiry_get(operator, group, side, row_number),
		}
	}

	pub fn join_expiries_by_time(
		&self,
		operator: OperatorId,
		group: GroupId,
		limit: u64,
	) -> Vec<StoredJoinRowExpiry> {
		match self {
			Self::Sqlite(storage) => storage.join_expiries_by_time(operator, group, limit),
		}
	}

	pub fn join_expiries_due(
		&self,
		operator: OperatorId,
		group: GroupId,
		at: DateTime,
		limit: u64,
	) -> Vec<StoredJoinRowExpiry> {
		match self {
			Self::Sqlite(storage) => storage.join_expiries_due(operator, group, at, limit),
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

	pub fn page_cache_metrics(&self) -> PageCacheMetrics {
		match self {
			Self::Sqlite(storage) => storage.page_cache_metrics(),
		}
	}

	pub fn set_checkpoint_threshold(&self, frames: u32) {
		match self {
			Self::Sqlite(storage) => storage.set_checkpoint_threshold(frames),
		}
	}
}

#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
impl OperatorPersistentTier {
	pub fn get(&self, _operator: OperatorId, _key: &EncodedKey) -> Option<EncodedPodRow> {
		match *self {}
	}

	pub fn get_many(&self, _operator: OperatorId, _keys: &[EncodedKey]) -> HashMap<EncodedKey, EncodedPodRow> {
		match *self {}
	}

	pub fn contains(&self, _operator: OperatorId, _key: &EncodedKey) -> bool {
		match *self {}
	}

	pub fn join_expiry_filter(&self) -> &KeyFilter<JoinExpiryKeys> {
		match *self {}
	}

	pub fn join_expiries_out_of_band(&self) -> bool {
		match *self {}
	}

	pub fn state_sizes(&self, _operator: OperatorId, _keys: &[EncodedKey]) -> HashMap<EncodedKey, ByteSize> {
		match *self {}
	}

	pub fn range_batch(&self, _operator: OperatorId, _range: EncodedKeyRange, _batch_size: u64) -> OperatorBatch {
		match *self {}
	}

	pub fn last_batch(&self, _operator: OperatorId, _range: EncodedKeyRange, _batch_size: u64) -> OperatorBatch {
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

	pub fn join_expiry_census(&self) -> Vec<StoredJoinRowExpiryCensus> {
		match *self {}
	}

	pub fn join_expiry_get(
		&self,
		_operator: OperatorId,
		_group: GroupId,
		_side: u8,
		_row_number: RowNumber,
	) -> Option<DateTime> {
		match *self {}
	}

	pub fn join_expiries_by_time(
		&self,
		_operator: OperatorId,
		_group: GroupId,
		_limit: u64,
	) -> Vec<StoredJoinRowExpiry> {
		match *self {}
	}

	pub fn join_expiries_due(
		&self,
		_operator: OperatorId,
		_group: GroupId,
		_at: DateTime,
		_limit: u64,
	) -> Vec<StoredJoinRowExpiry> {
		match *self {}
	}

	pub fn flush_batch(&self, _batch: &FlushBatch) {
		match *self {}
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		match *self {}
	}

	pub fn page_cache_metrics(&self) -> PageCacheMetrics {
		match *self {}
	}

	pub fn set_checkpoint_threshold(&self, _frames: u32) {
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
