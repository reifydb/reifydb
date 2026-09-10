// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod filter;
pub mod memory;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod sqlite;
pub mod testing;

use std::{collections::HashMap, sync::Arc};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId},
	metrics::collect::MetricsCollector,
};
use reifydb_runtime::shutdown::Shutdown;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store::metrics::PageCacheMetrics;
use reifydb_value::byte_size::ByteSize;
use tracing::warn;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::persistent::sqlite::SqlitePersistent;
use crate::{
	error::Result,
	persistent::{
		memory::MemoryPersistent,
		testing::{PersistentHooks, TestingPersistent},
	},
	types::{Applied, DropMarker, FlushBatch, OperatorBatch, OperatorStateCensus, StagedWrite},
};

#[derive(Clone)]
pub enum PersistentTier {
	Absent,
	Memory(MemoryPersistent),
	Testing(TestingPersistent),
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	Sqlite(SqlitePersistent),
}

impl PersistentTier {
	pub fn memory() -> Self {
		Self::Memory(MemoryPersistent::new())
	}

	pub fn testing(hooks: Arc<dyn PersistentHooks>) -> Self {
		Self::Testing(TestingPersistent::new(hooks))
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::Sqlite(SqlitePersistent::new(config))
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_in_memory() -> (Self, SqliteTempPathGuard) {
		let (storage, guard) = SqlitePersistent::in_memory();
		(Self::Sqlite(storage), guard)
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_storage(&self) -> Option<&SqlitePersistent> {
		match self {
			Self::Sqlite(storage) => Some(storage),
			_ => None,
		}
	}

	pub fn total_bytes(&self) -> ByteSize {
		match self {
			Self::Absent => ByteSize::ZERO,
			Self::Memory(memory) => total_of(memory),
			Self::Testing(testing) => total_of(testing),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.total_bytes(),
		}
	}

	pub fn occupied_keyspaces(&self, operator: OperatorId) -> Vec<KeyspaceId> {
		match self {
			Self::Absent => Vec::new(),
			Self::Memory(memory) => occupied_of(memory, operator),
			Self::Testing(testing) => occupied_of(testing, operator),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.occupied_keyspaces(operator),
		}
	}

	pub fn flush_batch(&self, batch: &FlushBatch) {
		match self {
			Self::Absent => {}
			Self::Memory(memory) => flush_of(memory, batch),
			Self::Testing(testing) => flush_of(testing, batch),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.flush_batch(batch),
		}
	}

	pub fn page_cache_metrics(&self) -> Option<PageCacheMetrics> {
		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		if let Self::Sqlite(storage) = self {
			return Some(storage.page_cache_metrics());
		}
		None
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn state_keys_after(
		&self,
		operator: OperatorId,
		keyspace: KeyspaceId,
		after: Option<&EncodedKey>,
		limit: u64,
	) -> Vec<EncodedKey> {
		match self {
			Self::Sqlite(storage) => storage.state_keys_after(operator, keyspace, after, limit),
			_ => Vec::new(),
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn set_checkpoint_threshold(&self, frames: u32) {
		if let Self::Sqlite(storage) = self {
			storage.set_checkpoint_threshold(frames);
		}
	}

	#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
	pub fn set_checkpoint_threshold(&self, _frames: u32) {}
}

fn total_of(persistent: &impl Enumerate) -> ByteSize {
	match persistent.census() {
		Ok(entries) => ByteSize::from_bytes(
			entries.iter().map(|entry| entry.key_bytes.as_bytes() + entry.value_bytes.as_bytes()).sum(),
		),
		Err(error) => {
			warn!(error = %error, "operator census failed; reporting zero total bytes");
			ByteSize::ZERO
		}
	}
}

fn occupied_of(persistent: &impl Enumerate, operator: OperatorId) -> Vec<KeyspaceId> {
	match persistent.keyspaces(operator) {
		Ok(keyspaces) => keyspaces,
		Err(error) => {
			warn!(error = %error, "operator keyspace enumeration failed; treating none as occupied");
			Vec::new()
		}
	}
}

fn flush_of(persistent: &impl Apply, batch: &FlushBatch) {
	if let Err(error) = persistent.apply(batch) {
		warn!(error = %error, "operator flush batch failed; buffered rows were not persisted");
	}
}

impl Shutdown for PersistentTier {
	fn shutdown(&self) {
		match self {
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.shutdown(),
			_ => {}
		}
	}
}

pub trait Persistent: Clone + Send + Sync + 'static {
	fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>>;

	fn is_absent(&self) -> bool {
		false
	}
}

pub trait Fetch: Persistent {
	fn get(&self, operator: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedPodRow>>;

	fn get_many(
		&self,
		operator: OperatorId,
		keys: &[GroupStateKey],
	) -> Result<HashMap<GroupStateKey, EncodedPodRow>>;

	fn contains(&self, operator: OperatorId, key: &GroupStateKey) -> Result<bool>;
}

pub trait Page: Persistent {
	fn range_batch(
		&self,
		operator: OperatorId,
		range: EncodedKeyRange,
		batch: u64,
		occupied: u64,
	) -> Result<OperatorBatch>;

	fn last_batch(
		&self,
		operator: OperatorId,
		range: EncodedKeyRange,
		batch: u64,
		occupied: u64,
	) -> Result<OperatorBatch>;

	fn group_page(&self, operator: OperatorId, groups: &[GroupId], batch: u64, mask: u64) -> Result<OperatorBatch>;
}

pub trait Measure: Persistent {
	fn state_sizes(&self, operator: OperatorId, keys: &[GroupStateKey])
	-> Result<HashMap<GroupStateKey, ByteSize>>;

	fn bytes(&self, operator: OperatorId) -> Result<ByteSize>;
}

pub trait Enumerate: Persistent {
	fn census(&self) -> Result<Vec<OperatorStateCensus>>;

	fn operators(&self) -> Result<Vec<OperatorId>>;

	fn keyspaces(&self, operator: OperatorId) -> Result<Vec<KeyspaceId>>;
}

pub trait Checkpoint: Persistent {
	fn checkpoint_get(&self, flow: FlowId) -> Result<Option<CommitVersion>>;

	fn checkpoint_set(&self, flow: FlowId, version: CommitVersion) -> Result<()>;

	fn checkpoint_remove(&self, flow: FlowId) -> Result<()>;

	fn checkpoint_floor(&self) -> Result<Option<CommitVersion>>;

	fn checkpoint_list(&self) -> Result<Vec<FlowId>>;
}

pub trait Apply: Persistent {
	fn apply(&self, batch: &FlushBatch) -> Result<Applied>;

	fn drop_operator(&self, operator: OperatorId) -> Result<()>;
}

impl Persistent for PersistentTier {
	fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		match self {
			Self::Absent => Vec::new(),
			Self::Memory(memory) => Persistent::metrics_collectors(memory),
			Self::Testing(testing) => Persistent::metrics_collectors(testing),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => storage.metrics_collectors(),
		}
	}

	fn is_absent(&self) -> bool {
		matches!(self, Self::Absent)
	}
}

impl Fetch for PersistentTier {
	fn get(&self, operator: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		match self {
			Self::Absent => Ok(None),
			Self::Memory(memory) => Fetch::get(memory, operator, key),
			Self::Testing(testing) => Fetch::get(testing, operator, key),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => Ok(storage.get(operator, key.as_encoded())),
		}
	}

	fn get_many(
		&self,
		operator: OperatorId,
		keys: &[GroupStateKey],
	) -> Result<HashMap<GroupStateKey, EncodedPodRow>> {
		match self {
			Self::Absent => Ok(HashMap::new()),
			Self::Memory(memory) => Fetch::get_many(memory, operator, keys),
			Self::Testing(testing) => Fetch::get_many(testing, operator, keys),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => {
				let encoded: Vec<EncodedKey> =
					keys.iter().map(|key| key.as_encoded().clone()).collect();
				Ok(storage
					.get_many(operator, &encoded)
					.into_iter()
					.map(|(key, row)| (GroupStateKey::bound_unchecked(key), row))
					.collect())
			}
		}
	}

	fn contains(&self, operator: OperatorId, key: &GroupStateKey) -> Result<bool> {
		match self {
			Self::Absent => Ok(false),
			Self::Memory(memory) => Fetch::contains(memory, operator, key),
			Self::Testing(testing) => Fetch::contains(testing, operator, key),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => Ok(storage.contains(operator, key.as_encoded())),
		}
	}
}

impl Page for PersistentTier {
	fn range_batch(
		&self,
		operator: OperatorId,
		range: EncodedKeyRange,
		batch: u64,
		occupied: u64,
	) -> Result<OperatorBatch> {
		match self {
			Self::Absent => Ok(OperatorBatch::empty()),
			Self::Memory(memory) => Page::range_batch(memory, operator, range, batch, occupied),
			Self::Testing(testing) => Page::range_batch(testing, operator, range, batch, occupied),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => Ok(storage.range_batch(operator, range, batch)),
		}
	}

	fn last_batch(
		&self,
		operator: OperatorId,
		range: EncodedKeyRange,
		batch: u64,
		occupied: u64,
	) -> Result<OperatorBatch> {
		match self {
			Self::Absent => Ok(OperatorBatch::empty()),
			Self::Memory(memory) => Page::last_batch(memory, operator, range, batch, occupied),
			Self::Testing(testing) => Page::last_batch(testing, operator, range, batch, occupied),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => Ok(storage.last_batch(operator, range, batch)),
		}
	}

	fn group_page(&self, operator: OperatorId, groups: &[GroupId], batch: u64, mask: u64) -> Result<OperatorBatch> {
		match self {
			Self::Absent => Ok(OperatorBatch::empty()),
			Self::Memory(memory) => Page::group_page(memory, operator, groups, batch, mask),
			Self::Testing(testing) => Page::group_page(testing, operator, groups, batch, mask),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => Ok(storage.group_page(operator, groups, batch, mask)),
		}
	}
}

impl Measure for PersistentTier {
	fn state_sizes(
		&self,
		operator: OperatorId,
		keys: &[GroupStateKey],
	) -> Result<HashMap<GroupStateKey, ByteSize>> {
		match self {
			Self::Absent => Ok(HashMap::new()),
			Self::Memory(memory) => Measure::state_sizes(memory, operator, keys),
			Self::Testing(testing) => Measure::state_sizes(testing, operator, keys),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => {
				let encoded: Vec<EncodedKey> =
					keys.iter().map(|key| key.as_encoded().clone()).collect();
				Ok(storage
					.state_sizes(operator, &encoded)
					.into_iter()
					.map(|(key, size)| (GroupStateKey::bound_unchecked(key), size))
					.collect())
			}
		}
	}

	fn bytes(&self, operator: OperatorId) -> Result<ByteSize> {
		match self {
			Self::Absent => Ok(ByteSize::ZERO),
			Self::Memory(memory) => Measure::bytes(memory, operator),
			Self::Testing(testing) => Measure::bytes(testing, operator),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => Ok(storage.bytes(operator)),
		}
	}
}

impl Enumerate for PersistentTier {
	fn census(&self) -> Result<Vec<OperatorStateCensus>> {
		match self {
			Self::Absent => Ok(Vec::new()),
			Self::Memory(memory) => Enumerate::census(memory),
			Self::Testing(testing) => Enumerate::census(testing),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => Ok(storage.census()),
		}
	}

	fn operators(&self) -> Result<Vec<OperatorId>> {
		match self {
			Self::Absent => Ok(Vec::new()),
			Self::Memory(memory) => Enumerate::operators(memory),
			Self::Testing(testing) => Enumerate::operators(testing),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => {
				let mut out: Vec<OperatorId> =
					storage.census().into_iter().map(|entry| entry.operator).collect();
				out.sort_unstable();
				out.dedup();
				Ok(out)
			}
		}
	}

	fn keyspaces(&self, operator: OperatorId) -> Result<Vec<KeyspaceId>> {
		match self {
			Self::Absent => Ok(Vec::new()),
			Self::Memory(memory) => Enumerate::keyspaces(memory, operator),
			Self::Testing(testing) => Enumerate::keyspaces(testing, operator),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => Ok(storage.occupied_keyspaces(operator)),
		}
	}
}

impl Checkpoint for PersistentTier {
	fn checkpoint_get(&self, flow: FlowId) -> Result<Option<CommitVersion>> {
		match self {
			Self::Absent => Ok(None),
			Self::Memory(memory) => Checkpoint::checkpoint_get(memory, flow),
			Self::Testing(testing) => Checkpoint::checkpoint_get(testing, flow),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => Ok(storage.checkpoint_get(flow)),
		}
	}

	fn checkpoint_set(&self, flow: FlowId, version: CommitVersion) -> Result<()> {
		match self {
			Self::Absent => Ok(()),
			Self::Memory(memory) => Checkpoint::checkpoint_set(memory, flow, version),
			Self::Testing(testing) => Checkpoint::checkpoint_set(testing, flow, version),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => {
				let mut batch = FlushBatch::default();
				batch.checkpoints.insert(flow, Some(version));
				storage.flush_batch(&batch);
				Ok(())
			}
		}
	}

	fn checkpoint_remove(&self, flow: FlowId) -> Result<()> {
		match self {
			Self::Absent => Ok(()),
			Self::Memory(memory) => Checkpoint::checkpoint_remove(memory, flow),
			Self::Testing(testing) => Checkpoint::checkpoint_remove(testing, flow),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => {
				let mut batch = FlushBatch::default();
				batch.checkpoints.insert(flow, None);
				storage.flush_batch(&batch);
				Ok(())
			}
		}
	}

	fn checkpoint_floor(&self) -> Result<Option<CommitVersion>> {
		match self {
			Self::Absent => Ok(None),
			Self::Memory(memory) => Checkpoint::checkpoint_floor(memory),
			Self::Testing(testing) => Checkpoint::checkpoint_floor(testing),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => Ok(storage.checkpoint_floor()),
		}
	}

	fn checkpoint_list(&self) -> Result<Vec<FlowId>> {
		match self {
			Self::Absent => Ok(Vec::new()),
			Self::Memory(memory) => Checkpoint::checkpoint_list(memory),
			Self::Testing(testing) => Checkpoint::checkpoint_list(testing),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => Ok(storage.checkpoint_list()),
		}
	}
}

fn written_bytes(batch: &FlushBatch) -> ByteSize {
	let total: u64 = batch
		.writes
		.iter()
		.filter_map(|(_, key, write)| match write {
			StagedWrite::Set(row) => Some(key.as_slice().len() as u64 + row.len() as u64),
			StagedWrite::Remove => None,
		})
		.sum();
	ByteSize::from_bytes(total)
}

impl Apply for PersistentTier {
	fn apply(&self, batch: &FlushBatch) -> Result<Applied> {
		match self {
			Self::Absent => Ok(Applied::default()),
			Self::Memory(memory) => Apply::apply(memory, batch),
			Self::Testing(testing) => Apply::apply(testing, batch),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => {
				let rows = batch.writes.len();
				let bytes = written_bytes(batch);
				storage.flush_batch(batch);
				Ok(Applied {
					rows,
					bytes,
				})
			}
		}
	}

	fn drop_operator(&self, operator: OperatorId) -> Result<()> {
		match self {
			Self::Absent => Ok(()),
			Self::Memory(memory) => Apply::drop_operator(memory, operator),
			Self::Testing(testing) => Apply::drop_operator(testing, operator),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(storage) => {
				let mut batch = FlushBatch::default();
				batch.drops.push(DropMarker::OperatorState(operator));
				storage.flush_batch(&batch);
				Ok(())
			}
		}
	}
}
