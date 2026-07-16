// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Cold tier of the multi-version store. Holds the durable, version-history-bearing record of every key the
//! buffer has flushed. The default backend is SQLite; the trait surface is generic so other backends can be
//! plugged in without touching the buffer or transaction layer.

use std::{collections::HashMap, ops::Bound};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::FlowNodeId, store::EntryKind},
};
use reifydb_runtime::shutdown::Shutdown;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_value::{Result, byte_size::ByteSize, util::cowvec::CowVec};

use crate::{
	MultiVersionScope,
	tier::{
		HistoricalCursor, RangeBatch, RangeCursor, RawEntry, TierBackend, TierBatch, TierStorage,
		VersionedGetResult,
	},
};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod sqlite;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use sqlite::storage::SqlitePersistentStorage;

#[derive(Clone)]
#[cfg_attr(all(feature = "sqlite", not(target_arch = "wasm32")), repr(u8))]
pub enum MultiPersistentTier {
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	Sqlite(SqlitePersistentStorage) = 0,
}

impl Shutdown for MultiPersistentTier {
	fn shutdown(&self) {
		match self {
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.shutdown(),
			#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
			_ => {}
		}
	}
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl MultiPersistentTier {
	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::Sqlite(SqlitePersistentStorage::new(config))
	}

	pub fn sqlite_in_memory() -> (Self, SqliteTempPathGuard) {
		let (storage, guard) = SqlitePersistentStorage::in_memory();
		(Self::Sqlite(storage), guard)
	}

	pub fn set_checkpoint_threshold(&self, frames: u32) {
		match self {
			Self::Sqlite(s) => s.set_checkpoint_threshold(frames),
		}
	}

	pub fn delete_below_version(
		&self,
		table: EntryKind,
		cutoff_version: CommitVersion,
		prefix: Option<&[u8]>,
	) -> Result<Vec<EncodedKey>> {
		match self {
			Self::Sqlite(s) => s.delete_below_version(table, cutoff_version, prefix),
		}
	}

	pub fn delete_keys(&self, table: EntryKind, keys: &[EncodedKey]) -> Result<u64> {
		match self {
			Self::Sqlite(s) => s.delete_keys(table, keys),
		}
	}

	pub fn set_collecting_accepted(&self, version: CommitVersion, batches: TierBatch) -> Result<Vec<EncodedKey>> {
		match self {
			Self::Sqlite(s) => s.set_collecting_accepted(version, batches),
		}
	}

	pub fn persist_sweep(&self, batches: Vec<(CommitVersion, TierBatch)>) -> Result<Vec<EncodedKey>> {
		match self {
			Self::Sqlite(s) => s.persist_sweep(batches),
		}
	}

	pub fn load_range_consistent(
		&self,
		table: EntryKind,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		read: CommitVersion,
		limit: Option<usize>,
	) -> Result<Vec<RawEntry>> {
		match self {
			Self::Sqlite(s) => s.load_range_consistent(table, start, end, read, limit),
		}
	}

	pub fn delete_keys_through(&self, table: EntryKind, keys: &[(EncodedKey, CommitVersion)]) -> Result<u64> {
		match self {
			Self::Sqlite(s) => s.delete_keys_through(table, keys),
		}
	}

	pub fn operator_disk_payload_bytes(&self) -> Result<Vec<(FlowNodeId, ByteSize)>> {
		match self {
			Self::Sqlite(s) => s.operator_disk_payload_bytes(),
		}
	}
}

#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
impl MultiPersistentTier {
	pub fn operator_disk_payload_bytes(&self) -> Result<Vec<(FlowNodeId, ByteSize)>> {
		match *self {}
	}

	pub fn set_checkpoint_threshold(&self, _frames: u32) {
		match *self {}
	}

	pub fn delete_below_version(
		&self,
		_table: EntryKind,
		_cutoff_version: CommitVersion,
		_prefix: Option<&[u8]>,
	) -> Result<Vec<EncodedKey>> {
		match *self {}
	}

	pub fn delete_keys(&self, _table: EntryKind, _keys: &[EncodedKey]) -> Result<u64> {
		match *self {}
	}

	pub fn persist_sweep(&self, _batches: Vec<(CommitVersion, TierBatch)>) -> Result<Vec<EncodedKey>> {
		match *self {}
	}

	pub fn load_range_consistent(
		&self,
		_table: EntryKind,
		_start: Bound<&[u8]>,
		_end: Bound<&[u8]>,
		_read: CommitVersion,
		_limit: Option<usize>,
	) -> Result<Vec<RawEntry>> {
		match *self {}
	}

	pub fn delete_keys_through(&self, _table: EntryKind, _keys: &[(EncodedKey, CommitVersion)]) -> Result<u64> {
		match *self {}
	}
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl TierStorage for MultiPersistentTier {
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult> {
		match self {
			Self::Sqlite(s) => s.get(table, key, version),
		}
	}

	fn get_many(
		&self,
		table: EntryKind,
		keys: &[&[u8]],
		version: CommitVersion,
	) -> Result<Vec<VersionedGetResult>> {
		match self {
			Self::Sqlite(s) => s.get_many(table, keys, version),
		}
	}

	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<()> {
		match self {
			Self::Sqlite(s) => s.set(version, batches),
		}
	}

	fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Sqlite(s) => s.range_next(table, cursor, start, end, scope, batch_size),
		}
	}

	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Sqlite(s) => s.range_rev_next(table, cursor, start, end, scope, batch_size),
		}
	}

	fn ensure_table(&self, table: EntryKind) -> Result<()> {
		match self {
			Self::Sqlite(s) => s.ensure_table(table),
		}
	}

	fn clear_table(&self, table: EntryKind) -> Result<()> {
		match self {
			Self::Sqlite(s) => s.clear_table(table),
		}
	}

	fn drop(&self, batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>>) -> Result<()> {
		match self {
			Self::Sqlite(s) => s.drop(batches),
		}
	}

	fn get_all_versions(&self, table: EntryKind, key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		match self {
			Self::Sqlite(s) => s.get_all_versions(table, key),
		}
	}

	fn scan_historical_below(
		&self,
		table: EntryKind,
		cutoff: CommitVersion,
		cursor: &mut HistoricalCursor,
		batch_size: usize,
	) -> Result<Vec<(EncodedKey, CommitVersion)>> {
		match self {
			Self::Sqlite(s) => s.scan_historical_below(table, cutoff, cursor, batch_size),
		}
	}
}

#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
impl TierStorage for MultiPersistentTier {
	fn get(&self, _table: EntryKind, _key: &[u8], _version: CommitVersion) -> Result<VersionedGetResult> {
		match *self {}
	}

	fn set(&self, _version: CommitVersion, _batches: TierBatch) -> Result<()> {
		match *self {}
	}

	fn range_next(
		&self,
		_table: EntryKind,
		_cursor: &mut RangeCursor,
		_start: Bound<&[u8]>,
		_end: Bound<&[u8]>,
		_scope: MultiVersionScope,
		_batch_size: usize,
	) -> Result<RangeBatch> {
		match *self {}
	}

	fn range_rev_next(
		&self,
		_table: EntryKind,
		_cursor: &mut RangeCursor,
		_start: Bound<&[u8]>,
		_end: Bound<&[u8]>,
		_scope: MultiVersionScope,
		_batch_size: usize,
	) -> Result<RangeBatch> {
		match *self {}
	}

	fn ensure_table(&self, _table: EntryKind) -> Result<()> {
		match *self {}
	}

	fn clear_table(&self, _table: EntryKind) -> Result<()> {
		match *self {}
	}

	fn drop(&self, _batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>>) -> Result<()> {
		match *self {}
	}

	fn get_all_versions(&self, _table: EntryKind, _key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		match *self {}
	}

	fn scan_historical_below(
		&self,
		_table: EntryKind,
		_cutoff: CommitVersion,
		_cursor: &mut HistoricalCursor,
		_batch_size: usize,
	) -> Result<Vec<(EncodedKey, CommitVersion)>> {
		match *self {}
	}
}

impl TierBackend for MultiPersistentTier {}
