// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Cold tier of the multi-version store. Holds the durable, version-history-bearing record of every key the
//! buffer has flushed. The default backend is SQLite; the trait surface is generic so other backends can be
//! plugged in without touching the buffer or transaction layer.

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{common::CommitVersion, encoded::key::EncodedKey, interface::store::EntryKind, row::TtlAnchor};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteConfig;
use reifydb_type::{Result, util::cowvec::CowVec};

use crate::tier::{HistoricalCursor, RangeBatch, RangeCursor, TierBackend, TierBatch, TierStorage, VersionedGetResult};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod sqlite;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use sqlite::storage::SqlitePersistentStorage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointOutcome {
	pub log_frames: u32,
	pub restarted: bool,
}

#[derive(Clone)]
#[cfg_attr(all(feature = "sqlite", not(target_arch = "wasm32")), repr(u8))]
pub enum MultiPersistentTier {
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	Sqlite(SqlitePersistentStorage) = 0,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl MultiPersistentTier {
	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::Sqlite(SqlitePersistentStorage::new(config))
	}

	pub fn sqlite_in_memory() -> Self {
		Self::Sqlite(SqlitePersistentStorage::in_memory())
	}

	pub fn maybe_checkpoint(&self) -> Result<CheckpointOutcome> {
		match self {
			Self::Sqlite(s) => s.maybe_checkpoint(),
		}
	}

	pub fn delete_expired(
		&self,
		table: EntryKind,
		anchor: TtlAnchor,
		cutoff_nanos: u64,
		prefix: Option<&[u8]>,
	) -> Result<u64> {
		match self {
			Self::Sqlite(s) => s.delete_expired(table, anchor, cutoff_nanos, prefix),
		}
	}

	pub fn delete_keys(&self, table: EntryKind, keys: &[EncodedKey]) -> Result<u64> {
		match self {
			Self::Sqlite(s) => s.delete_keys(table, keys),
		}
	}
}

#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
impl MultiPersistentTier {
	pub fn maybe_checkpoint(&self) -> Result<CheckpointOutcome> {
		match *self {}
	}

	pub fn delete_expired(
		&self,
		_table: EntryKind,
		_anchor: TtlAnchor,
		_cutoff_nanos: u64,
		_prefix: Option<&[u8]>,
	) -> Result<u64> {
		match *self {}
	}

	pub fn delete_keys(&self, _table: EntryKind, _keys: &[EncodedKey]) -> Result<u64> {
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
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Sqlite(s) => s.range_next(table, cursor, start, end, version, batch_size),
		}
	}

	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Sqlite(s) => s.range_rev_next(table, cursor, start, end, version, batch_size),
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
		_version: CommitVersion,
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
		_version: CommitVersion,
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
