// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{common::CommitVersion, interface::store::EntryKind};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteConfig;
use reifydb_type::{Result, util::cowvec::CowVec};

use crate::tier::{HistoricalCursor, RangeBatch, RangeCursor, TierBackend, TierBatch, TierStorage};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod sqlite;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use sqlite::storage::SqliteWarmStorage;

#[derive(Clone)]
#[cfg_attr(all(feature = "sqlite", not(target_arch = "wasm32")), repr(u8))]
pub enum WarmStorage {
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	Sqlite(SqliteWarmStorage) = 0,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl WarmStorage {
	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::Sqlite(SqliteWarmStorage::new(config))
	}

	pub fn sqlite_in_memory() -> Self {
		Self::Sqlite(SqliteWarmStorage::in_memory())
	}
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl TierStorage for WarmStorage {
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<Option<CowVec<u8>>> {
		match self {
			Self::Sqlite(s) => s.get(table, key, version),
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

	fn drop(&self, batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>>) -> Result<()> {
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
	) -> Result<Vec<(CowVec<u8>, CommitVersion)>> {
		match self {
			Self::Sqlite(s) => s.scan_historical_below(table, cutoff, cursor, batch_size),
		}
	}
}

#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
impl TierStorage for WarmStorage {
	fn get(&self, _table: EntryKind, _key: &[u8], _version: CommitVersion) -> Result<Option<CowVec<u8>>> {
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

	fn drop(&self, _batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>>) -> Result<()> {
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
	) -> Result<Vec<(CowVec<u8>, CommitVersion)>> {
		match *self {}
	}
}

impl TierBackend for WarmStorage {}
