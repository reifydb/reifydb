// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_runtime::shutdown::Shutdown;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_value::{Result, util::cowvec::CowVec};

use crate::tier::{RangeBatch, RangeCursor, TierBackend, TierStorage};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod sqlite;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use sqlite::storage::SqlitePersistentStorage;

#[derive(Clone)]
#[cfg_attr(all(feature = "sqlite", not(target_arch = "wasm32")), repr(u8))]
pub enum SinglePersistentTier {
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	Sqlite(SqlitePersistentStorage) = 0,
}

impl Shutdown for SinglePersistentTier {
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
impl SinglePersistentTier {
	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::Sqlite(SqlitePersistentStorage::new(config))
	}

	pub fn sqlite_in_memory() -> (Self, SqliteTempPathGuard) {
		let (storage, guard) = SqlitePersistentStorage::in_memory();
		(Self::Sqlite(storage), guard)
	}
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl TierStorage for SinglePersistentTier {
	#[inline]
	fn get(&self, key: &[u8]) -> Result<Option<CowVec<u8>>> {
		match self {
			Self::Sqlite(s) => s.get(key),
		}
	}

	#[inline]
	fn contains(&self, key: &[u8]) -> Result<bool> {
		match self {
			Self::Sqlite(s) => s.contains(key),
		}
	}

	#[inline]
	fn get_with_tombstone(&self, key: &[u8]) -> Result<Option<Option<CowVec<u8>>>> {
		match self {
			Self::Sqlite(s) => s.get_with_tombstone(key),
		}
	}

	#[inline]
	fn set(&self, entries: Vec<(EncodedKey, Option<CowVec<u8>>)>) -> Result<()> {
		match self {
			Self::Sqlite(s) => s.set(entries),
		}
	}

	#[inline]
	fn range_next(
		&self,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Sqlite(s) => s.range_next(cursor, start, end, batch_size),
		}
	}

	#[inline]
	fn range_rev_next(
		&self,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Sqlite(s) => s.range_rev_next(cursor, start, end, batch_size),
		}
	}

	#[inline]
	fn ensure_table(&self) -> Result<()> {
		match self {
			Self::Sqlite(s) => s.ensure_table(),
		}
	}

	#[inline]
	fn clear_table(&self) -> Result<()> {
		match self {
			Self::Sqlite(s) => s.clear_table(),
		}
	}
}

#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
impl TierStorage for SinglePersistentTier {
	fn get(&self, _key: &[u8]) -> Result<Option<CowVec<u8>>> {
		match *self {}
	}

	fn get_with_tombstone(&self, _key: &[u8]) -> Result<Option<Option<CowVec<u8>>>> {
		match *self {}
	}

	fn set(&self, _entries: Vec<(EncodedKey, Option<CowVec<u8>>)>) -> Result<()> {
		match *self {}
	}

	fn range_next(
		&self,
		_cursor: &mut RangeCursor,
		_start: Bound<&[u8]>,
		_end: Bound<&[u8]>,
		_batch_size: usize,
	) -> Result<RangeBatch> {
		match *self {}
	}

	fn range_rev_next(
		&self,
		_cursor: &mut RangeCursor,
		_start: Bound<&[u8]>,
		_end: Bound<&[u8]>,
		_batch_size: usize,
	) -> Result<RangeBatch> {
		match *self {}
	}

	fn ensure_table(&self) -> Result<()> {
		match *self {}
	}

	fn clear_table(&self) -> Result<()> {
		match *self {}
	}
}

impl TierBackend for SinglePersistentTier {}
