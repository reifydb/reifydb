// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Hot storage tier enum.
//!
//! This module provides the hot storage tier that dispatches to either
//! Memory or SQLite primitive storage implementations.

use std::{collections::HashMap, ops::Bound};

use reifydb_core::common::CommitVersion;
use reifydb_type::{Result, util::cowvec::CowVec};

use super::memory::storage::MemoryPrimitiveStorage;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use super::sqlite::storage::SqlitePrimitiveStorage;
use crate::tier::{EntryKind, RangeBatch, RangeCursor, TierBackend, TierStorage};

/// Hot storage tier.
///
/// Provides a single interface for hot tier storage operations, dispatching
/// to either Memory or SQLite implementations.
#[derive(Clone)]
#[repr(u8)]
pub enum HotStorage {
	/// In-memory storage (non-persistent)
	Memory(MemoryPrimitiveStorage) = 0,
	/// SQLite-based persistent storage
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	Sqlite(SqlitePrimitiveStorage) = 1,
}

impl HotStorage {
	/// Create a new in-memory backend
	pub fn memory() -> Self {
		Self::Memory(MemoryPrimitiveStorage::new())
	}

	/// Create a new SQLite backend with in-memory database
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite_in_memory() -> Self {
		Self::Sqlite(SqlitePrimitiveStorage::in_memory())
	}

	/// Create a new SQLite backend with the given configuration
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(config: super::sqlite::config::SqliteConfig) -> Self {
		Self::Sqlite(SqlitePrimitiveStorage::new(config))
	}
}

impl TierStorage for HotStorage {
	#[inline]
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<Option<CowVec<u8>>> {
		match self {
			Self::Memory(s) => s.get(table, key, version),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.get(table, key, version),
		}
	}

	#[inline]
	fn contains(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<bool> {
		match self {
			Self::Memory(s) => s.contains(table, key, version),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.contains(table, key, version),
		}
	}

	#[inline]
	fn set(
		&self,
		version: CommitVersion,
		batches: HashMap<EntryKind, Vec<(CowVec<u8>, Option<CowVec<u8>>)>>,
	) -> Result<()> {
		match self {
			Self::Memory(s) => s.set(version, batches),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.set(version, batches),
		}
	}

	#[inline]
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
			Self::Memory(s) => s.range_next(table, cursor, start, end, version, batch_size),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.range_next(table, cursor, start, end, version, batch_size),
		}
	}

	#[inline]
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
			Self::Memory(s) => s.range_rev_next(table, cursor, start, end, version, batch_size),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.range_rev_next(table, cursor, start, end, version, batch_size),
		}
	}

	#[inline]
	fn ensure_table(&self, table: EntryKind) -> Result<()> {
		match self {
			Self::Memory(s) => s.ensure_table(table),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.ensure_table(table),
		}
	}

	#[inline]
	fn clear_table(&self, table: EntryKind) -> Result<()> {
		match self {
			Self::Memory(s) => s.clear_table(table),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.clear_table(table),
		}
	}

	#[inline]
	fn drop(&self, batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>>) -> Result<()> {
		match self {
			Self::Memory(s) => s.drop(batches),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.drop(batches),
		}
	}

	#[inline]
	fn get_all_versions(&self, table: EntryKind, key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		match self {
			Self::Memory(s) => s.get_all_versions(table, key),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.get_all_versions(table, key),
		}
	}
}

impl TierBackend for HotStorage {}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_memory_backend() {
		let storage = HotStorage::memory();

		let key = CowVec::new(b"key".to_vec());
		let version = CommitVersion(1);

		storage.set(
			version,
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value".to_vec())))])]),
		)
		.unwrap();
		assert_eq!(storage.get(EntryKind::Multi, &key, version).unwrap().as_deref(), Some(b"value".as_slice()));
	}

	#[test]
	fn test_sqlite_backend() {
		let storage = HotStorage::sqlite_in_memory();

		let key = CowVec::new(b"key".to_vec());
		let version = CommitVersion(1);

		storage.set(
			version,
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value".to_vec())))])]),
		)
		.unwrap();
		assert_eq!(storage.get(EntryKind::Multi, &key, version).unwrap().as_deref(), Some(b"value".as_slice()));
	}

	#[test]
	fn test_range_next_memory() {
		let storage = HotStorage::memory();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
					(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
					(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 100)
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
	}

	#[test]
	fn test_range_next_sqlite() {
		let storage = HotStorage::sqlite_in_memory();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
					(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
					(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 100)
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
	}
}
