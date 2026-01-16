// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Hot storage tier enum.
//!
//! This module provides the hot storage tier that dispatches to either
//! Memory or SQLite primitive storage implementations.

use std::ops::Bound;

use reifydb_core::runtime::compute::ComputePool;
use reifydb_type::{Result, util::cowvec::CowVec};

use super::{memory::storage::MemoryPrimitiveStorage, sqlite::storage::SqlitePrimitiveStorage};
use crate::tier::{RangeBatch, RangeCursor, TierBackend, TierStorage};

/// Hot storage tier.
///
/// Provides a single interface for hot tier storage operations, dispatching
/// to either Memory or SQLite implementations.
#[derive(Clone)]
#[repr(u8)]
pub enum HotTier {
	/// In-memory storage (non-persistent)
	Memory(MemoryPrimitiveStorage) = 0,
	/// SQLite-based persistent storage
	Sqlite(SqlitePrimitiveStorage) = 1,
}

impl HotTier {
	/// Create a new in-memory backend
	pub fn memory(compute_pool: ComputePool) -> Self {
		Self::Memory(MemoryPrimitiveStorage::new(compute_pool))
	}

	/// Create a new SQLite backend with in-memory database
	pub fn sqlite_in_memory() -> Self {
		Self::Sqlite(SqlitePrimitiveStorage::in_memory())
	}

	/// Create a new SQLite backend with the given configuration
	pub fn sqlite(config: super::sqlite::config::SqliteConfig) -> Self {
		Self::Sqlite(SqlitePrimitiveStorage::new(config))
	}
}

impl TierStorage for HotTier {
	#[inline]
	fn get(&self, key: &[u8]) -> Result<Option<CowVec<u8>>> {
		match self {
			Self::Memory(s) => s.get(key),
			Self::Sqlite(s) => s.get(key),
		}
	}

	#[inline]
	fn contains(&self, key: &[u8]) -> Result<bool> {
		match self {
			Self::Memory(s) => s.contains(key),
			Self::Sqlite(s) => s.contains(key),
		}
	}

	#[inline]
	fn set(&self, entries: Vec<(CowVec<u8>, Option<CowVec<u8>>)>) -> Result<()> {
		match self {
			Self::Memory(s) => s.set(entries),
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
			Self::Memory(s) => s.range_next(cursor, start, end, batch_size),
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
			Self::Memory(s) => s.range_rev_next(cursor, start, end, batch_size),
			Self::Sqlite(s) => s.range_rev_next(cursor, start, end, batch_size),
		}
	}

	#[inline]
	fn ensure_table(&self) -> Result<()> {
		match self {
			Self::Memory(s) => s.ensure_table(),
			Self::Sqlite(s) => s.ensure_table(),
		}
	}

	#[inline]
	fn clear_table(&self) -> Result<()> {
		match self {
			Self::Memory(s) => s.clear_table(),
			Self::Sqlite(s) => s.clear_table(),
		}
	}
}

impl TierBackend for HotTier {}

#[cfg(test)]
pub mod tests {
	use reifydb_core::runtime::compute::ComputePool;

	use super::*;

	fn test_compute_pool() -> ComputePool {
		ComputePool::new(2, 8)
	}

	#[test]
	fn test_memory_backend() {
		let storage = HotTier::memory(test_compute_pool());

		storage.set(vec![(CowVec::new(b"key".to_vec()), Some(CowVec::new(b"value".to_vec())))]).unwrap();
		assert_eq!(storage.get(b"key").unwrap().as_deref(), Some(b"value".as_slice()));
	}

	#[test]
	fn test_sqlite_backend() {
		let storage = HotTier::sqlite_in_memory();

		storage.set(vec![(CowVec::new(b"key".to_vec()), Some(CowVec::new(b"value".to_vec())))]).unwrap();
		assert_eq!(storage.get(b"key").unwrap().as_deref(), Some(b"value".as_slice()));
	}

	#[test]
	fn test_range_next_memory() {
		let storage = HotTier::memory(test_compute_pool());

		storage.set(vec![
			(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
			(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
			(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
		])
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage.range_next(&mut cursor, Bound::Unbounded, Bound::Unbounded, 100).unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
	}

	#[test]
	fn test_range_next_sqlite() {
		let storage = HotTier::sqlite_in_memory();

		storage.set(vec![
			(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
			(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
			(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
		])
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage.range_next(&mut cursor, Bound::Unbounded, Bound::Unbounded, 100).unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
	}
}
