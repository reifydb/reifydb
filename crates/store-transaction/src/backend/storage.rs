// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Unified backend storage enum.
//!
//! This module provides a single enum that dispatches to either
//! Memory or SQLite primitive storage implementations.

use std::ops::Bound;

use reifydb_type::Result;

use super::{
	memory::MemoryPrimitiveStorage,
	primitive::{PrimitiveBackend, PrimitiveStorage, RawEntry, TableId},
	sqlite::SqlitePrimitiveStorage,
};

/// Unified backend storage enum.
///
/// Provides a single interface for storage operations, dispatching
/// to either Memory or SQLite implementations.
#[derive(Clone)]
#[repr(u8)]
pub enum BackendStorage {
	/// In-memory storage (non-persistent)
	Memory(MemoryPrimitiveStorage) = 0,
	/// SQLite-based persistent storage
	Sqlite(SqlitePrimitiveStorage) = 1,
}

impl BackendStorage {
	/// Create a new in-memory backend for testing
	pub fn memory() -> Self {
		Self::Memory(MemoryPrimitiveStorage::new())
	}

	/// Create a new SQLite backend with in-memory database
	pub fn sqlite_in_memory() -> Self {
		Self::Sqlite(SqlitePrimitiveStorage::in_memory())
	}

	/// Create a new SQLite backend with the given configuration
	pub fn sqlite(config: super::sqlite::SqliteConfig) -> Self {
		Self::Sqlite(SqlitePrimitiveStorage::new(config))
	}
}

/// Forward iterator for BackendStorage
pub enum BackendRangeIter<'a> {
	Memory(<MemoryPrimitiveStorage as PrimitiveStorage>::RangeIter<'a>),
	Sqlite(<SqlitePrimitiveStorage as PrimitiveStorage>::RangeIter<'a>),
}

impl<'a> Iterator for BackendRangeIter<'a> {
	type Item = Result<RawEntry>;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			Self::Memory(iter) => iter.next(),
			Self::Sqlite(iter) => iter.next(),
		}
	}
}

/// Reverse iterator for BackendStorage
pub enum BackendRangeRevIter<'a> {
	Memory(<MemoryPrimitiveStorage as PrimitiveStorage>::RangeRevIter<'a>),
	Sqlite(<SqlitePrimitiveStorage as PrimitiveStorage>::RangeRevIter<'a>),
}

impl<'a> Iterator for BackendRangeRevIter<'a> {
	type Item = Result<RawEntry>;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			Self::Memory(iter) => iter.next(),
			Self::Sqlite(iter) => iter.next(),
		}
	}
}

impl PrimitiveStorage for BackendStorage {
	type RangeIter<'a> = BackendRangeIter<'a>;
	type RangeRevIter<'a> = BackendRangeRevIter<'a>;

	#[inline]
	fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Vec<u8>>> {
		match self {
			Self::Memory(s) => s.get(table, key),
			Self::Sqlite(s) => s.get(table, key),
		}
	}

	#[inline]
	fn contains(&self, table: TableId, key: &[u8]) -> Result<bool> {
		match self {
			Self::Memory(s) => s.contains(table, key),
			Self::Sqlite(s) => s.contains(table, key),
		}
	}

	#[inline]
	fn put_batch(&self, table: TableId, entries: &[(&[u8], Option<&[u8]>)]) -> Result<()> {
		match self {
			Self::Memory(s) => s.put_batch(table, entries),
			Self::Sqlite(s) => s.put_batch(table, entries),
		}
	}

	#[inline]
	fn range(
		&self,
		table: TableId,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<Self::RangeIter<'_>> {
		match self {
			Self::Memory(s) => Ok(BackendRangeIter::Memory(s.range(table, start, end, batch_size)?)),
			Self::Sqlite(s) => Ok(BackendRangeIter::Sqlite(s.range(table, start, end, batch_size)?)),
		}
	}

	#[inline]
	fn range_rev(
		&self,
		table: TableId,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<Self::RangeRevIter<'_>> {
		match self {
			Self::Memory(s) => Ok(BackendRangeRevIter::Memory(s.range_rev(table, start, end, batch_size)?)),
			Self::Sqlite(s) => Ok(BackendRangeRevIter::Sqlite(s.range_rev(table, start, end, batch_size)?)),
		}
	}

	#[inline]
	fn ensure_table(&self, table: TableId) -> Result<()> {
		match self {
			Self::Memory(s) => s.ensure_table(table),
			Self::Sqlite(s) => s.ensure_table(table),
		}
	}

	#[inline]
	fn clear_table(&self, table: TableId) -> Result<()> {
		match self {
			Self::Memory(s) => s.clear_table(table),
			Self::Sqlite(s) => s.clear_table(table),
		}
	}
}

impl PrimitiveBackend for BackendStorage {}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_memory_backend() {
		let storage = BackendStorage::memory();

		storage.put(TableId::Multi, b"key", Some(b"value")).unwrap();
		assert_eq!(storage.get(TableId::Multi, b"key").unwrap(), Some(b"value".to_vec()));
	}

	#[test]
	fn test_sqlite_backend() {
		let storage = BackendStorage::sqlite_in_memory();

		storage.put(TableId::Multi, b"key", Some(b"value")).unwrap();
		assert_eq!(storage.get(TableId::Multi, b"key").unwrap(), Some(b"value".to_vec()));
	}

	#[test]
	fn test_range_iteration_memory() {
		let storage = BackendStorage::memory();

		storage.put(TableId::Multi, b"a", Some(b"1")).unwrap();
		storage.put(TableId::Multi, b"b", Some(b"2")).unwrap();
		storage.put(TableId::Multi, b"c", Some(b"3")).unwrap();

		let entries: Vec<_> = storage
			.range(TableId::Multi, Bound::Unbounded, Bound::Unbounded, 100)
			.unwrap()
			.collect::<Result<Vec<_>>>()
			.unwrap();

		assert_eq!(entries.len(), 3);
	}

	#[test]
	fn test_range_iteration_sqlite() {
		let storage = BackendStorage::sqlite_in_memory();

		storage.put(TableId::Multi, b"a", Some(b"1")).unwrap();
		storage.put(TableId::Multi, b"b", Some(b"2")).unwrap();
		storage.put(TableId::Multi, b"c", Some(b"3")).unwrap();

		let entries: Vec<_> = storage
			.range(TableId::Multi, Bound::Unbounded, Bound::Unbounded, 100)
			.unwrap()
			.collect::<Result<Vec<_>>>()
			.unwrap();

		assert_eq!(entries.len(), 3);
	}
}
