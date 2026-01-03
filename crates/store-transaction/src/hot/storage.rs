// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Hot storage tier enum.
//!
//! This module provides the hot storage tier that dispatches to either
//! Memory or SQLite primitive storage implementations.

use std::{collections::HashMap, ops::Bound};

use async_trait::async_trait;
use reifydb_type::Result;

use super::{memory::MemoryPrimitiveStorage, sqlite::SqlitePrimitiveStorage};
use crate::tier::{RangeBatch, Store, TierBackend, TierStorage};

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
	Sqlite(SqlitePrimitiveStorage) = 1,
}

impl HotStorage {
	/// Create a new in-memory backend for testing
	pub async fn memory() -> Self {
		Self::Memory(MemoryPrimitiveStorage::new().await)
	}

	/// Create a new SQLite backend with in-memory database
	pub async fn sqlite_in_memory() -> Self {
		Self::Sqlite(SqlitePrimitiveStorage::in_memory().await)
	}

	/// Create a new SQLite backend with the given configuration
	pub async fn sqlite(config: super::sqlite::SqliteConfig) -> Self {
		Self::Sqlite(SqlitePrimitiveStorage::new(config).await)
	}
}

#[async_trait]
impl TierStorage for HotStorage {
	#[inline]
	async fn get(&self, table: Store, key: &[u8]) -> Result<Option<Vec<u8>>> {
		match self {
			Self::Memory(s) => s.get(table, key).await,
			Self::Sqlite(s) => s.get(table, key).await,
		}
	}

	#[inline]
	async fn contains(&self, table: Store, key: &[u8]) -> Result<bool> {
		match self {
			Self::Memory(s) => s.contains(table, key).await,
			Self::Sqlite(s) => s.contains(table, key).await,
		}
	}

	#[inline]
	async fn set(&self, batches: HashMap<Store, Vec<(Vec<u8>, Option<Vec<u8>>)>>) -> Result<()> {
		match self {
			Self::Memory(s) => s.set(batches).await,
			Self::Sqlite(s) => s.set(batches).await,
		}
	}

	#[inline]
	async fn range_batch(
		&self,
		table: Store,
		start: Bound<Vec<u8>>,
		end: Bound<Vec<u8>>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Memory(s) => s.range_batch(table, start, end, batch_size).await,
			Self::Sqlite(s) => s.range_batch(table, start, end, batch_size).await,
		}
	}

	#[inline]
	async fn range_rev_batch(
		&self,
		table: Store,
		start: Bound<Vec<u8>>,
		end: Bound<Vec<u8>>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Memory(s) => s.range_rev_batch(table, start, end, batch_size).await,
			Self::Sqlite(s) => s.range_rev_batch(table, start, end, batch_size).await,
		}
	}

	#[inline]
	async fn ensure_table(&self, table: Store) -> Result<()> {
		match self {
			Self::Memory(s) => s.ensure_table(table).await,
			Self::Sqlite(s) => s.ensure_table(table).await,
		}
	}

	#[inline]
	async fn clear_table(&self, table: Store) -> Result<()> {
		match self {
			Self::Memory(s) => s.clear_table(table).await,
			Self::Sqlite(s) => s.clear_table(table).await,
		}
	}
}

impl TierBackend for HotStorage {}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn test_memory_backend() {
		let storage = HotStorage::memory().await;

		storage.set(HashMap::from([(Store::Multi, vec![(b"key".to_vec(), Some(b"value".to_vec()))])]))
			.await
			.unwrap();
		assert_eq!(storage.get(Store::Multi, b"key").await.unwrap(), Some(b"value".to_vec()));
	}

	#[tokio::test]
	async fn test_sqlite_backend() {
		let storage = HotStorage::sqlite_in_memory().await;

		storage.set(HashMap::from([(Store::Multi, vec![(b"key".to_vec(), Some(b"value".to_vec()))])]))
			.await
			.unwrap();
		assert_eq!(storage.get(Store::Multi, b"key").await.unwrap(), Some(b"value".to_vec()));
	}

	#[tokio::test]
	async fn test_range_batch_memory() {
		let storage = HotStorage::memory().await;

		storage.set(HashMap::from([(
			Store::Multi,
			vec![
				(b"a".to_vec(), Some(b"1".to_vec())),
				(b"b".to_vec(), Some(b"2".to_vec())),
				(b"c".to_vec(), Some(b"3".to_vec())),
			],
		)]))
		.await
		.unwrap();

		let batch = storage.range_batch(Store::Multi, Bound::Unbounded, Bound::Unbounded, 100).await.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
	}

	#[tokio::test]
	async fn test_range_batch_sqlite() {
		let storage = HotStorage::sqlite_in_memory().await;

		storage.set(HashMap::from([(
			Store::Multi,
			vec![
				(b"a".to_vec(), Some(b"1".to_vec())),
				(b"b".to_vec(), Some(b"2".to_vec())),
				(b"c".to_vec(), Some(b"3".to_vec())),
			],
		)]))
		.await
		.unwrap();

		let batch = storage.range_batch(Store::Multi, Bound::Unbounded, Bound::Unbounded, 100).await.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
	}
}
