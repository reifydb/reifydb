// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Memory implementation of PrimitiveStorage.
//!
//! Uses BTreeMap for ordered key-value storage with RwLock for thread safety.

use std::{collections::HashMap, ops::Bound, sync::Arc};

use async_trait::async_trait;
use reifydb_type::Result;
use tokio::sync::RwLock;
use tracing::instrument;

use super::tables::Tables;
use crate::tier::{RangeBatch, RangeCursor, RawEntry, Store, TierBackend, TierStorage};

/// Memory-based primitive storage implementation.
///
/// Stores data in BTreeMaps with RwLock for concurrent access.
/// Uses direct writes for maximum performance.
#[derive(Clone)]
pub struct MemoryPrimitiveStorage {
	inner: Arc<MemoryPrimitiveStorageInner>,
}

struct MemoryPrimitiveStorageInner {
	/// Storage for each table type
	tables: RwLock<Tables>,
}

impl MemoryPrimitiveStorage {
	#[instrument(name = "store::memory::new", level = "debug")]
	pub async fn new() -> Self {
		Self {
			inner: Arc::new(MemoryPrimitiveStorageInner {
				tables: RwLock::new(Tables::default()),
			}),
		}
	}
}

#[async_trait]
impl TierStorage for MemoryPrimitiveStorage {
	#[instrument(name = "store::memory::get", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len()))]
	async fn get(&self, table: Store, key: &[u8]) -> Result<Option<Vec<u8>>> {
		let tables = self.inner.tables.read().await;
		if let Some(table_data) = tables.get_table(table) {
			Ok(table_data.get(key).cloned().flatten())
		} else {
			Ok(None)
		}
	}

	#[instrument(name = "store::memory::contains", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len()), ret)]
	async fn contains(&self, table: Store, key: &[u8]) -> Result<bool> {
		let tables = self.inner.tables.read().await;
		if let Some(table_data) = tables.get_table(table) {
			// Key exists and is not a tombstone
			Ok(table_data.get(key).map_or(false, |v| v.is_some()))
		} else {
			Ok(false)
		}
	}

	#[instrument(name = "store::memory::set", level = "debug", skip(self, batches), fields(table_count = batches.len()))]
	async fn set(&self, batches: HashMap<Store, Vec<(Vec<u8>, Option<Vec<u8>>)>>) -> Result<()> {
		let mut guard = self.inner.tables.write().await;
		for (table, entries) in batches {
			let table_data = guard.get_table_mut(table);
			for (key, value) in entries {
				table_data.insert(key, value);
			}
		}
		Ok(())
	}

	#[instrument(name = "store::memory::range_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size))]
	async fn range_next(
		&self,
		table: Store,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let tables = self.inner.tables.read().await;
		if let Some(table_data) = tables.get_table(table) {
			// Determine effective start bound based on cursor state
			let effective_start: Bound<&[u8]> = match &cursor.last_key {
				Some(last) => Bound::Excluded(last.as_slice()),
				None => start,
			};

			let range_bounds = make_range_bounds_ref(effective_start, end);

			// Fetch batch_size + 1 to determine if there are more entries
			let entries: Vec<RawEntry> = table_data
				.range::<[u8], _>(range_bounds)
				.take(batch_size + 1)
				.map(|(k, v)| RawEntry {
					key: k.clone(),
					value: v.clone(),
				})
				.collect();

			let has_more = entries.len() > batch_size;
			let entries: Vec<RawEntry> = if has_more {
				entries.into_iter().take(batch_size).collect()
			} else {
				entries
			};

			// Update cursor
			if let Some(last_entry) = entries.last() {
				cursor.last_key = Some(last_entry.key.clone());
			}
			if !has_more {
				cursor.exhausted = true;
			}

			Ok(RangeBatch {
				entries,
				has_more,
			})
		} else {
			cursor.exhausted = true;
			Ok(RangeBatch::empty())
		}
	}

	#[instrument(name = "store::memory::range_rev_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size))]
	async fn range_rev_next(
		&self,
		table: Store,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let tables = self.inner.tables.read().await;
		if let Some(table_data) = tables.get_table(table) {
			// For reverse iteration, effective end bound based on cursor
			let effective_end: Bound<&[u8]> = match &cursor.last_key {
				Some(last) => Bound::Excluded(last.as_slice()),
				None => end,
			};

			let range_bounds = make_range_bounds_ref(start, effective_end);

			// Fetch batch_size + 1 to determine if there are more entries
			let entries: Vec<RawEntry> = table_data
				.range::<[u8], _>(range_bounds)
				.rev()
				.take(batch_size + 1)
				.map(|(k, v)| RawEntry {
					key: k.clone(),
					value: v.clone(),
				})
				.collect();

			let has_more = entries.len() > batch_size;
			let entries: Vec<RawEntry> = if has_more {
				entries.into_iter().take(batch_size).collect()
			} else {
				entries
			};

			// Update cursor
			if let Some(last_entry) = entries.last() {
				cursor.last_key = Some(last_entry.key.clone());
			}
			if !has_more {
				cursor.exhausted = true;
			}

			Ok(RangeBatch {
				entries,
				has_more,
			})
		} else {
			cursor.exhausted = true;
			Ok(RangeBatch::empty())
		}
	}

	#[instrument(name = "store::memory::ensure_table", level = "trace", skip(self), fields(table = ?table))]
	async fn ensure_table(&self, table: Store) -> Result<()> {
		// For memory backend, tables are created on-demand, so this is a no-op
		let mut tables = self.inner.tables.write().await;
		let _ = tables.get_table_mut(table);
		Ok(())
	}

	#[instrument(name = "store::memory::clear_table", level = "debug", skip(self), fields(table = ?table))]
	async fn clear_table(&self, table: Store) -> Result<()> {
		let mut guard = self.inner.tables.write().await;
		let table_data = guard.get_table_mut(table);
		table_data.clear();
		Ok(())
	}
}

impl TierBackend for MemoryPrimitiveStorage {}

/// Convert Bound<&[u8]> to a tuple for BTreeMap range queries.
fn make_range_bounds_ref<'a>(start: Bound<&'a [u8]>, end: Bound<&'a [u8]>) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
	(start, end)
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::TableId as CoreTableId;

	use super::*;

	#[tokio::test]
	async fn test_basic_operations() {
		let storage = MemoryPrimitiveStorage::new().await;

		// Put and get
		storage.set(HashMap::from([(Store::Multi, vec![(b"key1".to_vec(), Some(b"value1".to_vec()))])]))
			.await
			.unwrap();
		let value = storage.get(Store::Multi, b"key1").await.unwrap();
		assert_eq!(value, Some(b"value1".to_vec()));

		// Contains
		assert!(storage.contains(Store::Multi, b"key1").await.unwrap());
		assert!(!storage.contains(Store::Multi, b"nonexistent").await.unwrap());

		// Delete (tombstone)
		storage.set(HashMap::from([(Store::Multi, vec![(b"key1".to_vec(), None)])])).await.unwrap();
		assert!(!storage.contains(Store::Multi, b"key1").await.unwrap());
	}

	#[tokio::test]
	async fn test_separate_tables() {
		let storage = MemoryPrimitiveStorage::new().await;

		storage.set(HashMap::from([(Store::Multi, vec![(b"key".to_vec(), Some(b"multi".to_vec()))])]))
			.await
			.unwrap();
		storage.set(HashMap::from([(Store::Single, vec![(b"key".to_vec(), Some(b"single".to_vec()))])]))
			.await
			.unwrap();

		assert_eq!(storage.get(Store::Multi, b"key").await.unwrap(), Some(b"multi".to_vec()));
		assert_eq!(storage.get(Store::Single, b"key").await.unwrap(), Some(b"single".to_vec()));
	}

	#[tokio::test]
	async fn test_source_tables() {
		use reifydb_core::interface::PrimitiveId;

		let storage = MemoryPrimitiveStorage::new().await;

		let source1 = PrimitiveId::Table(CoreTableId(1));
		let source2 = PrimitiveId::Table(CoreTableId(2));

		storage.set(HashMap::from([(
			Store::Source(source1),
			vec![(b"key".to_vec(), Some(b"table1".to_vec()))],
		)]))
		.await
		.unwrap();
		storage.set(HashMap::from([(
			Store::Source(source2),
			vec![(b"key".to_vec(), Some(b"table2".to_vec()))],
		)]))
		.await
		.unwrap();

		assert_eq!(storage.get(Store::Source(source1), b"key").await.unwrap(), Some(b"table1".to_vec()));
		assert_eq!(storage.get(Store::Source(source2), b"key").await.unwrap(), Some(b"table2".to_vec()));
	}

	#[tokio::test]
	async fn test_range_next() {
		let storage = MemoryPrimitiveStorage::new().await;

		storage.set(HashMap::from([(Store::Multi, vec![(b"a".to_vec(), Some(b"1".to_vec()))])])).await.unwrap();
		storage.set(HashMap::from([(Store::Multi, vec![(b"b".to_vec(), Some(b"2".to_vec()))])])).await.unwrap();
		storage.set(HashMap::from([(Store::Multi, vec![(b"c".to_vec(), Some(b"3".to_vec()))])])).await.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(Store::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 100)
			.await
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
		assert_eq!(batch.entries[0].key, b"a".to_vec());
		assert_eq!(batch.entries[1].key, b"b".to_vec());
		assert_eq!(batch.entries[2].key, b"c".to_vec());
	}

	#[tokio::test]
	async fn test_range_rev_next() {
		let storage = MemoryPrimitiveStorage::new().await;

		storage.set(HashMap::from([(Store::Multi, vec![(b"a".to_vec(), Some(b"1".to_vec()))])])).await.unwrap();
		storage.set(HashMap::from([(Store::Multi, vec![(b"b".to_vec(), Some(b"2".to_vec()))])])).await.unwrap();
		storage.set(HashMap::from([(Store::Multi, vec![(b"c".to_vec(), Some(b"3".to_vec()))])])).await.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_rev_next(Store::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 100)
			.await
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
		assert_eq!(batch.entries[0].key, b"c".to_vec());
		assert_eq!(batch.entries[1].key, b"b".to_vec());
		assert_eq!(batch.entries[2].key, b"a".to_vec());
	}

	#[tokio::test]
	async fn test_range_streaming_pagination() {
		let storage = MemoryPrimitiveStorage::new().await;

		// Insert 10 entries
		for i in 0..10u8 {
			storage.set(HashMap::from([(Store::Multi, vec![(vec![i], Some(vec![i * 10]))])]))
				.await
				.unwrap();
		}

		// Use a single cursor to stream through all entries
		let mut cursor = RangeCursor::new();

		// First batch of 3
		let batch1 = storage
			.range_next(Store::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.await
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(batch1.entries[0].key, vec![0]);
		assert_eq!(batch1.entries[2].key, vec![2]);

		// Second batch of 3 - cursor automatically continues
		let batch2 = storage
			.range_next(Store::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.await
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(batch2.entries[0].key, vec![3]);
		assert_eq!(batch2.entries[2].key, vec![5]);

		// Third batch of 3
		let batch3 = storage
			.range_next(Store::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.await
			.unwrap();
		assert_eq!(batch3.entries.len(), 3);
		assert!(batch3.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(batch3.entries[0].key, vec![6]);
		assert_eq!(batch3.entries[2].key, vec![8]);

		// Fourth batch - only 1 entry remaining
		let batch4 = storage
			.range_next(Store::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.await
			.unwrap();
		assert_eq!(batch4.entries.len(), 1);
		assert!(!batch4.has_more);
		assert!(cursor.exhausted);
		assert_eq!(batch4.entries[0].key, vec![9]);

		// Fifth call - exhausted
		let batch5 = storage
			.range_next(Store::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.await
			.unwrap();
		assert!(batch5.entries.is_empty());
	}

	#[tokio::test]
	async fn test_range_rev_streaming_pagination() {
		let storage = MemoryPrimitiveStorage::new().await;

		// Insert 10 entries
		for i in 0..10u8 {
			storage.set(HashMap::from([(Store::Multi, vec![(vec![i], Some(vec![i * 10]))])]))
				.await
				.unwrap();
		}

		// Use a single cursor to stream in reverse
		let mut cursor = RangeCursor::new();

		// First batch of 3 (reverse)
		let batch1 = storage
			.range_rev_next(Store::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.await
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(batch1.entries[0].key, vec![9]);
		assert_eq!(batch1.entries[2].key, vec![7]);

		// Second batch
		let batch2 = storage
			.range_rev_next(Store::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.await
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(batch2.entries[0].key, vec![6]);
		assert_eq!(batch2.entries[2].key, vec![4]);
	}
}
