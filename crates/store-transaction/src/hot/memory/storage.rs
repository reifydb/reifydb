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
use crate::tier::{RangeBatch, RawEntry, TableId, TierBackend, TierStorage};

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
	async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Vec<u8>>> {
		let tables = self.inner.tables.read().await;
		if let Some(table_data) = tables.get_table(table) {
			Ok(table_data.get(key).cloned().flatten())
		} else {
			Ok(None)
		}
	}

	#[instrument(name = "store::memory::contains", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len()), ret)]
	async fn contains(&self, table: TableId, key: &[u8]) -> Result<bool> {
		let tables = self.inner.tables.read().await;
		if let Some(table_data) = tables.get_table(table) {
			// Key exists and is not a tombstone
			Ok(table_data.get(key).map_or(false, |v| v.is_some()))
		} else {
			Ok(false)
		}
	}

	#[instrument(name = "store::memory::set", level = "debug", skip(self, batches), fields(table_count = batches.len()))]
	async fn set(&self, batches: HashMap<TableId, Vec<(Vec<u8>, Option<Vec<u8>>)>>) -> Result<()> {
		let mut guard = self.inner.tables.write().await;
		for (table, entries) in batches {
			let table_data = guard.get_table_mut(table);
			for (key, value) in entries {
				table_data.insert(key, value);
			}
		}
		Ok(())
	}

	#[instrument(name = "store::memory::range_batch", level = "trace", skip(self, start, end), fields(table = ?table, batch_size = batch_size))]
	async fn range_batch(
		&self,
		table: TableId,
		start: Bound<Vec<u8>>,
		end: Bound<Vec<u8>>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		let tables = self.inner.tables.read().await;
		if let Some(table_data) = tables.get_table(table) {
			let range_bounds = make_range_bounds(&start, &end);

			// Fetch batch_size + 1 to determine if there are more entries
			let entries: Vec<RawEntry> = table_data
				.range::<Vec<u8>, _>(range_bounds)
				.take(batch_size + 1)
				.map(|(k, v)| RawEntry {
					key: k.clone(),
					value: v.clone(),
				})
				.collect();

			let has_more = entries.len() > batch_size;
			let entries = if has_more {
				entries.into_iter().take(batch_size).collect()
			} else {
				entries
			};

			Ok(RangeBatch {
				entries,
				has_more,
			})
		} else {
			Ok(RangeBatch::empty())
		}
	}

	#[instrument(name = "store::memory::range_rev_batch", level = "trace", skip(self, start, end), fields(table = ?table, batch_size = batch_size))]
	async fn range_rev_batch(
		&self,
		table: TableId,
		start: Bound<Vec<u8>>,
		end: Bound<Vec<u8>>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		let tables = self.inner.tables.read().await;
		if let Some(table_data) = tables.get_table(table) {
			let range_bounds = make_range_bounds(&start, &end);

			// Fetch batch_size + 1 to determine if there are more entries
			let entries: Vec<RawEntry> = table_data
				.range::<Vec<u8>, _>(range_bounds)
				.rev()
				.take(batch_size + 1)
				.map(|(k, v)| RawEntry {
					key: k.clone(),
					value: v.clone(),
				})
				.collect();

			let has_more = entries.len() > batch_size;
			let entries = if has_more {
				entries.into_iter().take(batch_size).collect()
			} else {
				entries
			};

			Ok(RangeBatch {
				entries,
				has_more,
			})
		} else {
			Ok(RangeBatch::empty())
		}
	}

	#[instrument(name = "store::memory::ensure_table", level = "trace", skip(self), fields(table = ?table))]
	async fn ensure_table(&self, table: TableId) -> Result<()> {
		// For memory backend, tables are created on-demand, so this is a no-op
		let mut tables = self.inner.tables.write().await;
		let _ = tables.get_table_mut(table);
		Ok(())
	}

	#[instrument(name = "store::memory::clear_table", level = "debug", skip(self), fields(table = ?table))]
	async fn clear_table(&self, table: TableId) -> Result<()> {
		let mut guard = self.inner.tables.write().await;
		let table_data = guard.get_table_mut(table);
		table_data.clear();
		Ok(())
	}
}

impl TierBackend for MemoryPrimitiveStorage {}

/// Convert Bound references to a tuple for BTreeMap range queries.
fn make_range_bounds<'a>(
	start: &'a Bound<Vec<u8>>,
	end: &'a Bound<Vec<u8>>,
) -> (Bound<&'a Vec<u8>>, Bound<&'a Vec<u8>>) {
	let start_bound = match start {
		Bound::Included(v) => Bound::Included(v),
		Bound::Excluded(v) => Bound::Excluded(v),
		Bound::Unbounded => Bound::Unbounded,
	};
	let end_bound = match end {
		Bound::Included(v) => Bound::Included(v),
		Bound::Excluded(v) => Bound::Excluded(v),
		Bound::Unbounded => Bound::Unbounded,
	};
	(start_bound, end_bound)
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::TableId as CoreTableId;

	use super::*;

	#[tokio::test]
	async fn test_basic_operations() {
		let storage = MemoryPrimitiveStorage::new().await;

		// Put and get
		storage.set(HashMap::from([(TableId::Multi, vec![(b"key1".to_vec(), Some(b"value1".to_vec()))])]))
			.await
			.unwrap();
		let value = storage.get(TableId::Multi, b"key1").await.unwrap();
		assert_eq!(value, Some(b"value1".to_vec()));

		// Contains
		assert!(storage.contains(TableId::Multi, b"key1").await.unwrap());
		assert!(!storage.contains(TableId::Multi, b"nonexistent").await.unwrap());

		// Delete (tombstone)
		storage.set(HashMap::from([(TableId::Multi, vec![(b"key1".to_vec(), None)])])).await.unwrap();
		assert!(!storage.contains(TableId::Multi, b"key1").await.unwrap());
	}

	#[tokio::test]
	async fn test_separate_tables() {
		let storage = MemoryPrimitiveStorage::new().await;

		storage.set(HashMap::from([(TableId::Multi, vec![(b"key".to_vec(), Some(b"multi".to_vec()))])]))
			.await
			.unwrap();
		storage.set(HashMap::from([(TableId::Single, vec![(b"key".to_vec(), Some(b"single".to_vec()))])]))
			.await
			.unwrap();

		assert_eq!(storage.get(TableId::Multi, b"key").await.unwrap(), Some(b"multi".to_vec()));
		assert_eq!(storage.get(TableId::Single, b"key").await.unwrap(), Some(b"single".to_vec()));
	}

	#[tokio::test]
	async fn test_source_tables() {
		use reifydb_core::interface::PrimitiveId;

		let storage = MemoryPrimitiveStorage::new().await;

		let source1 = PrimitiveId::Table(CoreTableId(1));
		let source2 = PrimitiveId::Table(CoreTableId(2));

		storage.set(HashMap::from([(
			TableId::Source(source1),
			vec![(b"key".to_vec(), Some(b"table1".to_vec()))],
		)]))
		.await
		.unwrap();
		storage.set(HashMap::from([(
			TableId::Source(source2),
			vec![(b"key".to_vec(), Some(b"table2".to_vec()))],
		)]))
		.await
		.unwrap();

		assert_eq!(storage.get(TableId::Source(source1), b"key").await.unwrap(), Some(b"table1".to_vec()));
		assert_eq!(storage.get(TableId::Source(source2), b"key").await.unwrap(), Some(b"table2".to_vec()));
	}

	#[tokio::test]
	async fn test_range_batch() {
		let storage = MemoryPrimitiveStorage::new().await;

		storage.set(HashMap::from([(TableId::Multi, vec![(b"a".to_vec(), Some(b"1".to_vec()))])]))
			.await
			.unwrap();
		storage.set(HashMap::from([(TableId::Multi, vec![(b"b".to_vec(), Some(b"2".to_vec()))])]))
			.await
			.unwrap();
		storage.set(HashMap::from([(TableId::Multi, vec![(b"c".to_vec(), Some(b"3".to_vec()))])]))
			.await
			.unwrap();

		let batch = storage.range_batch(TableId::Multi, Bound::Unbounded, Bound::Unbounded, 100).await.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert_eq!(batch.entries[0].key, b"a".to_vec());
		assert_eq!(batch.entries[1].key, b"b".to_vec());
		assert_eq!(batch.entries[2].key, b"c".to_vec());
	}

	#[tokio::test]
	async fn test_range_rev_batch() {
		let storage = MemoryPrimitiveStorage::new().await;

		storage.set(HashMap::from([(TableId::Multi, vec![(b"a".to_vec(), Some(b"1".to_vec()))])]))
			.await
			.unwrap();
		storage.set(HashMap::from([(TableId::Multi, vec![(b"b".to_vec(), Some(b"2".to_vec()))])]))
			.await
			.unwrap();
		storage.set(HashMap::from([(TableId::Multi, vec![(b"c".to_vec(), Some(b"3".to_vec()))])]))
			.await
			.unwrap();

		let batch =
			storage.range_rev_batch(TableId::Multi, Bound::Unbounded, Bound::Unbounded, 100).await.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert_eq!(batch.entries[0].key, b"c".to_vec());
		assert_eq!(batch.entries[1].key, b"b".to_vec());
		assert_eq!(batch.entries[2].key, b"a".to_vec());
	}

	#[tokio::test]
	async fn test_range_batch_pagination() {
		let storage = MemoryPrimitiveStorage::new().await;

		// Insert 10 entries
		for i in 0..10u8 {
			storage.set(HashMap::from([(TableId::Multi, vec![(vec![i], Some(vec![i * 10]))])]))
				.await
				.unwrap();
		}

		// First batch of 3
		let batch1 = storage.range_batch(TableId::Multi, Bound::Unbounded, Bound::Unbounded, 3).await.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert_eq!(batch1.entries[0].key, vec![0]);
		assert_eq!(batch1.entries[2].key, vec![2]);

		// Next batch using last key
		let last_key = batch1.entries.last().unwrap().key.clone();
		let batch2 = storage
			.range_batch(TableId::Multi, Bound::Excluded(last_key), Bound::Unbounded, 3)
			.await
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert_eq!(batch2.entries[0].key, vec![3]);
		assert_eq!(batch2.entries[2].key, vec![5]);
	}

	#[tokio::test]
	async fn test_range_rev_batch_pagination() {
		let storage = MemoryPrimitiveStorage::new().await;

		// Insert 10 entries
		for i in 0..10u8 {
			storage.set(HashMap::from([(TableId::Multi, vec![(vec![i], Some(vec![i * 10]))])]))
				.await
				.unwrap();
		}

		// First batch of 3 (reverse)
		let batch1 =
			storage.range_rev_batch(TableId::Multi, Bound::Unbounded, Bound::Unbounded, 3).await.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert_eq!(batch1.entries[0].key, vec![9]);
		assert_eq!(batch1.entries[2].key, vec![7]);

		// Next batch using last key (reverse continues from before last key)
		let last_key = batch1.entries.last().unwrap().key.clone();
		let batch2 = storage
			.range_rev_batch(TableId::Multi, Bound::Unbounded, Bound::Excluded(last_key), 3)
			.await
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert_eq!(batch2.entries[0].key, vec![6]);
		assert_eq!(batch2.entries[2].key, vec![4]);
	}
}
