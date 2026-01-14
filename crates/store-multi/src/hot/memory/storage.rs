// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Memory implementation of PrimitiveStorage.
//!
//! Uses DashMap for per-table sharding and RwLock<BTreeMap> for concurrent access.

use rayon::prelude::*;
use reifydb_core::runtime::ComputePool;
use reifydb_type::{CowVec, Result};
use std::{collections::HashMap, ops::Bound, sync::Arc};
use tracing::{Span, instrument};

use super::entry::{Entries, Entry, OrderedMap, entry_id_to_key};
use crate::tier::{EntryKind, RangeBatch, RangeCursor, RawEntry, TierBackend, TierStorage};

/// Memory-based primitive storage implementation.
///
/// Uses DashMap for per-table sharding with RwLock<BTreeMap> for concurrent access.
#[derive(Clone)]
pub struct MemoryPrimitiveStorage {
	inner: Arc<MemoryPrimitiveStorageInner>,
}

struct MemoryPrimitiveStorageInner {
	/// Storage for each type
	entries: Entries,
	/// Compute pool for parallel processing
	compute_pool: ComputePool,
}

impl MemoryPrimitiveStorage {
	#[instrument(name = "store::multi::memory::new", level = "debug", skip(compute_pool))]
	pub fn new(compute_pool: ComputePool) -> Self {
		Self {
			inner: Arc::new(MemoryPrimitiveStorageInner {
				entries: Entries::default(),
				compute_pool,
			}),
		}
	}

	/// Get or create a table entry
	#[inline]
	#[instrument(name = "store::multi::memory::get_or_create_table", level = "trace", skip(self), fields(table = ?table))]
	fn get_or_create_table(&self, table: EntryKind) -> Entry {
		let table_key = entry_id_to_key(table);
		self.inner.entries.data.entry(table_key).or_insert_with(Entry::new).value().clone()
	}

	/// Process a single table batch: get/create table, write entries
	#[inline]
	#[instrument(name = "store::multi::memory::set::table", level = "trace", skip(self, entries), fields(
		table = ?table,
		entry_count = entries.len(),
	))]
	fn process_table(&self, table: EntryKind, entries: Vec<(CowVec<u8>, Option<CowVec<u8>>)>) {
		let table_entry = self.get_or_create_table(table);
		let mut map = table_entry.data.write();
		for (key, value) in entries {
			map.insert(key, value);
		}
	}
}

impl TierStorage for MemoryPrimitiveStorage {
	#[instrument(name = "store::multi::memory::get", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len()))]
	fn get(&self, table: EntryKind, key: &[u8]) -> Result<Option<CowVec<u8>>> {
		let table_key = entry_id_to_key(table);
		let entry = match self.inner.entries.data.get(&table_key) {
			Some(e) => e.value().clone(),
			None => return Ok(None),
		};

		let map = entry.data.read();
		Ok(map.get(key).cloned().flatten())
	}

	#[instrument(name = "store::multi::memory::contains", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len()), ret)]
	fn contains(&self, table: EntryKind, key: &[u8]) -> Result<bool> {
		let table_key = entry_id_to_key(table);
		let entry = match self.inner.entries.data.get(&table_key) {
			Some(e) => e.value().clone(),
			None => return Ok(false),
		};

		let map = entry.data.read();
		// Key exists and is not a tombstone (None value)
		Ok(map.get(key).map_or(false, |v| v.is_some()))
	}

	#[instrument(name = "store::multi::memory::set", level = "trace", skip(self, batches), fields(
		table_count = batches.len(),
		total_entry_count = tracing::field::Empty
	))]
	fn set(&self, batches: HashMap<EntryKind, Vec<(CowVec<u8>, Option<CowVec<u8>>)>>) -> Result<()> {
		let total_entries: usize = batches.values().map(|v| v.len()).sum();

		self.inner.compute_pool.install(|| {
			batches.into_par_iter().for_each(|(table, entries)| {
				self.process_table(table, entries);
			});
		});

		Span::current().record("total_entry_count", total_entries);
		Ok(())
	}

	#[instrument(name = "store::multi::memory::range_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size))]
	fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let table_key = entry_id_to_key(table);
		let entry = match self.inner.entries.data.get(&table_key) {
			Some(e) => e.value().clone(),
			None => {
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
		};

		// Determine effective start bound based on cursor state
		let effective_start: Bound<&[u8]> = match &cursor.last_key {
			Some(last) => Bound::Excluded(last.as_slice()),
			None => start,
		};

		let map = entry.data.read();

		// Fetch batch_size + 1 to determine if there are more entries
		let entries: Vec<RawEntry> = map
			.range::<[u8], _>((effective_start, end))
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
	}

	#[instrument(name = "store::multi::memory::range_rev_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size))]
	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let table_key = entry_id_to_key(table);
		let entry = match self.inner.entries.data.get(&table_key) {
			Some(e) => e.value().clone(),
			None => {
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
		};

		// For reverse iteration, effective end bound based on cursor
		let effective_end: Bound<&[u8]> = match &cursor.last_key {
			Some(last) => Bound::Excluded(last.as_slice()),
			None => end,
		};

		let map = entry.data.read();

		// Fetch batch_size + 1 to determine if there are more entries
		let entries: Vec<RawEntry> = map
			.range::<[u8], _>((start, effective_end))
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
	}

	#[instrument(name = "store::multi::memory::ensure_table", level = "trace", skip(self), fields(table = ?table))]
	fn ensure_table(&self, table: EntryKind) -> Result<()> {
		let _ = self.get_or_create_table(table);
		Ok(())
	}

	#[instrument(name = "store::multi::memory::clear_table", level = "debug", skip(self), fields(table = ?table))]
	fn clear_table(&self, table: EntryKind) -> Result<()> {
		let table_key = entry_id_to_key(table);
		if let Some(entry) = self.inner.entries.data.get(&table_key) {
			*entry.value().data.write() = OrderedMap::new();
		}
		Ok(())
	}
}

impl TierBackend for MemoryPrimitiveStorage {}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::TableId as CoreTableId;
	use reifydb_core::runtime::ComputePool;

	use super::*;

	fn test_compute_pool() -> ComputePool {
		ComputePool::new(2, 8)
	}

	#[test]
	fn test_basic_operations() {
		let storage = MemoryPrimitiveStorage::new(test_compute_pool());

		// Put and get
		storage.set(HashMap::from([(
			EntryKind::Multi,
			vec![(CowVec::new(b"key1".to_vec()), Some(CowVec::new(b"value1".to_vec())))],
		)]))
		.unwrap();
		let value = storage.get(EntryKind::Multi, b"key1").unwrap();
		assert_eq!(value.as_deref(), Some(b"value1".as_slice()));

		// Contains
		assert!(storage.contains(EntryKind::Multi, b"key1").unwrap());
		assert!(!storage.contains(EntryKind::Multi, b"nonexistent").unwrap());

		// Delete (tombstone)
		storage.set(HashMap::from([(EntryKind::Multi, vec![(CowVec::new(b"key1".to_vec()), None)])])).unwrap();
		assert!(!storage.contains(EntryKind::Multi, b"key1").unwrap());
	}

	#[test]
	fn test_source_tables() {
		use reifydb_core::interface::PrimitiveId;

		let storage = MemoryPrimitiveStorage::new(test_compute_pool());

		let source1 = PrimitiveId::Table(CoreTableId(1));
		let source2 = PrimitiveId::Table(CoreTableId(2));

		storage.set(HashMap::from([(
			EntryKind::Source(source1),
			vec![(CowVec::new(b"key".to_vec()), Some(CowVec::new(b"table1".to_vec())))],
		)]))
		.unwrap();
		storage.set(HashMap::from([(
			EntryKind::Source(source2),
			vec![(CowVec::new(b"key".to_vec()), Some(CowVec::new(b"table2".to_vec())))],
		)]))
		.unwrap();

		assert_eq!(
			storage.get(EntryKind::Source(source1), b"key").unwrap().as_deref(),
			Some(b"table1".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Source(source2), b"key").unwrap().as_deref(),
			Some(b"table2".as_slice())
		);
	}

	#[test]
	fn test_range_next() {
		let storage = MemoryPrimitiveStorage::new(test_compute_pool());

		storage.set(HashMap::from([(
			EntryKind::Multi,
			vec![(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec())))],
		)]))
		.unwrap();
		storage.set(HashMap::from([(
			EntryKind::Multi,
			vec![(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec())))],
		)]))
		.unwrap();
		storage.set(HashMap::from([(
			EntryKind::Multi,
			vec![(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec())))],
		)]))
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 100)
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
		assert_eq!(&*batch.entries[0].key, b"a");
		assert_eq!(&*batch.entries[1].key, b"b");
		assert_eq!(&*batch.entries[2].key, b"c");
	}

	#[test]
	fn test_range_rev_next() {
		let storage = MemoryPrimitiveStorage::new(test_compute_pool());

		storage.set(HashMap::from([(
			EntryKind::Multi,
			vec![(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec())))],
		)]))
		.unwrap();
		storage.set(HashMap::from([(
			EntryKind::Multi,
			vec![(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec())))],
		)]))
		.unwrap();
		storage.set(HashMap::from([(
			EntryKind::Multi,
			vec![(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec())))],
		)]))
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_rev_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 100)
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
		assert_eq!(&*batch.entries[0].key, b"c");
		assert_eq!(&*batch.entries[1].key, b"b");
		assert_eq!(&*batch.entries[2].key, b"a");
	}

	#[test]
	fn test_range_streaming_pagination() {
		let storage = MemoryPrimitiveStorage::new(test_compute_pool());

		// Insert 10 entries
		for i in 0..10u8 {
			storage.set(HashMap::from([(
				EntryKind::Multi,
				vec![(CowVec::new(vec![i]), Some(CowVec::new(vec![i * 10])))],
			)]))
			.unwrap();
		}

		// Use a single cursor to stream through all entries
		let mut cursor = RangeCursor::new();

		// First batch of 3
		let batch1 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch1.entries[0].key, &[0]);
		assert_eq!(&*batch1.entries[2].key, &[2]);

		// Second batch of 3 - cursor automatically continues
		let batch2 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch2.entries[0].key, &[3]);
		assert_eq!(&*batch2.entries[2].key, &[5]);

		// Third batch of 3
		let batch3 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.unwrap();
		assert_eq!(batch3.entries.len(), 3);
		assert!(batch3.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch3.entries[0].key, &[6]);
		assert_eq!(&*batch3.entries[2].key, &[8]);

		// Fourth batch - only 1 entry remaining
		let batch4 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.unwrap();
		assert_eq!(batch4.entries.len(), 1);
		assert!(!batch4.has_more);
		assert!(cursor.exhausted);
		assert_eq!(&*batch4.entries[0].key, &[9]);

		// Fifth call - exhausted
		let batch5 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.unwrap();
		assert!(batch5.entries.is_empty());
	}

	#[test]
	fn test_range_reving_pagination() {
		let storage = MemoryPrimitiveStorage::new(test_compute_pool());

		// Insert 10 entries
		for i in 0..10u8 {
			storage.set(HashMap::from([(
				EntryKind::Multi,
				vec![(CowVec::new(vec![i]), Some(CowVec::new(vec![i * 10])))],
			)]))
			.unwrap();
		}

		// Use a single cursor to stream in reverse
		let mut cursor = RangeCursor::new();

		// First batch of 3 (reverse)
		let batch1 = storage
			.range_rev_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch1.entries[0].key, &[9]);
		assert_eq!(&*batch1.entries[2].key, &[7]);

		// Second batch
		let batch2 = storage
			.range_rev_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, 3)
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch2.entries[0].key, &[6]);
		assert_eq!(&*batch2.entries[2].key, &[4]);
	}
}
