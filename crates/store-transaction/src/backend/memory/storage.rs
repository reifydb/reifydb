// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Memory implementation of PrimitiveStorage.
//!
//! Uses BTreeMap for ordered key-value storage with RwLock for thread safety.

use std::{
	ops::Bound,
	sync::{Arc, mpsc},
	thread,
};

use parking_lot::RwLock;
use reifydb_type::{Result, diagnostic::internal::internal, error};
use tracing::instrument;

use super::{
	iterator::{MemoryRangeIter, MemoryRangeRevIter},
	tables::Tables,
	writer::{WriteCommand, run_writer},
};
use crate::backend::primitive::{PrimitiveBackend, PrimitiveStorage, TableId};

/// Memory-based primitive storage implementation.
///
/// Stores data in BTreeMaps with RwLock for concurrent access.
/// Uses a background writer thread for batched writes.
#[derive(Clone)]
pub struct MemoryPrimitiveStorage {
	inner: Arc<MemoryPrimitiveStorageInner>,
}

struct MemoryPrimitiveStorageInner {
	/// Storage for each table type
	tables: Arc<RwLock<Tables>>,
	/// Writer channel for async writes
	writer: mpsc::Sender<WriteCommand>,
}

impl Drop for MemoryPrimitiveStorageInner {
	fn drop(&mut self) {
		let _ = self.writer.send(WriteCommand::Shutdown);
	}
}

impl Default for MemoryPrimitiveStorage {
	fn default() -> Self {
		Self::new()
	}
}

impl MemoryPrimitiveStorage {
	#[instrument(level = "debug", name = "MemoryPrimitiveStorage::new")]
	pub fn new() -> Self {
		let tables = Arc::new(RwLock::new(Tables::default()));

		let (sender, receiver) = mpsc::channel();

		// Clone for the writer thread
		let writer_tables = tables.clone();

		thread::spawn(move || {
			run_writer(receiver, writer_tables);
		});

		Self {
			inner: Arc::new(MemoryPrimitiveStorageInner {
				tables,
				writer: sender,
			}),
		}
	}
}

impl PrimitiveStorage for MemoryPrimitiveStorage {
	type RangeIter<'a> = MemoryRangeIter;
	type RangeRevIter<'a> = MemoryRangeRevIter;

	#[instrument(level = "trace", skip(self, key), fields(table = ?table, key_len = key.len()))]
	fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Vec<u8>>> {
		let tables = self.inner.tables.read();
		if let Some(table_data) = tables.get_table(table) {
			Ok(table_data.get(key).cloned().flatten())
		} else {
			Ok(None)
		}
	}

	#[instrument(level = "trace", skip(self, key), fields(table = ?table, key_len = key.len()), ret)]
	fn contains(&self, table: TableId, key: &[u8]) -> Result<bool> {
		let tables = self.inner.tables.read();
		if let Some(table_data) = tables.get_table(table) {
			// Key exists and is not a tombstone
			Ok(table_data.get(key).map_or(false, |v| v.is_some()))
		} else {
			Ok(false)
		}
	}

	#[instrument(level = "debug", skip(self, entries), fields(table = ?table, entry_count = entries.len()))]
	fn put_batch(&self, table: TableId, entries: &[(&[u8], Option<&[u8]>)]) -> Result<()> {
		let (respond_to, receiver) = mpsc::channel();

		let owned_entries: Vec<(Vec<u8>, Option<Vec<u8>>)> =
			entries.iter().map(|(k, v)| (k.to_vec(), v.map(|v| v.to_vec()))).collect();

		self.inner
			.writer
			.send(WriteCommand::PutBatch {
				table,
				entries: owned_entries,
				respond_to,
			})
			.map_err(|_| error!(internal("Writer thread died")))?;

		receiver.recv().map_err(|_| error!(internal("Writer thread died")))?
	}

	#[instrument(level = "trace", skip(self, start, end), fields(table = ?table, batch_size = batch_size))]
	fn range(
		&self,
		table: TableId,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<Self::RangeIter<'_>> {
		// Convert end bound to owned
		let end_owned = match end {
			Bound::Included(v) => Bound::Included(v.to_vec()),
			Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
			Bound::Unbounded => Bound::Unbounded,
		};

		let mut iter = MemoryRangeIter {
			tables: self.inner.tables.clone(),
			table,
			end: end_owned,
			batch_size,
			buffer: Vec::new(),
			pos: 0,
			exhausted: false,
		};

		// Load initial batch
		iter.load_initial(start);

		Ok(iter)
	}

	#[instrument(level = "trace", skip(self, start, end), fields(table = ?table, batch_size = batch_size))]
	fn range_rev(
		&self,
		table: TableId,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<Self::RangeRevIter<'_>> {
		// Convert start bound to owned
		let start_owned = match start {
			Bound::Included(v) => Bound::Included(v.to_vec()),
			Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
			Bound::Unbounded => Bound::Unbounded,
		};

		let mut iter = MemoryRangeRevIter {
			tables: self.inner.tables.clone(),
			table,
			start: start_owned,
			batch_size,
			buffer: Vec::new(),
			pos: 0,
			exhausted: false,
		};

		// Load initial batch
		iter.load_initial(end);

		Ok(iter)
	}

	#[instrument(level = "trace", skip(self), fields(table = ?table))]
	fn ensure_table(&self, table: TableId) -> Result<()> {
		// For memory backend, tables are created on-demand, so this is a no-op
		let mut tables = self.inner.tables.write();
		let _ = tables.get_table_mut(table);
		Ok(())
	}

	#[instrument(level = "debug", skip(self), fields(table = ?table))]
	fn clear_table(&self, table: TableId) -> Result<()> {
		let (respond_to, receiver) = mpsc::channel();

		self.inner
			.writer
			.send(WriteCommand::ClearTable {
				table,
				respond_to,
			})
			.map_err(|_| error!(internal("Writer thread died")))?;

		receiver.recv().map_err(|_| error!(internal("Writer thread died")))?
	}
}

impl PrimitiveBackend for MemoryPrimitiveStorage {}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::TableId as CoreTableId;

	use super::*;

	#[test]
	fn test_basic_operations() {
		let storage = MemoryPrimitiveStorage::new();

		// Put and get
		storage.put(TableId::Multi, b"key1", Some(b"value1")).unwrap();
		let value = storage.get(TableId::Multi, b"key1").unwrap();
		assert_eq!(value, Some(b"value1".to_vec()));

		// Contains
		assert!(storage.contains(TableId::Multi, b"key1").unwrap());
		assert!(!storage.contains(TableId::Multi, b"nonexistent").unwrap());

		// Delete (tombstone)
		storage.put(TableId::Multi, b"key1", None).unwrap();
		assert!(!storage.contains(TableId::Multi, b"key1").unwrap());
	}

	#[test]
	fn test_separate_tables() {
		let storage = MemoryPrimitiveStorage::new();

		storage.put(TableId::Multi, b"key", Some(b"multi")).unwrap();
		storage.put(TableId::Single, b"key", Some(b"single")).unwrap();

		assert_eq!(storage.get(TableId::Multi, b"key").unwrap(), Some(b"multi".to_vec()));
		assert_eq!(storage.get(TableId::Single, b"key").unwrap(), Some(b"single".to_vec()));
	}

	#[test]
	fn test_source_tables() {
		use reifydb_core::interface::SourceId;

		let storage = MemoryPrimitiveStorage::new();

		let source1 = SourceId::Table(CoreTableId(1));
		let source2 = SourceId::Table(CoreTableId(2));

		storage.put(TableId::Source(source1), b"key", Some(b"table1")).unwrap();
		storage.put(TableId::Source(source2), b"key", Some(b"table2")).unwrap();

		assert_eq!(storage.get(TableId::Source(source1), b"key").unwrap(), Some(b"table1".to_vec()));
		assert_eq!(storage.get(TableId::Source(source2), b"key").unwrap(), Some(b"table2".to_vec()));
	}

	#[test]
	fn test_range_iteration() {
		let storage = MemoryPrimitiveStorage::new();

		storage.put(TableId::Multi, b"a", Some(b"1")).unwrap();
		storage.put(TableId::Multi, b"b", Some(b"2")).unwrap();
		storage.put(TableId::Multi, b"c", Some(b"3")).unwrap();

		let entries: Vec<_> = storage
			.range(TableId::Multi, Bound::Unbounded, Bound::Unbounded, 100)
			.unwrap()
			.collect::<Result<Vec<_>>>()
			.unwrap();

		assert_eq!(entries.len(), 3);
		assert_eq!(entries[0].key, b"a".to_vec());
		assert_eq!(entries[1].key, b"b".to_vec());
		assert_eq!(entries[2].key, b"c".to_vec());
	}

	#[test]
	fn test_range_reverse_iteration() {
		let storage = MemoryPrimitiveStorage::new();

		storage.put(TableId::Multi, b"a", Some(b"1")).unwrap();
		storage.put(TableId::Multi, b"b", Some(b"2")).unwrap();
		storage.put(TableId::Multi, b"c", Some(b"3")).unwrap();

		let entries: Vec<_> = storage
			.range_rev(TableId::Multi, Bound::Unbounded, Bound::Unbounded, 100)
			.unwrap()
			.collect::<Result<Vec<_>>>()
			.unwrap();

		assert_eq!(entries.len(), 3);
		assert_eq!(entries[0].key, b"c".to_vec());
		assert_eq!(entries[1].key, b"b".to_vec());
		assert_eq!(entries[2].key, b"a".to_vec());
	}

	#[test]
	fn test_range_lazy_pagination() {
		let storage = MemoryPrimitiveStorage::new();

		// Insert 10 entries
		for i in 0..10u8 {
			storage.put(TableId::Multi, &[i], Some(&[i * 10])).unwrap();
		}

		// Use batch_size of 3, which should require 4 batches (3+3+3+1)
		let entries: Vec<_> = storage
			.range(TableId::Multi, Bound::Unbounded, Bound::Unbounded, 3)
			.unwrap()
			.collect::<Result<Vec<_>>>()
			.unwrap();

		assert_eq!(entries.len(), 10);
		for (i, entry) in entries.iter().enumerate() {
			assert_eq!(entry.key, vec![i as u8]);
			assert_eq!(entry.value, Some(vec![(i * 10) as u8]));
		}
	}

	#[test]
	fn test_range_rev_lazy_pagination() {
		let storage = MemoryPrimitiveStorage::new();

		// Insert 10 entries
		for i in 0..10u8 {
			storage.put(TableId::Multi, &[i], Some(&[i * 10])).unwrap();
		}

		// Use batch_size of 3, which should require 4 batches (3+3+3+1)
		let entries: Vec<_> = storage
			.range_rev(TableId::Multi, Bound::Unbounded, Bound::Unbounded, 3)
			.unwrap()
			.collect::<Result<Vec<_>>>()
			.unwrap();

		assert_eq!(entries.len(), 10);
		for (i, entry) in entries.iter().enumerate() {
			// Reverse order: 9, 8, 7, ...
			let expected_key = (9 - i) as u8;
			assert_eq!(entry.key, vec![expected_key]);
			assert_eq!(entry.value, Some(vec![expected_key * 10]));
		}
	}
}
