// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! SQLite implementation of PrimitiveStorage.
//!
//! Uses SQLite tables for persistent key-value storage.

use std::{
	collections::HashSet,
	ops::Bound,
	sync::{Arc, Mutex, mpsc},
	thread,
};

use parking_lot::RwLock;
use reifydb_type::{Result, diagnostic::internal::internal, error};
use rusqlite::params;
use tracing::instrument;

use super::{
	DbPath, SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	iterator::{SqliteRangeIter, SqliteRangeRevIter},
	tables::table_id_to_name,
	writer::{WriteCommand, run_writer},
};
use crate::backend::primitive::{PrimitiveBackend, PrimitiveStorage, TableId};

/// SQLite-based primitive storage implementation.
///
/// Uses SQLite for persistent storage with a writer thread for writes
/// and a connection pool for reads.
#[derive(Clone)]
pub struct SqlitePrimitiveStorage {
	inner: Arc<SqlitePrimitiveStorageInner>,
}

struct SqlitePrimitiveStorageInner {
	/// Writer channel for async writes
	writer: mpsc::Sender<WriteCommand>,
	/// Writer thread handle
	writer_thread: Mutex<Option<thread::JoinHandle<()>>>,
	/// Reader connection
	reader: Arc<Mutex<rusqlite::Connection>>,
	/// Database path
	db_path: DbPath,
	/// Track which tables have been created
	created_tables: RwLock<HashSet<String>>,
}

impl Drop for SqlitePrimitiveStorageInner {
	fn drop(&mut self) {
		let _ = self.writer.send(WriteCommand::Shutdown);

		if let Some(handle) = self.writer_thread.lock().unwrap().take() {
			let _ = handle.join();
		}

		// Cleanup tmpfs files for in-memory databases
		if let DbPath::Tmpfs(path) = &self.db_path {
			let _ = std::fs::remove_file(path);
			let _ = std::fs::remove_file(format!("{}-wal", path.display()));
			let _ = std::fs::remove_file(format!("{}-shm", path.display()));
		}

		if let DbPath::Memory(path) = &self.db_path {
			let _ = std::fs::remove_file(path);
			let _ = std::fs::remove_file(format!("{}-wal", path.display()));
			let _ = std::fs::remove_file(format!("{}-shm", path.display()));
		}
	}
}

impl SqlitePrimitiveStorage {
	/// Create a new SQLite primitive storage with the given configuration.
	#[instrument(name = "store::sqlite::new", level = "info", skip(config), fields(
		db_path = ?config.path,
		page_size = config.page_size,
		journal_mode = %config.journal_mode.as_str()
	))]
	pub fn new(config: SqliteConfig) -> Self {
		let db_path = resolve_db_path(config.path);
		let flags = convert_flags(&config.flags);

		let conn = connect(&db_path, flags.clone()).expect("Failed to connect to database");

		// Configure SQLite pragmas
		conn.pragma_update(None, "page_size", config.page_size).unwrap();
		conn.pragma_update(None, "journal_mode", config.journal_mode.as_str()).unwrap();
		conn.pragma_update(None, "synchronous", config.synchronous_mode.as_str()).unwrap();
		conn.pragma_update(None, "temp_store", config.temp_store.as_str()).unwrap();
		conn.pragma_update(None, "auto_vacuum", "INCREMENTAL").unwrap();
		conn.pragma_update(None, "cache_size", -(config.cache_size as i32)).unwrap();
		conn.pragma_update(None, "wal_autocheckpoint", config.wal_autocheckpoint).unwrap();
		conn.pragma_update(None, "mmap_size", config.mmap_size as i64).unwrap();

		let (sender, receiver) = mpsc::channel();

		let writer_conn = connect(&db_path, flags.clone()).expect("Failed to connect to database");

		let writer_thread = thread::spawn(move || {
			run_writer(receiver, writer_conn);
		});

		let reader_conn = connect(&db_path, flags).expect("Failed to connect to database");

		Self {
			inner: Arc::new(SqlitePrimitiveStorageInner {
				writer: sender,
				writer_thread: Mutex::new(Some(writer_thread)),
				reader: Arc::new(Mutex::new(reader_conn)),
				db_path,
				created_tables: RwLock::new(HashSet::new()),
			}),
		}
	}

	/// Create an in-memory SQLite storage for testing.
	pub fn in_memory() -> Self {
		Self::new(SqliteConfig::in_memory())
	}
}

impl PrimitiveStorage for SqlitePrimitiveStorage {
	type RangeIter<'a> = SqliteRangeIter;
	type RangeRevIter<'a> = SqliteRangeRevIter;

	#[instrument(name = "store::sqlite::get", level = "trace", skip(self), fields(table = ?table, key_len = key.len()))]
	fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Vec<u8>>> {
		let table_name = table_id_to_name(table);

		// Check if table exists
		{
			let created = self.inner.created_tables.read();
			if !created.contains(&table_name) {
				return Ok(None);
			}
		}

		let conn = self.inner.reader.lock().unwrap();
		let result = conn.query_row(
			&format!("SELECT value FROM \"{}\" WHERE key = ?1", table_name),
			params![key],
			|row| row.get::<_, Option<Vec<u8>>>(0),
		);

		match result {
			Ok(value) => Ok(value),
			Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
			Err(e) => Err(error!(internal(format!("Failed to get: {}", e)))),
		}
	}

	#[instrument(name = "store::sqlite::contains", level = "trace", skip(self), fields(table = ?table, key_len = key.len()), ret)]
	fn contains(&self, table: TableId, key: &[u8]) -> Result<bool> {
		let table_name = table_id_to_name(table);

		// Check if table exists
		{
			let created = self.inner.created_tables.read();
			if !created.contains(&table_name) {
				return Ok(false);
			}
		}

		let conn = self.inner.reader.lock().unwrap();
		let result = conn.query_row(
			&format!("SELECT value IS NOT NULL FROM \"{}\" WHERE key = ?1", table_name),
			params![key],
			|row| row.get::<_, bool>(0),
		);

		match result {
			Ok(has_value) => Ok(has_value),
			Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
			Err(e) => Err(error!(internal(format!("Failed to check contains: {}", e)))),
		}
	}

	#[instrument(name = "store::sqlite::put", level = "debug", skip(self, entries), fields(table = ?table, entry_count = entries.len()))]
	fn put(&self, table: TableId, entries: &[(&[u8], Option<&[u8]>)]) -> Result<()> {
		let table_name = table_id_to_name(table);

		// Mark table as created
		{
			let mut created = self.inner.created_tables.write();
			created.insert(table_name.clone());
		}

		let (respond_to, receiver) = mpsc::channel();

		let owned_entries: Vec<(Vec<u8>, Option<Vec<u8>>)> =
			entries.iter().map(|(k, v)| (k.to_vec(), v.map(|v| v.to_vec()))).collect();

		self.inner
			.writer
			.send(WriteCommand::PutBatch {
				table_name,
				entries: owned_entries,
				respond_to,
			})
			.map_err(|_| error!(internal("Writer thread died")))?;

		receiver.recv().map_err(|_| error!(internal("Writer thread died")))?
	}

	fn range(
		&self,
		table: TableId,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<Self::RangeIter<'_>> {
		let table_name = table_id_to_name(table);

		// Check if table exists
		{
			let created = self.inner.created_tables.read();
			if !created.contains(&table_name) {
				return Ok(SqliteRangeIter {
					reader: self.inner.reader.clone(),
					table_name,
					end: Bound::Unbounded,
					batch_size,
					buffer: Vec::new(),
					pos: 0,
					exhausted: true,
				});
			}
		}

		// Convert end bound to owned
		let end_owned = match end {
			Bound::Included(v) => Bound::Included(v.to_vec()),
			Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
			Bound::Unbounded => Bound::Unbounded,
		};

		let mut iter = SqliteRangeIter {
			reader: self.inner.reader.clone(),
			table_name,
			end: end_owned,
			batch_size,
			buffer: Vec::new(),
			pos: 0,
			exhausted: false,
		};

		// Load initial batch
		iter.load_initial(start)?;

		Ok(iter)
	}

	fn range_rev(
		&self,
		table: TableId,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<Self::RangeRevIter<'_>> {
		let table_name = table_id_to_name(table);

		// Check if table exists
		{
			let created = self.inner.created_tables.read();
			if !created.contains(&table_name) {
				return Ok(SqliteRangeRevIter {
					reader: self.inner.reader.clone(),
					table_name,
					start: Bound::Unbounded,
					batch_size,
					buffer: Vec::new(),
					pos: 0,
					exhausted: true,
				});
			}
		}

		// Convert start bound to owned
		let start_owned = match start {
			Bound::Included(v) => Bound::Included(v.to_vec()),
			Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
			Bound::Unbounded => Bound::Unbounded,
		};

		let mut iter = SqliteRangeRevIter {
			reader: self.inner.reader.clone(),
			table_name,
			start: start_owned,
			batch_size,
			buffer: Vec::new(),
			pos: 0,
			exhausted: false,
		};

		// Load initial batch
		iter.load_initial(end)?;

		Ok(iter)
	}

	fn ensure_table(&self, table: TableId) -> Result<()> {
		let table_name = table_id_to_name(table);

		// Check if already created
		{
			let created = self.inner.created_tables.read();
			if created.contains(&table_name) {
				return Ok(());
			}
		}

		let (respond_to, receiver) = mpsc::channel();

		self.inner
			.writer
			.send(WriteCommand::EnsureTable {
				table_name: table_name.clone(),
				respond_to,
			})
			.map_err(|_| error!(internal("Writer thread died")))?;

		let result = receiver.recv().map_err(|_| error!(internal("Writer thread died")))?;

		if result.is_ok() {
			let mut created = self.inner.created_tables.write();
			created.insert(table_name);
		}

		result
	}

	fn clear_table(&self, table: TableId) -> Result<()> {
		let table_name = table_id_to_name(table);

		let (respond_to, receiver) = mpsc::channel();

		self.inner
			.writer
			.send(WriteCommand::ClearTable {
				table_name,
				respond_to,
			})
			.map_err(|_| error!(internal("Writer thread died")))?;

		receiver.recv().map_err(|_| error!(internal("Writer thread died")))?
	}
}

impl PrimitiveBackend for SqlitePrimitiveStorage {}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::TableId as CoreTableId;

	use super::*;

	#[test]
	fn test_basic_operations() {
		let storage = SqlitePrimitiveStorage::in_memory();

		// Put and get
		storage.put(TableId::Multi, &[(b"key1".as_slice(), Some(b"value1".as_slice()))]).unwrap();
		let value = storage.get(TableId::Multi, b"key1").unwrap();
		assert_eq!(value, Some(b"value1".to_vec()));

		// Contains
		assert!(storage.contains(TableId::Multi, b"key1").unwrap());
		assert!(!storage.contains(TableId::Multi, b"nonexistent").unwrap());

		// Delete (tombstone)
		storage.put(TableId::Multi, &[(b"key1".as_slice(), None)]).unwrap();
		assert!(!storage.contains(TableId::Multi, b"key1").unwrap());
	}

	#[test]
	fn test_separate_tables() {
		let storage = SqlitePrimitiveStorage::in_memory();

		storage.put(TableId::Multi, &[(b"key".as_slice(), Some(b"multi".as_slice()))]).unwrap();
		storage.put(TableId::Single, &[(b"key".as_slice(), Some(b"single".as_slice()))]).unwrap();

		assert_eq!(storage.get(TableId::Multi, b"key").unwrap(), Some(b"multi".to_vec()));
		assert_eq!(storage.get(TableId::Single, b"key").unwrap(), Some(b"single".to_vec()));
	}

	#[test]
	fn test_source_tables() {
		use reifydb_core::interface::SourceId;

		let storage = SqlitePrimitiveStorage::in_memory();

		let source1 = SourceId::Table(CoreTableId(1));
		let source2 = SourceId::Table(CoreTableId(2));

		storage.put(TableId::Source(source1), &[(b"key".as_slice(), Some(b"table1".as_slice()))]).unwrap();
		storage.put(TableId::Source(source2), &[(b"key".as_slice(), Some(b"table2".as_slice()))]).unwrap();

		assert_eq!(storage.get(TableId::Source(source1), b"key").unwrap(), Some(b"table1".to_vec()));
		assert_eq!(storage.get(TableId::Source(source2), b"key").unwrap(), Some(b"table2".to_vec()));
	}

	#[test]
	fn test_range_iteration() {
		let storage = SqlitePrimitiveStorage::in_memory();

		storage.put(TableId::Multi, &[(b"a".as_slice(), Some(b"1".as_slice()))]).unwrap();
		storage.put(TableId::Multi, &[(b"b".as_slice(), Some(b"2".as_slice()))]).unwrap();
		storage.put(TableId::Multi, &[(b"c".as_slice(), Some(b"3".as_slice()))]).unwrap();

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
		let storage = SqlitePrimitiveStorage::in_memory();

		storage.put(TableId::Multi, &[(b"a".as_slice(), Some(b"1".as_slice()))]).unwrap();
		storage.put(TableId::Multi, &[(b"b".as_slice(), Some(b"2".as_slice()))]).unwrap();
		storage.put(TableId::Multi, &[(b"c".as_slice(), Some(b"3".as_slice()))]).unwrap();

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
		let storage = SqlitePrimitiveStorage::in_memory();

		// Insert 10 entries
		for i in 0..10u8 {
			storage.put(TableId::Multi, &[(&[i][..], Some(&[i * 10][..]))]).unwrap();
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
		let storage = SqlitePrimitiveStorage::in_memory();

		// Insert 10 entries
		for i in 0..10u8 {
			storage.put(TableId::Multi, &[(&[i][..], Some(&[i * 10][..]))]).unwrap();
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
