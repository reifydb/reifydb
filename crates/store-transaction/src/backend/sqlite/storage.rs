// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! SQLite implementation of PrimitiveStorage.
//!
//! Uses SQLite tables for persistent key-value storage.

use std::{collections::HashSet, ops::Bound, sync::Arc};

use async_trait::async_trait;
use parking_lot::RwLock;
use reifydb_type::{Result, diagnostic::internal::internal, error};
use rusqlite::params;
use tokio::{sync::oneshot, task::spawn_blocking};
use tracing::instrument;

use super::{
	DbPath, SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	query::build_range_query,
	tables::table_id_to_name,
	writer::{WriteCommand, WriterSender, spawn_writer},
};
use crate::backend::primitive::{PrimitiveBackend, PrimitiveStorage, RangeBatch, RawEntry, TableId};

/// SQLite-based primitive storage implementation.
///
/// Uses SQLite for persistent storage with a writer thread for writes
/// and spawn_blocking for reads.
#[derive(Clone)]
pub struct SqlitePrimitiveStorage {
	inner: Arc<SqlitePrimitiveStorageInner>,
}

struct SqlitePrimitiveStorageInner {
	/// Writer channel for async writes (uses std channel since writer thread is sync)
	writer: WriterSender,
	/// Reader connection wrapped in mutex for spawn_blocking
	reader: Arc<std::sync::Mutex<rusqlite::Connection>>,
	/// Database path
	db_path: DbPath,
	/// Track which tables have been created
	created_tables: RwLock<HashSet<String>>,
}

impl Drop for SqlitePrimitiveStorageInner {
	fn drop(&mut self) {
		let _ = self.writer.send(WriteCommand::Shutdown);

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

		// Create writer connection and spawn writer thread
		let writer_conn = connect(&db_path, flags.clone()).expect("Failed to connect to database");
		let writer = spawn_writer(writer_conn);

		// Create reader connection
		let reader_conn = connect(&db_path, flags).expect("Failed to connect to database");

		Self {
			inner: Arc::new(SqlitePrimitiveStorageInner {
				writer,
				reader: Arc::new(std::sync::Mutex::new(reader_conn)),
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

#[async_trait]
impl PrimitiveStorage for SqlitePrimitiveStorage {
	#[instrument(name = "store::sqlite::get", level = "trace", skip(self), fields(table = ?table, key_len = key.len()))]
	async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Vec<u8>>> {
		let table_name = table_id_to_name(table);

		// Check if table exists
		{
			let created = self.inner.created_tables.read();
			if !created.contains(&table_name) {
				return Ok(None);
			}
		}

		let reader = self.inner.reader.clone();
		let key = key.to_vec();

		spawn_blocking(move || {
			let conn = reader.lock().unwrap();
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
		})
		.await
		.map_err(|e| error!(internal(format!("spawn_blocking failed: {}", e))))?
	}

	#[instrument(name = "store::sqlite::contains", level = "trace", skip(self), fields(table = ?table, key_len = key.len()), ret)]
	async fn contains(&self, table: TableId, key: &[u8]) -> Result<bool> {
		let table_name = table_id_to_name(table);

		// Check if table exists
		{
			let created = self.inner.created_tables.read();
			if !created.contains(&table_name) {
				return Ok(false);
			}
		}

		let reader = self.inner.reader.clone();
		let key = key.to_vec();

		spawn_blocking(move || {
			let conn = reader.lock().unwrap();
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
		})
		.await
		.map_err(|e| error!(internal(format!("spawn_blocking failed: {}", e))))?
	}

	#[instrument(name = "store::sqlite::put", level = "debug", skip(self, entries), fields(table = ?table, entry_count = entries.len()))]
	async fn put(&self, table: TableId, entries: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> Result<()> {
		let table_name = table_id_to_name(table);

		// Mark table as created
		{
			let mut created = self.inner.created_tables.write();
			created.insert(table_name.clone());
		}

		let (respond_to, receiver) = oneshot::channel();

		self.inner
			.writer
			.send(WriteCommand::PutBatch {
				table_name,
				entries,
				respond_to,
			})
			.map_err(|_| error!(internal("Writer thread died")))?;

		receiver.await.map_err(|_| error!(internal("Writer thread died")))?
	}

	#[instrument(name = "store::sqlite::range_batch", level = "trace", skip(self, start, end), fields(table = ?table, batch_size = batch_size))]
	async fn range_batch(
		&self,
		table: TableId,
		start: Bound<Vec<u8>>,
		end: Bound<Vec<u8>>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		let table_name = table_id_to_name(table);

		// Check if table exists
		{
			let created = self.inner.created_tables.read();
			if !created.contains(&table_name) {
				return Ok(RangeBatch::empty());
			}
		}

		let reader = self.inner.reader.clone();

		spawn_blocking(move || {
			let conn = reader.lock().unwrap();

			// Build query with limit + 1 to detect has_more
			let start_ref = bound_as_ref(&start);
			let end_ref = bound_as_ref(&end);
			let (query, params) = build_range_query(&table_name, start_ref, end_ref, false, batch_size + 1);

			let mut stmt = conn
				.prepare(&query)
				.map_err(|e| error!(internal(format!("Failed to prepare: {}", e))))?;

			let params_refs: Vec<&dyn rusqlite::ToSql> =
				params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

			let entries: Vec<RawEntry> = stmt
				.query_map(params_refs.as_slice(), |row| {
					Ok(RawEntry {
						key: row.get(0)?,
						value: row.get(1)?,
					})
				})
				.map_err(|e| error!(internal(format!("Failed to query: {}", e))))?
				.filter_map(|r| r.ok())
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
		})
		.await
		.map_err(|e| error!(internal(format!("spawn_blocking failed: {}", e))))?
	}

	#[instrument(name = "store::sqlite::range_rev_batch", level = "trace", skip(self, start, end), fields(table = ?table, batch_size = batch_size))]
	async fn range_rev_batch(
		&self,
		table: TableId,
		start: Bound<Vec<u8>>,
		end: Bound<Vec<u8>>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		let table_name = table_id_to_name(table);

		// Check if table exists
		{
			let created = self.inner.created_tables.read();
			if !created.contains(&table_name) {
				return Ok(RangeBatch::empty());
			}
		}

		let reader = self.inner.reader.clone();

		spawn_blocking(move || {
			let conn = reader.lock().unwrap();

			// Build query with limit + 1 to detect has_more
			let start_ref = bound_as_ref(&start);
			let end_ref = bound_as_ref(&end);
			let (query, params) = build_range_query(&table_name, start_ref, end_ref, true, batch_size + 1);

			let mut stmt = conn
				.prepare(&query)
				.map_err(|e| error!(internal(format!("Failed to prepare: {}", e))))?;

			let params_refs: Vec<&dyn rusqlite::ToSql> =
				params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

			let entries: Vec<RawEntry> = stmt
				.query_map(params_refs.as_slice(), |row| {
					Ok(RawEntry {
						key: row.get(0)?,
						value: row.get(1)?,
					})
				})
				.map_err(|e| error!(internal(format!("Failed to query: {}", e))))?
				.filter_map(|r| r.ok())
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
		})
		.await
		.map_err(|e| error!(internal(format!("spawn_blocking failed: {}", e))))?
	}

	async fn ensure_table(&self, table: TableId) -> Result<()> {
		let table_name = table_id_to_name(table);

		// Check if already created
		{
			let created = self.inner.created_tables.read();
			if created.contains(&table_name) {
				return Ok(());
			}
		}

		let (respond_to, receiver) = oneshot::channel();

		self.inner
			.writer
			.send(WriteCommand::EnsureTable {
				table_name: table_name.clone(),
				respond_to,
			})
			.map_err(|_| error!(internal("Writer thread died")))?;

		let result = receiver.await.map_err(|_| error!(internal("Writer thread died")))?;

		if result.is_ok() {
			let mut created = self.inner.created_tables.write();
			created.insert(table_name);
		}

		result
	}

	async fn clear_table(&self, table: TableId) -> Result<()> {
		let table_name = table_id_to_name(table);

		let (respond_to, receiver) = oneshot::channel();

		self.inner
			.writer
			.send(WriteCommand::ClearTable {
				table_name,
				respond_to,
			})
			.map_err(|_| error!(internal("Writer thread died")))?;

		receiver.await.map_err(|_| error!(internal("Writer thread died")))?
	}
}

impl PrimitiveBackend for SqlitePrimitiveStorage {}

/// Convert owned Bound to Bound<&[u8]>
fn bound_as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
	match bound {
		Bound::Included(v) => Bound::Included(v.as_slice()),
		Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::TableId as CoreTableId;

	use super::*;

	#[tokio::test]
	async fn test_basic_operations() {
		let storage = SqlitePrimitiveStorage::in_memory();

		// Put and get
		storage.put(TableId::Multi, vec![(b"key1".to_vec(), Some(b"value1".to_vec()))]).await.unwrap();
		let value = storage.get(TableId::Multi, b"key1").await.unwrap();
		assert_eq!(value, Some(b"value1".to_vec()));

		// Contains
		assert!(storage.contains(TableId::Multi, b"key1").await.unwrap());
		assert!(!storage.contains(TableId::Multi, b"nonexistent").await.unwrap());

		// Delete (tombstone)
		storage.put(TableId::Multi, vec![(b"key1".to_vec(), None)]).await.unwrap();
		assert!(!storage.contains(TableId::Multi, b"key1").await.unwrap());
	}

	#[tokio::test]
	async fn test_separate_tables() {
		let storage = SqlitePrimitiveStorage::in_memory();

		storage.put(TableId::Multi, vec![(b"key".to_vec(), Some(b"multi".to_vec()))]).await.unwrap();
		storage.put(TableId::Single, vec![(b"key".to_vec(), Some(b"single".to_vec()))]).await.unwrap();

		assert_eq!(storage.get(TableId::Multi, b"key").await.unwrap(), Some(b"multi".to_vec()));
		assert_eq!(storage.get(TableId::Single, b"key").await.unwrap(), Some(b"single".to_vec()));
	}

	#[tokio::test]
	async fn test_source_tables() {
		use reifydb_core::interface::SourceId;

		let storage = SqlitePrimitiveStorage::in_memory();

		let source1 = SourceId::Table(CoreTableId(1));
		let source2 = SourceId::Table(CoreTableId(2));

		storage.put(TableId::Source(source1), vec![(b"key".to_vec(), Some(b"table1".to_vec()))]).await.unwrap();
		storage.put(TableId::Source(source2), vec![(b"key".to_vec(), Some(b"table2".to_vec()))]).await.unwrap();

		assert_eq!(storage.get(TableId::Source(source1), b"key").await.unwrap(), Some(b"table1".to_vec()));
		assert_eq!(storage.get(TableId::Source(source2), b"key").await.unwrap(), Some(b"table2".to_vec()));
	}

	#[tokio::test]
	async fn test_range_batch() {
		let storage = SqlitePrimitiveStorage::in_memory();

		storage.put(TableId::Multi, vec![(b"a".to_vec(), Some(b"1".to_vec()))]).await.unwrap();
		storage.put(TableId::Multi, vec![(b"b".to_vec(), Some(b"2".to_vec()))]).await.unwrap();
		storage.put(TableId::Multi, vec![(b"c".to_vec(), Some(b"3".to_vec()))]).await.unwrap();

		let batch = storage.range_batch(TableId::Multi, Bound::Unbounded, Bound::Unbounded, 100).await.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert_eq!(batch.entries[0].key, b"a".to_vec());
		assert_eq!(batch.entries[1].key, b"b".to_vec());
		assert_eq!(batch.entries[2].key, b"c".to_vec());
	}

	#[tokio::test]
	async fn test_range_rev_batch() {
		let storage = SqlitePrimitiveStorage::in_memory();

		storage.put(TableId::Multi, vec![(b"a".to_vec(), Some(b"1".to_vec()))]).await.unwrap();
		storage.put(TableId::Multi, vec![(b"b".to_vec(), Some(b"2".to_vec()))]).await.unwrap();
		storage.put(TableId::Multi, vec![(b"c".to_vec(), Some(b"3".to_vec()))]).await.unwrap();

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
		let storage = SqlitePrimitiveStorage::in_memory();

		// Insert 10 entries
		for i in 0..10u8 {
			storage.put(TableId::Multi, vec![(vec![i], Some(vec![i * 10]))]).await.unwrap();
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
		let storage = SqlitePrimitiveStorage::in_memory();

		// Insert 10 entries
		for i in 0..10u8 {
			storage.put(TableId::Multi, vec![(vec![i], Some(vec![i * 10]))]).await.unwrap();
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
