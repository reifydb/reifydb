// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! SQLite implementation of PrimitiveStorage.
//!
//! Uses SQLite tables for persistent key-value storage.
//! All operations use a single connection protected by RwLock for thread safety,
//! matching the memory backend's synchronization pattern.

use std::{collections::HashMap, ops::Bound, sync::Arc};

use async_trait::async_trait;
use reifydb_type::{Result, diagnostic::internal::internal, error};
use rusqlite::{Error::QueryReturnedNoRows, params};
use tokio::sync::RwLock;
use tokio_rusqlite::Connection;
use tracing::instrument;

use super::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	query::build_range_query,
	tables::table_id_to_name,
};
use crate::backend::primitive::{PrimitiveBackend, PrimitiveStorage, RangeBatch, RawEntry, TableId};

/// SQLite-based primitive storage implementation.
///
/// Uses SQLite for persistent storage with a single connection protected by RwLock.
/// This matches the memory backend's synchronization pattern, ensuring writes
/// are immediately visible to subsequent reads.
#[derive(Clone)]
pub struct SqlitePrimitiveStorage {
	inner: Arc<SqlitePrimitiveStorageInner>,
}

struct SqlitePrimitiveStorageInner {
	/// Single connection protected by RwLock for thread-safe access
	conn: RwLock<Connection>,
}

impl SqlitePrimitiveStorage {
	/// Create a new SQLite primitive storage with the given configuration.
	#[instrument(name = "store::sqlite::new", level = "info", skip(config), fields(
		db_path = ?config.path,
		page_size = config.page_size,
		journal_mode = %config.journal_mode.as_str()
	))]
	pub async fn new(config: SqliteConfig) -> Self {
		let db_path = resolve_db_path(config.path);
		let flags = convert_flags(&config.flags);

		let conn = connect(&db_path, flags).await.expect("Failed to connect to database");

		// Configure SQLite pragmas
		let page_size = config.page_size;
		let journal_mode = config.journal_mode.as_str().to_string();
		let synchronous_mode = config.synchronous_mode.as_str().to_string();
		let temp_store = config.temp_store.as_str().to_string();
		let cache_size = config.cache_size;
		let wal_autocheckpoint = config.wal_autocheckpoint;
		let mmap_size = config.mmap_size;

		conn.call(move |conn| -> rusqlite::Result<()> {
			conn.pragma_update(None, "page_size", page_size)?;
			conn.pragma_update(None, "journal_mode", &journal_mode)?;
			conn.pragma_update(None, "synchronous", &synchronous_mode)?;
			conn.pragma_update(None, "temp_store", &temp_store)?;
			conn.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
			conn.pragma_update(None, "cache_size", -(cache_size as i32))?;
			conn.pragma_update(None, "wal_autocheckpoint", wal_autocheckpoint)?;
			conn.pragma_update(None, "mmap_size", mmap_size as i64)?;
			Ok(())
		})
		.await
		.expect("Failed to configure database");

		Self {
			inner: Arc::new(SqlitePrimitiveStorageInner {
				conn: RwLock::new(conn),
			}),
		}
	}

	/// Create an in-memory SQLite storage for testing.
	pub async fn in_memory() -> Self {
		Self::new(SqliteConfig::in_memory()).await
	}
}

#[async_trait]
impl PrimitiveStorage for SqlitePrimitiveStorage {
	#[instrument(name = "store::sqlite::get", level = "trace", skip(self), fields(table = ?table, key_len = key.len()))]
	async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Vec<u8>>> {
		let table_name = table_id_to_name(table);
		let key = key.to_vec();

		let conn = self.inner.conn.read().await;
		let result = conn
			.call(move |conn| -> rusqlite::Result<Option<Vec<u8>>> {
				let result = conn.query_row(
					&format!("SELECT value FROM \"{}\" WHERE key = ?1", table_name),
					params![key],
					|row| row.get::<_, Option<Vec<u8>>>(0),
				);

				match result {
					Ok(value) => Ok(value),
					Err(QueryReturnedNoRows) => Ok(None),
					Err(e) => Err(e),
				}
			})
			.await;

		match result {
			Ok(value) => Ok(value),
			Err(e) if e.to_string().contains("no such table") => Ok(None),
			Err(e) => Err(error!(internal(format!("Failed to get: {}", e)))),
		}
	}

	#[instrument(name = "store::sqlite::contains", level = "trace", skip(self), fields(table = ?table, key_len = key.len()), ret)]
	async fn contains(&self, table: TableId, key: &[u8]) -> Result<bool> {
		let table_name = table_id_to_name(table);
		let key = key.to_vec();

		let conn = self.inner.conn.read().await;
		let result = conn
			.call(move |conn| -> rusqlite::Result<bool> {
				let result = conn.query_row(
					&format!("SELECT value IS NOT NULL FROM \"{}\" WHERE key = ?1", table_name),
					params![key],
					|row| row.get::<_, bool>(0),
				);

				match result {
					Ok(has_value) => Ok(has_value),
					Err(QueryReturnedNoRows) => Ok(false),
					Err(e) => Err(e),
				}
			})
			.await;

		match result {
			Ok(has_value) => Ok(has_value),
			Err(e) if e.to_string().contains("no such table") => Ok(false),
			Err(e) => Err(error!(internal(format!("Failed to check contains: {}", e)))),
		}
	}

	#[instrument(name = "store::sqlite::set", level = "debug", skip(self, batches), fields(table_count = batches.len()))]
	async fn set(&self, batches: HashMap<TableId, Vec<(Vec<u8>, Option<Vec<u8>>)>>) -> Result<()> {
		if batches.is_empty() {
			return Ok(());
		}

		// Convert TableId to table names before moving into closure
		let table_batches: Vec<(String, Vec<(Vec<u8>, Option<Vec<u8>>)>)> =
			batches.into_iter().map(|(table, entries)| (table_id_to_name(table), entries)).collect();

		let conn = self.inner.conn.write().await;
		conn.call(move |conn| {
			let tx = conn.unchecked_transaction()?;

			for (table_name, entries) in &table_batches {
				let result = insert_entries_in_tx(&tx, table_name, entries);
				if let Err(e) = result {
					if e.to_string().contains("no such table") {
						tx.execute(
							&format!(
								"CREATE TABLE IF NOT EXISTS \"{}\" (
									key BLOB NOT NULL PRIMARY KEY,
									value BLOB
								) WITHOUT ROWID",
								table_name
							),
							[],
						)?;
						insert_entries_in_tx(&tx, table_name, entries)?;
					} else {
						return Err(e);
					}
				}
			}

			tx.commit()?;

			Ok(())
		})
		.await
		.map_err(|e| error!(internal(format!("Failed to write_all: {}", e))))
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

		let conn = self.inner.conn.read().await;
		let result = conn
			.call(move |conn| -> rusqlite::Result<RangeBatch> {
				// Build query with limit + 1 to detect has_more
				let start_ref = bound_as_ref(&start);
				let end_ref = bound_as_ref(&end);
				let (query, params) =
					build_range_query(&table_name, start_ref, end_ref, false, batch_size + 1);

				let mut stmt = conn.prepare(&query)?;

				let params_refs: Vec<&dyn rusqlite::ToSql> =
					params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

				let entries: Vec<RawEntry> = stmt
					.query_map(params_refs.as_slice(), |row| {
						Ok(RawEntry {
							key: row.get(0)?,
							value: row.get(1)?,
						})
					})?
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
			.await;

		match result {
			Ok(batch) => Ok(batch),
			Err(e) if e.to_string().contains("no such table") => Ok(RangeBatch::empty()),
			Err(e) => Err(error!(internal(format!("Failed to query range: {}", e)))),
		}
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

		let conn = self.inner.conn.read().await;
		let result = conn
			.call(move |conn| -> rusqlite::Result<RangeBatch> {
				// Build query with limit + 1 to detect has_more
				let start_ref = bound_as_ref(&start);
				let end_ref = bound_as_ref(&end);
				let (query, params) =
					build_range_query(&table_name, start_ref, end_ref, true, batch_size + 1);

				let mut stmt = conn.prepare(&query)?;

				let params_refs: Vec<&dyn rusqlite::ToSql> =
					params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

				let entries: Vec<RawEntry> = stmt
					.query_map(params_refs.as_slice(), |row| {
						Ok(RawEntry {
							key: row.get(0)?,
							value: row.get(1)?,
						})
					})?
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
			.await;

		match result {
			Ok(batch) => Ok(batch),
			Err(e) if e.to_string().contains("no such table") => Ok(RangeBatch::empty()),
			Err(e) => Err(error!(internal(format!("Failed to query range: {}", e)))),
		}
	}

	async fn ensure_table(&self, table: TableId) -> Result<()> {
		let table_name = table_id_to_name(table);

		let conn = self.inner.conn.write().await;
		conn.call(move |conn| -> rusqlite::Result<()> {
			conn.execute(
				&format!(
					"CREATE TABLE IF NOT EXISTS \"{}\" (
						key   BLOB NOT NULL PRIMARY KEY,
						value BLOB
					) WITHOUT ROWID",
					table_name
				),
				[],
			)
			.map(|_| ())
		})
		.await
		.map_err(|e| error!(internal(format!("Failed to ensure table: {}", e))))
	}

	async fn clear_table(&self, table: TableId) -> Result<()> {
		let table_name = table_id_to_name(table);

		let conn = self.inner.conn.write().await;
		let result = conn
			.call(move |conn| -> rusqlite::Result<()> {
				conn.execute(&format!("DELETE FROM \"{}\"", table_name), []).map(|_| ())
			})
			.await;

		match result {
			Ok(()) => Ok(()),
			Err(e) if e.to_string().contains("no such table") => Ok(()),
			Err(e) => Err(error!(internal(format!("Failed to clear table: {}", e)))),
		}
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

/// Insert entries into a table within an existing transaction
fn insert_entries_in_tx(
	tx: &rusqlite::Transaction,
	table_name: &str,
	entries: &[(Vec<u8>, Option<Vec<u8>>)],
) -> rusqlite::Result<()> {
	for (key, value) in entries {
		tx.execute(
			&format!("INSERT OR REPLACE INTO \"{}\" (key, value) VALUES (?1, ?2)", table_name),
			params![key, value.as_deref()],
		)?;
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::TableId as CoreTableId;

	use super::*;

	#[tokio::test]
	async fn test_basic_operations() {
		let storage = SqlitePrimitiveStorage::in_memory().await;

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
		let storage = SqlitePrimitiveStorage::in_memory().await;

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

		let storage = SqlitePrimitiveStorage::in_memory().await;

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
		let storage = SqlitePrimitiveStorage::in_memory().await;

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
		let storage = SqlitePrimitiveStorage::in_memory().await;

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
		let storage = SqlitePrimitiveStorage::in_memory().await;

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
		let storage = SqlitePrimitiveStorage::in_memory().await;

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

	#[tokio::test]
	async fn test_get_nonexistent_table() {
		let storage = SqlitePrimitiveStorage::in_memory().await;

		// Should return None for non-existent table, not error
		let value = storage.get(TableId::Multi, b"key").await.unwrap();
		assert_eq!(value, None);
	}

	#[tokio::test]
	async fn test_range_nonexistent_table() {
		let storage = SqlitePrimitiveStorage::in_memory().await;

		// Should return empty batch for non-existent table, not error
		let batch = storage.range_batch(TableId::Multi, Bound::Unbounded, Bound::Unbounded, 100).await.unwrap();
		assert!(batch.entries.is_empty());
		assert!(!batch.has_more);
	}
}
