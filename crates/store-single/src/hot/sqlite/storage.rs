// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! SQLite implementation of single-version storage.
//!
//! Uses a single table for persistent key-value storage with thread-safe access.

use std::{ops::Bound, sync::Arc};

use parking_lot::Mutex;
use reifydb_type::{Result, error, error::diagnostic::internal::internal, util::cowvec::CowVec};
use rusqlite::{Connection, Error::QueryReturnedNoRows, params};
use tracing::instrument;

use super::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	query::build_range_query,
};
use crate::tier::{RangeBatch, RangeCursor, RawEntry, TierBackend, TierStorage};

/// Single table name for all storage
const TABLE_NAME: &str = "entries";

/// SQLite-based primitive storage implementation.
///
/// Uses a single table for persistent storage with a connection protected by Mutex.
#[derive(Clone)]
pub struct SqlitePrimitiveStorage {
	inner: Arc<SqlitePrimitiveStorageInner>,
}

struct SqlitePrimitiveStorageInner {
	/// Single connection protected by Mutex for thread-safe access.
	/// Note: We use Mutex instead of RwLock because rusqlite::Connection
	/// is Send but not Sync (contains RefCell).
	conn: Mutex<Connection>,
}

impl SqlitePrimitiveStorage {
	/// Create a new SQLite primitive storage with the given configuration.
	#[instrument(name = "store::single::sqlite::new", level = "debug", skip(config), fields(
		db_path = ?config.path,
		page_size = config.page_size,
		journal_mode = %config.journal_mode.as_str()
	))]
	pub fn new(config: SqliteConfig) -> Self {
		let db_path = resolve_db_path(config.path);
		let flags = convert_flags(&config.flags);

		let conn = connect(&db_path, flags).expect("Failed to connect to database");

		// Configure SQLite pragmas
		conn.pragma_update(None, "page_size", config.page_size).expect("Failed to set page_size");
		conn.pragma_update(None, "journal_mode", config.journal_mode.as_str())
			.expect("Failed to set journal_mode");
		conn.pragma_update(None, "synchronous", config.synchronous_mode.as_str())
			.expect("Failed to set synchronous");
		conn.pragma_update(None, "temp_store", config.temp_store.as_str()).expect("Failed to set temp_store");
		conn.pragma_update(None, "auto_vacuum", "INCREMENTAL").expect("Failed to set auto_vacuum");
		conn.pragma_update(None, "cache_size", -(config.cache_size as i32)).expect("Failed to set cache_size");
		conn.pragma_update(None, "wal_autocheckpoint", config.wal_autocheckpoint)
			.expect("Failed to set wal_autocheckpoint");
		conn.pragma_update(None, "mmap_size", config.mmap_size as i64).expect("Failed to set mmap_size");

		Self {
			inner: Arc::new(SqlitePrimitiveStorageInner {
				conn: Mutex::new(conn),
			}),
		}
	}

	/// Create an in-memory SQLite storage for testing.
	pub fn in_memory() -> Self {
		Self::new(SqliteConfig::in_memory())
	}
}

impl TierStorage for SqlitePrimitiveStorage {
	#[instrument(name = "store::single::sqlite::get", level = "trace", skip(self, key), fields(key_len = key.len()))]
	fn get(&self, key: &[u8]) -> Result<Option<CowVec<u8>>> {
		let conn = self.inner.conn.lock();

		let result = conn.query_row(
			&format!("SELECT value FROM \"{}\" WHERE key = ?1", TABLE_NAME),
			params![key],
			|row| row.get::<_, Option<Vec<u8>>>(0),
		);

		match result {
			Ok(Some(value)) => Ok(Some(CowVec::new(value))),
			Ok(None) => Ok(None),
			Err(QueryReturnedNoRows) => Ok(None),
			Err(e) if e.to_string().contains("no such table") => Ok(None),
			Err(e) => Err(error!(internal(format!("Failed to get: {}", e)))),
		}
	}

	#[instrument(name = "store::single::sqlite::contains", level = "trace", skip(self, key), fields(key_len = key.len()), ret)]
	fn contains(&self, key: &[u8]) -> Result<bool> {
		let conn = self.inner.conn.lock();

		let result = conn.query_row(
			&format!("SELECT value IS NOT NULL FROM \"{}\" WHERE key = ?1", TABLE_NAME),
			params![key],
			|row| row.get::<_, bool>(0),
		);

		match result {
			Ok(has_value) => Ok(has_value),
			Err(QueryReturnedNoRows) => Ok(false),
			Err(e) if e.to_string().contains("no such table") => Ok(false),
			Err(e) => Err(error!(internal(format!("Failed to check contains: {}", e)))),
		}
	}

	#[instrument(name = "store::single::sqlite::set", level = "debug", skip(self, entries), fields(entry_count = entries.len()))]
	fn set(&self, entries: Vec<(CowVec<u8>, Option<CowVec<u8>>)>) -> Result<()> {
		if entries.is_empty() {
			return Ok(());
		}

		let conn = self.inner.conn.lock();
		let tx = conn
			.unchecked_transaction()
			.map_err(|e| error!(internal(format!("Failed to start transaction: {}", e))))?;

		let result = insert_entries_in_tx(&tx, TABLE_NAME, &entries);
		if let Err(e) = result {
			if e.to_string().contains("no such table") {
				tx.execute(
					&format!(
						"CREATE TABLE IF NOT EXISTS \"{}\" (
							key BLOB NOT NULL PRIMARY KEY,
							value BLOB
						) WITHOUT ROWID",
						TABLE_NAME
					),
					[],
				)
				.map_err(|e| error!(internal(format!("Failed to create table: {}", e))))?;
				insert_entries_in_tx(&tx, TABLE_NAME, &entries)
					.map_err(|e| error!(internal(format!("Failed to insert entries: {}", e))))?;
			} else {
				return Err(error!(internal(format!("Failed to insert entries: {}", e))));
			}
		}

		tx.commit().map_err(|e| error!(internal(format!("Failed to commit transaction: {}", e))))
	}

	#[instrument(name = "store::single::sqlite::range_next", level = "trace", skip(self, cursor))]
	fn range_next(
		&self,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		// Determine effective start bound based on cursor state
		let effective_start: Bound<Vec<u8>> = match &cursor.last_key {
			Some(last) => Bound::Excluded(last.as_slice().to_vec()),
			None => bound_to_owned(start),
		};
		let end_owned = bound_to_owned(end);

		let conn = self.inner.conn.lock();

		let start_ref = bound_as_ref(&effective_start);
		let end_ref = bound_as_ref(&end_owned);
		let (query, params) = build_range_query(TABLE_NAME, start_ref, end_ref, false, batch_size + 1);

		let mut stmt = match conn.prepare(&query) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => {
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
			Err(e) => return Err(error!(internal(format!("Failed to prepare query: {}", e)))),
		};

		let params_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

		let entries: Vec<RawEntry> = stmt
			.query_map(params_refs.as_slice(), |row| {
				let key: Vec<u8> = row.get(0)?;
				let value: Option<Vec<u8>> = row.get(1)?;
				Ok(RawEntry {
					key: CowVec::new(key),
					value: value.map(CowVec::new),
				})
			})
			.map_err(|e| error!(internal(format!("Failed to query range: {}", e))))?
			.filter_map(|r| r.ok())
			.collect();

		let has_more = entries.len() > batch_size;
		let entries = if has_more {
			entries.into_iter().take(batch_size).collect()
		} else {
			entries
		};

		let batch = RangeBatch {
			entries,
			has_more,
		};

		// Update cursor
		if let Some(last_entry) = batch.entries.last() {
			cursor.last_key = Some(last_entry.key.clone());
		}
		if !batch.has_more {
			cursor.exhausted = true;
		}

		Ok(batch)
	}

	#[instrument(name = "store::single::sqlite::range_rev_next", level = "trace", skip(self, cursor))]
	fn range_rev_next(
		&self,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		// For reverse iteration, effective end bound based on cursor
		let start_owned = bound_to_owned(start);
		let effective_end: Bound<Vec<u8>> = match &cursor.last_key {
			Some(last) => Bound::Excluded(last.as_slice().to_vec()),
			None => bound_to_owned(end),
		};

		let conn = self.inner.conn.lock();

		let start_ref = bound_as_ref(&start_owned);
		let end_ref = bound_as_ref(&effective_end);
		let (query, params) = build_range_query(TABLE_NAME, start_ref, end_ref, true, batch_size + 1);

		let mut stmt = match conn.prepare(&query) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => {
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
			Err(e) => return Err(error!(internal(format!("Failed to prepare query: {}", e)))),
		};

		let params_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

		let entries: Vec<RawEntry> = stmt
			.query_map(params_refs.as_slice(), |row| {
				let key: Vec<u8> = row.get(0)?;
				let value: Option<Vec<u8>> = row.get(1)?;
				Ok(RawEntry {
					key: CowVec::new(key),
					value: value.map(CowVec::new),
				})
			})
			.map_err(|e| error!(internal(format!("Failed to query range: {}", e))))?
			.filter_map(|r| r.ok())
			.collect();

		let has_more = entries.len() > batch_size;
		let entries = if has_more {
			entries.into_iter().take(batch_size).collect()
		} else {
			entries
		};

		let batch = RangeBatch {
			entries,
			has_more,
		};

		// Update cursor
		if let Some(last_entry) = batch.entries.last() {
			cursor.last_key = Some(last_entry.key.clone());
		}
		if !batch.has_more {
			cursor.exhausted = true;
		}

		Ok(batch)
	}

	#[instrument(name = "store::single::sqlite::ensure_table", level = "trace", skip(self))]
	fn ensure_table(&self) -> Result<()> {
		let conn = self.inner.conn.lock();

		conn.execute(
			&format!(
				"CREATE TABLE IF NOT EXISTS \"{}\" (
					key   BLOB NOT NULL PRIMARY KEY,
					value BLOB
				) WITHOUT ROWID",
				TABLE_NAME
			),
			[],
		)
		.map(|_| ())
		.map_err(|e| error!(internal(format!("Failed to ensure table: {}", e))))
	}

	#[instrument(name = "store::single::sqlite::clear_table", level = "debug", skip(self))]
	fn clear_table(&self) -> Result<()> {
		let conn = self.inner.conn.lock();

		let result = conn.execute(&format!("DELETE FROM \"{}\"", TABLE_NAME), []);

		match result {
			Ok(_) => Ok(()),
			Err(e) if e.to_string().contains("no such table") => Ok(()),
			Err(e) => Err(error!(internal(format!("Failed to clear table: {}", e)))),
		}
	}
}

impl TierBackend for SqlitePrimitiveStorage {}

/// Convert owned Bound to Bound<&[u8]>
fn bound_as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
	match bound {
		Bound::Included(v) => Bound::Included(v.as_slice()),
		Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

/// Convert Bound<&[u8]> to Bound<Vec<u8>>
fn bound_to_owned(bound: Bound<&[u8]>) -> Bound<Vec<u8>> {
	match bound {
		Bound::Included(v) => Bound::Included(v.to_vec()),
		Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

/// Insert entries into a table within an existing transaction
fn insert_entries_in_tx(
	tx: &rusqlite::Transaction,
	table_name: &str,
	entries: &[(CowVec<u8>, Option<CowVec<u8>>)],
) -> rusqlite::Result<()> {
	for (key, value) in entries {
		match value {
			Some(v) => {
				tx.execute(
					&format!(
						"INSERT OR REPLACE INTO \"{}\" (key, value) VALUES (?1, ?2)",
						table_name
					),
					params![key.as_slice(), v.as_slice()],
				)?;
			}
			None => {
				tx.execute(
					&format!("DELETE FROM \"{}\" WHERE key = ?1", table_name),
					params![key.as_slice()],
				)?;
			}
		}
	}
	Ok(())
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_basic_operations() {
		let storage = SqlitePrimitiveStorage::in_memory();

		// Put and get
		storage.set(vec![(CowVec::new(b"key1".to_vec()), Some(CowVec::new(b"value1".to_vec())))]).unwrap();
		let value = storage.get(b"key1").unwrap();
		assert_eq!(value.as_deref(), Some(b"value1".as_slice()));

		// Contains
		assert!(storage.contains(b"key1").unwrap());
		assert!(!storage.contains(b"nonexistent").unwrap());

		// Delete (tombstone)
		storage.set(vec![(CowVec::new(b"key1".to_vec()), None)]).unwrap();
		assert!(!storage.contains(b"key1").unwrap());
	}

	#[test]
	fn test_range_next() {
		let storage = SqlitePrimitiveStorage::in_memory();

		storage.set(vec![(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec())))]).unwrap();
		storage.set(vec![(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec())))]).unwrap();
		storage.set(vec![(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec())))]).unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage.range_next(&mut cursor, Bound::Unbounded, Bound::Unbounded, 100).unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
		assert_eq!(&*batch.entries[0].key, b"a");
		assert_eq!(&*batch.entries[1].key, b"b");
		assert_eq!(&*batch.entries[2].key, b"c");
	}

	#[test]
	fn test_range_rev_next() {
		let storage = SqlitePrimitiveStorage::in_memory();

		storage.set(vec![(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec())))]).unwrap();
		storage.set(vec![(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec())))]).unwrap();
		storage.set(vec![(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec())))]).unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage.range_rev_next(&mut cursor, Bound::Unbounded, Bound::Unbounded, 100).unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
		assert_eq!(&*batch.entries[0].key, b"c");
		assert_eq!(&*batch.entries[1].key, b"b");
		assert_eq!(&*batch.entries[2].key, b"a");
	}

	#[test]
	fn test_range_streaming_pagination() {
		let storage = SqlitePrimitiveStorage::in_memory();

		// Insert 10 entries
		for i in 0..10u8 {
			storage.set(vec![(CowVec::new(vec![i]), Some(CowVec::new(vec![i * 10])))]).unwrap();
		}

		// Use a single cursor to stream through all entries
		let mut cursor = RangeCursor::new();

		// First batch of 3
		let batch1 = storage.range_next(&mut cursor, Bound::Unbounded, Bound::Unbounded, 3).unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch1.entries[0].key, &[0]);
		assert_eq!(&*batch1.entries[2].key, &[2]);

		// Second batch of 3 - cursor automatically continues
		let batch2 = storage.range_next(&mut cursor, Bound::Unbounded, Bound::Unbounded, 3).unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch2.entries[0].key, &[3]);
		assert_eq!(&*batch2.entries[2].key, &[5]);
	}

	#[test]
	fn test_range_reving_pagination() {
		let storage = SqlitePrimitiveStorage::in_memory();

		// Insert 10 entries
		for i in 0..10u8 {
			storage.set(vec![(CowVec::new(vec![i]), Some(CowVec::new(vec![i * 10])))]).unwrap();
		}

		// Use a single cursor to stream in reverse
		let mut cursor = RangeCursor::new();

		// First batch of 3 (reverse)
		let batch1 = storage.range_rev_next(&mut cursor, Bound::Unbounded, Bound::Unbounded, 3).unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch1.entries[0].key, &[9]);
		assert_eq!(&*batch1.entries[2].key, &[7]);

		// Second batch
		let batch2 = storage.range_rev_next(&mut cursor, Bound::Unbounded, Bound::Unbounded, 3).unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch2.entries[0].key, &[6]);
		assert_eq!(&*batch2.entries[2].key, &[4]);
	}

	#[test]
	fn test_get_nonexistent_table() {
		let storage = SqlitePrimitiveStorage::in_memory();

		// Should return None for non-existent table, not error
		let value = storage.get(b"key").unwrap();
		assert_eq!(value, None);
	}

	#[test]
	fn test_range_nonexistent_table() {
		let storage = SqlitePrimitiveStorage::in_memory();

		// Should return empty batch for non-existent table, not error
		let mut cursor = RangeCursor::new();
		let batch = storage.range_next(&mut cursor, Bound::Unbounded, Bound::Unbounded, 100).unwrap();
		assert!(batch.entries.is_empty());
		assert!(cursor.exhausted);
	}
}
