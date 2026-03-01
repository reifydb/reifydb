// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! SQLite implementation of PrimitiveStorage with MVCC versioning.
//!
//! Uses SQLite tables with (key, version) composite primary key for persistent
//! multi-version storage. All operations use a single connection protected by
//! Mutex for thread safety.

use std::{collections::HashMap, ops::Bound, sync::Arc};

use reifydb_core::{common::CommitVersion, error::diagnostic::internal::internal};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_type::{Result, error, util::cowvec::CowVec};
use rusqlite::{
	Connection, Error::QueryReturnedNoRows, Result as SqliteResult, ToSql, Transaction as SqliteTransaction, params,
};
use tracing::instrument;

use super::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	entry::entry_id_to_name,
	query::{build_versioned_range_query, version_to_bytes},
};
use crate::tier::{EntryKind, RangeBatch, RangeCursor, RawEntry, TierBackend, TierStorage};

/// SQLite-based primitive storage implementation with MVCC versioning.
///
/// Uses SQLite for persistent storage with a single connection protected by Mutex.
/// Tables use (key, version) composite primary key for multi-version support.
#[derive(Clone)]
pub struct SqlitePrimitiveStorage {
	inner: Arc<SqlitePrimitiveStorageInner>,
}

struct SqlitePrimitiveStorageInner {
	/// Single connection protected by Mutex for thread-safe access.
	/// Note: We use Mutex instead of RwLock because rusqlite::Connection
	/// is Send but not Sync.
	conn: Mutex<Connection>,
}

impl SqlitePrimitiveStorage {
	/// Create a new SQLite primitive storage with the given configuration.
	#[instrument(name = "store::multi::sqlite::new", level = "debug", skip(config), fields(
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

	/// Run incremental vacuum to return freed pages to the OS.
	pub fn incremental_vacuum(&self) {
		let conn = self.inner.conn.lock();
		let _ = conn.execute("PRAGMA incremental_vacuum", []);
	}

	/// Release unused memory back to the allocator.
	pub fn shrink_memory(&self) {
		let conn = self.inner.conn.lock();
		let _ = conn.pragma_update(None, "shrink_memory", 0);
	}

	/// Explicitly checkpoint WAL and shrink the page cache before shutdown.
	pub fn shutdown(&self) {
		let conn = self.inner.conn.lock();
		let _ = conn.pragma_update(None, "wal_checkpoint", "TRUNCATE");
		let _ = conn.pragma_update(None, "cache_size", 0);
	}

	/// Create a table with the versioned schema if it doesn't exist.
	fn create_table_if_needed(conn: &Connection, table_name: &str) -> SqliteResult<()> {
		conn.execute(
			&format!(
				"CREATE TABLE IF NOT EXISTS \"{}\" (
					key BLOB NOT NULL,
					version BLOB NOT NULL,
					value BLOB,
					PRIMARY KEY (key, version)
				) WITHOUT ROWID",
				table_name
			),
			[],
		)?;
		Ok(())
	}
}

impl TierStorage for SqlitePrimitiveStorage {
	#[instrument(name = "store::multi::sqlite::get", level = "trace", skip(self), fields(table = ?table, key_len = key.len(), version = version.0))]
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<Option<CowVec<u8>>> {
		let table_name = entry_id_to_name(table);
		let conn = self.inner.conn.lock();

		// Get the latest version <= requested version for this key
		let result = conn.query_row(
			&format!(
				"SELECT value FROM \"{}\" WHERE key = ?1 AND version <= ?2 ORDER BY version DESC LIMIT 1",
				table_name
			),
			params![key, version_to_bytes(version).as_slice()],
			|row| row.get::<_, Option<Vec<u8>>>(0),
		);

		match result {
			Ok(Some(value)) => Ok(Some(CowVec::new(value))),
			Ok(None) => Ok(None), // Tombstone
			Err(QueryReturnedNoRows) => Ok(None),
			Err(e) if e.to_string().contains("no such table") => Ok(None),
			Err(e) => Err(error!(internal(format!("Failed to get: {}", e)))),
		}
	}

	#[instrument(name = "store::multi::sqlite::contains", level = "trace", skip(self), fields(table = ?table, key_len = key.len(), version = version.0), ret)]
	fn contains(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<bool> {
		let table_name = entry_id_to_name(table);
		let conn = self.inner.conn.lock();

		// Check if value exists and is not a tombstone
		let result = conn.query_row(
			&format!(
				"SELECT value IS NOT NULL FROM \"{}\" WHERE key = ?1 AND version <= ?2 ORDER BY version DESC LIMIT 1",
				table_name
			),
			params![key, version_to_bytes(version).as_slice()],
			|row| row.get::<_, bool>(0),
		);

		match result {
			Ok(has_value) => Ok(has_value),
			Err(QueryReturnedNoRows) => Ok(false),
			Err(e) if e.to_string().contains("no such table") => Ok(false),
			Err(e) => Err(error!(internal(format!("Failed to check contains: {}", e)))),
		}
	}

	#[instrument(name = "store::multi::sqlite::set", level = "debug", skip(self, batches), fields(table_count = batches.len(), version = version.0))]
	fn set(
		&self,
		version: CommitVersion,
		batches: HashMap<EntryKind, Vec<(CowVec<u8>, Option<CowVec<u8>>)>>,
	) -> Result<()> {
		if batches.is_empty() {
			return Ok(());
		}

		let conn = self.inner.conn.lock();
		let tx = conn
			.unchecked_transaction()
			.map_err(|e| error!(internal(format!("Failed to start transaction: {}", e))))?;

		for (table, entries) in batches {
			let table_name = entry_id_to_name(table);

			// Try to insert entries, creating table if needed
			let result = insert_versioned_entries_in_tx(&tx, &table_name, version, &entries);
			if let Err(e) = result {
				if e.to_string().contains("no such table") {
					Self::create_table_if_needed(&tx, &table_name).map_err(|e| {
						error!(internal(format!("Failed to create table: {}", e)))
					})?;
					insert_versioned_entries_in_tx(&tx, &table_name, version, &entries).map_err(
						|e| error!(internal(format!("Failed to insert entries: {}", e))),
					)?;
				} else {
					return Err(error!(internal(format!("Failed to insert entries: {}", e))));
				}
			}
		}

		tx.commit().map_err(|e| error!(internal(format!("Failed to commit transaction: {}", e))))
	}

	#[instrument(name = "store::multi::sqlite::range_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size, version = version.0))]
	fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let table_name = entry_id_to_name(table);

		// Determine effective start bound based on cursor state
		let effective_start: Bound<Vec<u8>> = match &cursor.last_key {
			Some(last) => Bound::Excluded(last.as_slice().to_vec()),
			None => bound_to_owned(start),
		};
		let end_owned = bound_to_owned(end);

		let conn = self.inner.conn.lock();

		let start_ref = bound_as_ref(&effective_start);
		let end_ref = bound_as_ref(&end_owned);
		let (query, params) =
			build_versioned_range_query(&table_name, start_ref, end_ref, version, false, batch_size + 1);

		let mut stmt = match conn.prepare(&query) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => {
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
			Err(e) => return Err(error!(internal(format!("Failed to prepare query: {}", e)))),
		};

		let params_refs: Vec<&dyn ToSql> = params.iter().map(|p| p as &dyn ToSql).collect();

		let entries: Vec<RawEntry> = stmt
			.query_map(params_refs.as_slice(), |row| {
				let key: Vec<u8> = row.get(0)?;
				let version_bytes: Vec<u8> = row.get(1)?;
				let value: Option<Vec<u8>> = row.get(2)?;
				let version = u64::from_be_bytes(
					version_bytes.as_slice().try_into().expect("version must be 8 bytes"),
				);
				Ok(RawEntry {
					key: CowVec::new(key),
					version: CommitVersion(version),
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

	#[instrument(name = "store::multi::sqlite::range_rev_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size, version = version.0))]
	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let table_name = entry_id_to_name(table);

		// For reverse iteration, effective end bound based on cursor
		let start_owned = bound_to_owned(start);
		let effective_end: Bound<Vec<u8>> = match &cursor.last_key {
			Some(last) => Bound::Excluded(last.as_slice().to_vec()),
			None => bound_to_owned(end),
		};

		let conn = self.inner.conn.lock();

		let start_ref = bound_as_ref(&start_owned);
		let end_ref = bound_as_ref(&effective_end);
		let (query, params) =
			build_versioned_range_query(&table_name, start_ref, end_ref, version, true, batch_size + 1);

		let mut stmt = match conn.prepare(&query) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => {
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
			Err(e) => return Err(error!(internal(format!("Failed to prepare query: {}", e)))),
		};

		let params_refs: Vec<&dyn ToSql> = params.iter().map(|p| p as &dyn ToSql).collect();

		let entries: Vec<RawEntry> = stmt
			.query_map(params_refs.as_slice(), |row| {
				let key: Vec<u8> = row.get(0)?;
				let version_bytes: Vec<u8> = row.get(1)?;
				let value: Option<Vec<u8>> = row.get(2)?;
				let version = u64::from_be_bytes(
					version_bytes.as_slice().try_into().expect("version must be 8 bytes"),
				);
				Ok(RawEntry {
					key: CowVec::new(key),
					version: CommitVersion(version),
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

	fn ensure_table(&self, table: EntryKind) -> Result<()> {
		let table_name = entry_id_to_name(table);
		let conn = self.inner.conn.lock();

		Self::create_table_if_needed(&conn, &table_name)
			.map_err(|e| error!(internal(format!("Failed to ensure table: {}", e))))
	}

	fn clear_table(&self, table: EntryKind) -> Result<()> {
		let table_name = entry_id_to_name(table);
		let conn = self.inner.conn.lock();

		let result = conn.execute(&format!("DELETE FROM \"{}\"", table_name), []);

		match result {
			Ok(_) => Ok(()),
			Err(e) if e.to_string().contains("no such table") => Ok(()),
			Err(e) => Err(error!(internal(format!("Failed to clear table: {}", e)))),
		}
	}

	#[instrument(name = "store::multi::sqlite::drop", level = "debug", skip(self, batches), fields(table_count = batches.len()))]
	fn drop(&self, batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>>) -> Result<()> {
		if batches.is_empty() {
			return Ok(());
		}

		let conn = self.inner.conn.lock();
		let tx = conn
			.unchecked_transaction()
			.map_err(|e| error!(internal(format!("Failed to start transaction: {}", e))))?;

		for (table, entries) in batches {
			let table_name = entry_id_to_name(table);

			let max_version_sql = format!("SELECT MAX(version) FROM \"{}\" WHERE key = ?1", table_name);
			let delete_all_sql = format!("DELETE FROM \"{}\" WHERE key = ?1", table_name);
			let delete_one_sql = format!("DELETE FROM \"{}\" WHERE key = ?1 AND version = ?2", table_name);

			let mut max_version_stmt = match tx.prepare(&max_version_sql) {
				Ok(s) => s,
				Err(e) if e.to_string().contains("no such table") => continue,
				Err(e) => return Err(error!(internal(format!("Failed to prepare query: {}", e)))),
			};
			let mut delete_all_stmt = tx
				.prepare(&delete_all_sql)
				.map_err(|e| error!(internal(format!("Failed to prepare delete: {}", e))))?;
			let mut delete_one_stmt = tx
				.prepare(&delete_one_sql)
				.map_err(|e| error!(internal(format!("Failed to prepare delete: {}", e))))?;

			for (key, version) in entries {
				let version_bytes = version_to_bytes(version);

				let max_version: Option<Vec<u8>> = max_version_stmt
					.query_row(params![key.as_slice()], |row| row.get(0))
					.unwrap_or(None);

				let is_latest = max_version.as_deref() == Some(version_bytes.as_slice());

				let result = if is_latest {
					delete_all_stmt.execute(params![key.as_slice()])
				} else {
					delete_one_stmt.execute(params![key.as_slice(), version_bytes.as_slice()])
				};

				if let Err(e) = result {
					if !e.to_string().contains("no such table") {
						return Err(error!(internal(format!("Failed to delete entry: {}", e))));
					}
				}
			}
		}

		tx.commit().map_err(|e| error!(internal(format!("Failed to commit transaction: {}", e))))
	}

	#[instrument(name = "store::multi::sqlite::get_all_versions", level = "trace", skip(self), fields(table = ?table, key_len = key.len()))]
	fn get_all_versions(&self, table: EntryKind, key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		let table_name = entry_id_to_name(table);
		let conn = self.inner.conn.lock();

		let mut stmt = match conn.prepare(&format!(
			"SELECT version, value FROM \"{}\" WHERE key = ?1 ORDER BY version DESC",
			table_name
		)) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
			Err(e) => return Err(error!(internal(format!("Failed to prepare query: {}", e)))),
		};

		let versions: Vec<(CommitVersion, Option<CowVec<u8>>)> = stmt
			.query_map(params![key], |row| {
				let version_bytes: Vec<u8> = row.get(0)?;
				let value: Option<Vec<u8>> = row.get(1)?;
				let version = u64::from_be_bytes(
					version_bytes.as_slice().try_into().expect("version must be 8 bytes"),
				);
				Ok((CommitVersion(version), value.map(CowVec::new)))
			})
			.map_err(|e| error!(internal(format!("Failed to query versions: {}", e))))?
			.filter_map(|r| r.ok())
			.collect();

		Ok(versions)
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

/// Insert versioned entries into a table within an existing transaction
fn insert_versioned_entries_in_tx(
	tx: &SqliteTransaction,
	table_name: &str,
	version: CommitVersion,
	entries: &[(CowVec<u8>, Option<CowVec<u8>>)],
) -> SqliteResult<()> {
	let version_bytes = version_to_bytes(version);
	let sql = format!("INSERT OR REPLACE INTO \"{}\" (key, version, value) VALUES (?1, ?2, ?3)", table_name);
	let mut stmt = tx.prepare(&sql)?;
	for (key, value) in entries {
		stmt.execute(params![key.as_slice(), version_bytes.as_slice(), value.as_ref().map(|v| v.as_slice())])?;
	}
	Ok(())
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{id::TableId, primitive::PrimitiveId};

	use super::*;

	#[test]
	fn test_basic_operations() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let key = CowVec::new(b"key1".to_vec());
		let version = CommitVersion(1);

		// Put and get
		storage.set(
			version,
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value1".to_vec())))])]),
		)
		.unwrap();
		let value = storage.get(EntryKind::Multi, &key, version).unwrap();
		assert_eq!(value.as_deref(), Some(b"value1".as_slice()));

		// Contains
		assert!(storage.contains(EntryKind::Multi, &key, version).unwrap());
		assert!(!storage.contains(EntryKind::Multi, b"nonexistent", version).unwrap());

		// Delete (tombstone)
		let version2 = CommitVersion(2);
		storage.set(version2, HashMap::from([(EntryKind::Multi, vec![(key.clone(), None)])])).unwrap();
		assert!(!storage.contains(EntryKind::Multi, &key, version2).unwrap());
	}

	#[test]
	fn test_source_tables() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let source1 = PrimitiveId::Table(TableId(1));
		let source2 = PrimitiveId::Table(TableId(2));
		let key = CowVec::new(b"key".to_vec());
		let version = CommitVersion(1);

		storage.set(
			version,
			HashMap::from([(
				EntryKind::Source(source1),
				vec![(key.clone(), Some(CowVec::new(b"table1".to_vec())))],
			)]),
		)
		.unwrap();
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Source(source2),
				vec![(key.clone(), Some(CowVec::new(b"table2".to_vec())))],
			)]),
		)
		.unwrap();

		assert_eq!(
			storage.get(EntryKind::Source(source1), &key, version).unwrap().as_deref(),
			Some(b"table1".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Source(source2), &key, version).unwrap().as_deref(),
			Some(b"table2".as_slice())
		);
	}

	#[test]
	fn test_version_queries() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let key = CowVec::new(b"key1".to_vec());

		// Insert multiple versions
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(2),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v2".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(3),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v3".to_vec())))])]),
		)
		.unwrap();

		// Get at specific versions
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().as_deref(),
			Some(b"v3".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().as_deref(),
			Some(b"v2".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().as_deref(),
			Some(b"v1".as_slice())
		);

		// Get at intermediate version returns closest <= version
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(10)).unwrap().as_deref(),
			Some(b"v3".as_slice())
		);
	}

	#[test]
	fn test_range_next() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
					(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
					(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 100)
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
		let storage = SqlitePrimitiveStorage::in_memory();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
					(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
					(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_rev_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 100)
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
		let storage = SqlitePrimitiveStorage::in_memory();

		let version = CommitVersion(1);

		// Insert 10 entries
		let entries: Vec<_> =
			(0..10u8).map(|i| (CowVec::new(vec![i]), Some(CowVec::new(vec![i * 10])))).collect();
		storage.set(version, HashMap::from([(EntryKind::Multi, entries)])).unwrap();

		// Use a single cursor to stream through all entries
		let mut cursor = RangeCursor::new();

		// First batch of 3
		let batch1 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch1.entries[0].key, &[0]);
		assert_eq!(&*batch1.entries[2].key, &[2]);

		// Second batch of 3 - cursor automatically continues
		let batch2 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch2.entries[0].key, &[3]);
		assert_eq!(&*batch2.entries[2].key, &[5]);
	}

	#[test]
	fn test_range_reving_pagination() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let version = CommitVersion(1);

		// Insert 10 entries
		let entries: Vec<_> =
			(0..10u8).map(|i| (CowVec::new(vec![i]), Some(CowVec::new(vec![i * 10])))).collect();
		storage.set(version, HashMap::from([(EntryKind::Multi, entries)])).unwrap();

		// Use a single cursor to stream in reverse
		let mut cursor = RangeCursor::new();

		// First batch of 3 (reverse)
		let batch1 = storage
			.range_rev_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch1.entries[0].key, &[9]);
		assert_eq!(&*batch1.entries[2].key, &[7]);

		// Second batch
		let batch2 = storage
			.range_rev_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
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
		let value = storage.get(EntryKind::Multi, b"key", CommitVersion(1)).unwrap();
		assert_eq!(value, None);
	}

	#[test]
	fn test_range_nonexistent_table() {
		let storage = SqlitePrimitiveStorage::in_memory();

		// Should return empty batch for non-existent table, not error
		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				CommitVersion(1),
				100,
			)
			.unwrap();
		assert!(batch.entries.is_empty());
		assert!(cursor.exhausted);
	}

	#[test]
	fn test_drop_specific_version() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let key = CowVec::new(b"key1".to_vec());

		// Insert versions 1, 2, 3
		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{}", v).into_bytes())))],
				)]),
			)
			.unwrap();
		}

		// Drop version 1
		storage.drop(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(1))])])).unwrap();

		// Version 1 should no longer be accessible
		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().is_none());

		// Versions 2 and 3 should still work
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().as_deref(),
			Some(b"v2".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().as_deref(),
			Some(b"v3".as_slice())
		);
	}
}
