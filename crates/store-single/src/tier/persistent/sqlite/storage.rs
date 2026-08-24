// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicU64, AtomicUsize, Ordering},
	},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::internal_error;
use reifydb_runtime::{
	shutdown::Shutdown,
	sync::mutex::{Mutex, MutexGuard},
};
use reifydb_sqlite::{
	JournalMode, SqliteConfig, SqliteTempPathGuard,
	connection::{connect, convert_flags, resolve_db_path},
	memory::sweep_connection_cache,
	pragma,
};
use reifydb_value::{
	Result, byte_size::ByteSize, count::Count, reifydb_assertions, util::cowvec::CowVec, value::duration::Duration,
};
use rusqlite::{
	Connection, Error::QueryReturnedNoRows, Result as SqliteResult, ToSql, Transaction as SqliteTransaction,
	TransactionBehavior, params,
};
use tracing::{instrument, warn};

use super::query::build_range_query;
use crate::tier::{RangeBatch, RangeCursor, RawEntry, TierBackend, TierStorage, persistent::SinglePageCacheMetrics};

const TABLE_NAME: &str = "entries";

struct ReadPool {
	conns: Vec<Mutex<Option<Connection>>>,
	next: AtomicUsize,
}

impl ReadPool {
	fn acquire(&self) -> MutexGuard<'_, Option<Connection>> {
		let n = self.conns.len();
		let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
		for i in 0..n {
			if let Some(guard) = self.conns[(start + i) % n].try_lock() {
				return guard;
			}
		}
		self.conns[start].lock()
	}

	fn shutdown(&self) {
		for slot in &self.conns {
			drop(slot.lock().take());
		}
	}
}

const JOURNAL_MODE: JournalMode = JournalMode::Wal;

const BUSY_TIMEOUT: Duration = Duration::from_milliseconds_const(200);

#[derive(Clone)]
pub struct SqlitePersistentStorage {
	inner: Arc<SqlitePersistentStorageInner>,
}

struct SqlitePersistentStorageInner {
	conn: Mutex<Option<Connection>>,
	readers: ReadPool,
	cache_hits: AtomicU64,
	cache_misses: AtomicU64,
}

impl SqlitePersistentStorage {
	#[instrument(name = "store::single::persistent::new", level = "debug", skip(config), fields(
		db_path = ?config.path,
		page_size = config.page_size.as_ref().map(|size| size.as_bytes()),
		journal_mode = JOURNAL_MODE.as_str()
	))]
	pub fn new(config: SqliteConfig) -> Self {
		let config = SqliteConfig {
			journal_mode: Some(JOURNAL_MODE),
			..config
		};
		let db_path = resolve_db_path(config.path.clone(), "persistent.db");
		let flags = convert_flags(&config.flags);

		let conn = connect(&db_path, flags).expect("Failed to connect to database");
		pragma::apply(&conn, &config).expect("Failed to configure SQLite pragmas");
		conn.busy_timeout(BUSY_TIMEOUT.to_std()).expect("Failed to set single busy timeout");

		let pool_size = config.read_pool_size.max(1) as usize;
		let mut conns = Vec::with_capacity(pool_size);
		for _ in 0..pool_size {
			let reader = connect(&db_path, flags).expect("Failed to open single read connection");
			pragma::apply_read_only(&reader, &config).expect("Failed to configure single read connection");
			reader.busy_timeout(BUSY_TIMEOUT.to_std()).expect("Failed to set single read busy timeout");
			conns.push(Mutex::new(Some(reader)));
		}

		Self {
			inner: Arc::new(SqlitePersistentStorageInner {
				conn: Mutex::new(Some(conn)),
				readers: ReadPool {
					conns,
					next: AtomicUsize::new(0),
				},
				cache_hits: AtomicU64::new(0),
				cache_misses: AtomicU64::new(0),
			}),
		}
	}

	pub fn in_memory() -> (Self, SqliteTempPathGuard) {
		let (config, guard) = SqliteConfig::in_memory();
		(Self::new(config), guard)
	}

	pub fn page_cache_metrics(&self) -> SinglePageCacheMetrics {
		let mut used = 0u64;
		let mut sampled = 0u64;
		if let Some(guard) = self.inner.conn.try_lock()
			&& let Some(conn) = guard.as_ref()
		{
			let swept = sweep_connection_cache(conn);
			self.inner.cache_hits.fetch_add(swept.hits.as_u64(), Ordering::Relaxed);
			self.inner.cache_misses.fetch_add(swept.misses.as_u64(), Ordering::Relaxed);
			used += swept.used.as_bytes();
			sampled += 1;
		}
		SinglePageCacheMetrics {
			used: ByteSize::from_bytes(used),
			hits: Count::new(self.inner.cache_hits.load(Ordering::Relaxed)),
			misses: Count::new(self.inner.cache_misses.load(Ordering::Relaxed)),
			connections_sampled: Count::new(sampled),
			connections_total: Count::new(1),
		}
	}
}

impl TierStorage for SqlitePersistentStorage {
	#[instrument(name = "store::single::persistent::get", level = "trace", skip(self, key), fields(key_len = key.len()))]
	fn get(&self, key: &[u8]) -> Result<Option<CowVec<u8>>> {
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(None);
		};

		let result = conn
			.prepare_cached(&format!("SELECT value FROM \"{}\" WHERE key = ?1", TABLE_NAME))
			.and_then(|mut stmt| stmt.query_row(params![key], |row| row.get::<_, Option<Vec<u8>>>(0)));

		match result {
			Ok(Some(value)) => Ok(Some(CowVec::new(value))),
			Ok(None) => Ok(None),
			Err(QueryReturnedNoRows) => Ok(None),
			Err(e) if e.to_string().contains("no such table") => Ok(None),
			Err(e) => Err(internal_error!("Failed to get: {}", e)),
		}
	}

	#[instrument(name = "store::single::persistent::get_with_tombstone", level = "trace", skip(self, key), fields(key_len = key.len()))]
	fn get_with_tombstone(&self, key: &[u8]) -> Result<Option<Option<CowVec<u8>>>> {
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(None);
		};

		let result = conn
			.prepare_cached(&format!("SELECT value FROM \"{}\" WHERE key = ?1", TABLE_NAME))
			.and_then(|mut stmt| stmt.query_row(params![key], |row| row.get::<_, Option<Vec<u8>>>(0)));

		match result {
			Ok(value) => Ok(Some(value.map(CowVec::new))),
			Err(QueryReturnedNoRows) => Ok(None),
			Err(e) if e.to_string().contains("no such table") => Ok(None),
			Err(e) => Err(internal_error!("Failed to get_with_tombstone: {}", e)),
		}
	}

	#[instrument(name = "store::single::persistent::contains", level = "trace", skip(self, key), fields(key_len = key.len()), ret)]
	fn contains(&self, key: &[u8]) -> Result<bool> {
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(false);
		};

		let result = conn
			.prepare_cached(&format!("SELECT value IS NOT NULL FROM \"{}\" WHERE key = ?1", TABLE_NAME))
			.and_then(|mut stmt| stmt.query_row(params![key], |row| row.get::<_, bool>(0)));

		match result {
			Ok(has_value) => Ok(has_value),
			Err(QueryReturnedNoRows) => Ok(false),
			Err(e) if e.to_string().contains("no such table") => Ok(false),
			Err(e) => Err(internal_error!("Failed to check contains: {}", e)),
		}
	}

	#[instrument(name = "store::single::persistent::set", level = "debug", skip(self, entries), fields(entry_count = entries.len()))]
	fn set(&self, entries: Vec<(EncodedKey, Option<CowVec<u8>>)>) -> Result<()> {
		if entries.is_empty() {
			return Ok(());
		}

		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Err(internal_error!("Failed to set: the persistent connection is closed"));
		};

		let tx = self.begin_tx(conn)?;
		self.insert_with_create_table_retry(&tx, &entries)?;
		self.commit_tx(tx)
	}

	#[instrument(name = "store::single::persistent::range_next", level = "trace", skip(self, cursor))]
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

		let (effective_start, end_owned) = self.forward_bounds(cursor, start, end);

		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			cursor.exhausted = true;
			return Ok(RangeBatch::empty());
		};

		let Some(entries) = self.query_forward(conn, &effective_start, &end_owned, batch_size)? else {
			cursor.exhausted = true;
			return Ok(RangeBatch::empty());
		};

		let batch = trim_to_batch(entries, batch_size);
		self.advance_forward_cursor(cursor, &batch);
		Ok(batch)
	}

	#[instrument(name = "store::single::persistent::range_rev_next", level = "trace", skip(self, cursor))]
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

		let (start_owned, effective_end) = self.reverse_bounds(cursor, start, end);

		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			cursor.exhausted = true;
			return Ok(RangeBatch::empty());
		};

		let Some(entries) = self.query_reverse(conn, &start_owned, &effective_end, batch_size)? else {
			cursor.exhausted = true;
			return Ok(RangeBatch::empty());
		};

		let batch = trim_to_batch(entries, batch_size);
		self.advance_reverse_cursor(cursor, &batch);
		Ok(batch)
	}

	#[instrument(name = "store::single::persistent::ensure_table", level = "debug", skip(self))]
	fn ensure_table(&self) -> Result<()> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(());
		};

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
		.map_err(|e| internal_error!("Failed to ensure table: {}", e))
	}

	#[instrument(name = "store::single::persistent::clear_table", level = "debug", skip(self))]
	fn clear_table(&self) -> Result<()> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(());
		};

		let result = conn.execute(&format!("DELETE FROM \"{}\"", TABLE_NAME), []);

		match result {
			Ok(_) => Ok(()),
			Err(e) if e.to_string().contains("no such table") => Ok(()),
			Err(e) => Err(internal_error!("Failed to clear table: {}", e)),
		}
	}
}

impl SqlitePersistentStorage {
	#[inline]
	fn begin_tx<'a>(&self, conn: &'a Connection) -> Result<SqliteTransaction<'a>> {
		SqliteTransaction::new_unchecked(conn, TransactionBehavior::Immediate)
			.map_err(|e| internal_error!("Failed to start transaction: {}", e))
	}

	#[inline]
	fn insert_with_create_table_retry(
		&self,
		tx: &SqliteTransaction,
		entries: &[(EncodedKey, Option<CowVec<u8>>)],
	) -> Result<()> {
		let result = insert_entries_in_tx(tx, TABLE_NAME, entries);
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
				.map_err(|e| internal_error!("Failed to create table: {}", e))?;
				insert_entries_in_tx(tx, TABLE_NAME, entries)
					.map_err(|e| internal_error!("Failed to insert entries: {}", e))?;
			} else {
				return Err(internal_error!("Failed to insert entries: {}", e));
			}
		}
		Ok(())
	}

	#[inline]
	fn commit_tx(&self, tx: SqliteTransaction) -> Result<()> {
		tx.commit().map_err(|e| internal_error!("Failed to commit transaction: {}", e))
	}

	#[inline]
	fn forward_bounds(
		&self,
		cursor: &RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
	) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
		let effective_start = match &cursor.last_key {
			Some(last) => Bound::Excluded(last.as_slice().to_vec()),
			None => bound_to_owned(start),
		};
		(effective_start, bound_to_owned(end))
	}

	#[inline]
	fn query_forward(
		&self,
		conn: &Connection,
		effective_start: &Bound<Vec<u8>>,
		end_owned: &Bound<Vec<u8>>,
		batch_size: usize,
	) -> Result<Option<Vec<RawEntry>>> {
		let start_ref = bound_as_ref(effective_start);
		let end_ref = bound_as_ref(end_owned);
		let (query, params) = build_range_query(TABLE_NAME, start_ref, end_ref, false, batch_size + 1);

		let mut stmt = match conn.prepare_cached(&query) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => {
				return Ok(None);
			}
			Err(e) => return Err(internal_error!("Failed to prepare query: {}", e)),
		};

		let params_refs: Vec<&dyn ToSql> = params.iter().map(|p| p as &dyn ToSql).collect();

		let entries: Vec<RawEntry> = stmt
			.query_map(params_refs.as_slice(), |row| {
				let key: Vec<u8> = row.get(0)?;
				let value: Option<Vec<u8>> = row.get(1)?;
				Ok(RawEntry {
					key: EncodedKey::new(key),
					value: value.map(CowVec::new),
				})
			})
			.map_err(|e| internal_error!("Failed to query range: {}", e))?
			.filter_map(|r| r.ok())
			.collect();

		Ok(Some(entries))
	}

	#[inline]
	fn advance_forward_cursor(&self, cursor: &mut RangeCursor, batch: &RangeBatch) {
		reifydb_assertions! {
			if let (Some(prev), Some(first)) = (cursor.last_key.as_ref(), batch.entries.first()) {
				let prev_key = prev.as_slice();
				let first_key = first.key.as_slice();
				assert!(
					first_key > prev_key,
					"forward range scan yielded a first key not strictly greater than the previous batch's last key, so paging re-emits or reorders rows and a consumer reading the stream sees duplicates or moves backwards (prev_last={:?} batch_first={:?})",
					prev_key,
					first_key
				);
			}
		}

		if let Some(last_entry) = batch.entries.last() {
			cursor.last_key = Some(last_entry.key.clone());
		}
		if !batch.has_more {
			cursor.exhausted = true;
		}
	}

	#[inline]
	fn reverse_bounds(
		&self,
		cursor: &RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
	) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
		let effective_end = match &cursor.last_key {
			Some(last) => Bound::Excluded(last.as_slice().to_vec()),
			None => bound_to_owned(end),
		};
		(bound_to_owned(start), effective_end)
	}

	#[inline]
	fn query_reverse(
		&self,
		conn: &Connection,
		start_owned: &Bound<Vec<u8>>,
		effective_end: &Bound<Vec<u8>>,
		batch_size: usize,
	) -> Result<Option<Vec<RawEntry>>> {
		let start_ref = bound_as_ref(start_owned);
		let end_ref = bound_as_ref(effective_end);
		let (query, params) = build_range_query(TABLE_NAME, start_ref, end_ref, true, batch_size + 1);

		let mut stmt = match conn.prepare_cached(&query) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => {
				return Ok(None);
			}
			Err(e) => return Err(internal_error!("Failed to prepare query: {}", e)),
		};

		let params_refs: Vec<&dyn ToSql> = params.iter().map(|p| p as &dyn ToSql).collect();

		let entries: Vec<RawEntry> = stmt
			.query_map(params_refs.as_slice(), |row| {
				let key: Vec<u8> = row.get(0)?;
				let value: Option<Vec<u8>> = row.get(1)?;
				Ok(RawEntry {
					key: EncodedKey::new(key),
					value: value.map(CowVec::new),
				})
			})
			.map_err(|e| internal_error!("Failed to query range: {}", e))?
			.filter_map(|r| r.ok())
			.collect();

		Ok(Some(entries))
	}

	#[inline]
	fn advance_reverse_cursor(&self, cursor: &mut RangeCursor, batch: &RangeBatch) {
		reifydb_assertions! {
			if let (Some(prev), Some(first)) = (cursor.last_key.as_ref(), batch.entries.first()) {
				let prev_key = prev.as_slice();
				let first_key = first.key.as_slice();
				assert!(
					first_key < prev_key,
					"reverse range scan yielded a first key not strictly less than the previous batch's last key, so descending paging re-emits or reorders rows and a consumer reading the stream sees duplicates or moves forwards (prev_last={:?} batch_first={:?})",
					prev_key,
					first_key
				);
			}
		}

		if let Some(last_entry) = batch.entries.last() {
			cursor.last_key = Some(last_entry.key.clone());
		}
		if !batch.has_more {
			cursor.exhausted = true;
		}
	}
}

impl TierBackend for SqlitePersistentStorage {}

impl Shutdown for SqlitePersistentStorage {
	fn shutdown(&self) {
		self.inner.readers.shutdown();
		if let Some(conn) = self.inner.conn.lock().take() {
			if let Err(e) = pragma::shutdown(&conn) {
				warn!(error = %e, "single persistent close: pragma shutdown failed");
			}
			drop(conn);
		}
	}
}

fn trim_to_batch(entries: Vec<RawEntry>, batch_size: usize) -> RangeBatch {
	let has_more = entries.len() > batch_size;
	let entries = if has_more {
		entries.into_iter().take(batch_size).collect()
	} else {
		entries
	};
	RangeBatch {
		entries,
		has_more,
	}
}

fn bound_as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
	match bound {
		Bound::Included(v) => Bound::Included(v.as_slice()),
		Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

fn bound_to_owned(bound: Bound<&[u8]>) -> Bound<Vec<u8>> {
	match bound {
		Bound::Included(v) => Bound::Included(v.to_vec()),
		Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

fn insert_entries_in_tx(
	tx: &SqliteTransaction,
	table_name: &str,
	entries: &[(EncodedKey, Option<CowVec<u8>>)],
) -> SqliteResult<()> {
	let mut stmt =
		tx.prepare_cached(&format!("INSERT OR REPLACE INTO \"{}\" (key, value) VALUES (?1, ?2)", table_name))?;
	for (key, value) in entries {
		stmt.execute(params![key.as_slice(), value.as_ref().map(|v| v.as_slice())])?;
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use reifydb_testing::tempdir::temp_dir;

	use super::*;

	#[test]
	fn every_connection_reports_wal_regardless_of_what_the_caller_asked_for() {
		// Readers share the file with the writer, so a non-WAL mode makes them block each other outright.
		temp_dir(|dir| {
			let storage = SqlitePersistentStorage::new(
				SqliteConfig::new(dir.join("single.db")).journal_mode(JournalMode::Persist),
			);

			let guard = storage.inner.conn.lock();
			let conn = guard.as_ref().expect("write connection is present");
			let mode: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0)).unwrap();
			assert_eq!(mode, "wal", "an inherited journal_mode must be overridden, not applied");
			drop(guard);

			let reader = storage.inner.readers.acquire();
			let conn = reader.as_ref().expect("read connection is present");
			let mode: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0)).unwrap();
			assert_eq!(mode, "wal", "a reader that is not in wal cannot read while the writer writes");

			Ok(())
		})
		.unwrap();
	}

	#[test]
	fn the_store_opens_one_writer_and_a_reader_per_configured_pool_slot() {
		// A single shared connection serialises every read behind the writer, which is what wal avoids.
		temp_dir(|dir| {
			let storage = SqlitePersistentStorage::new(
				SqliteConfig::new(dir.join("single.db")).read_pool_size(3),
			);

			assert_eq!(storage.inner.readers.conns.len(), 3, "each pool slot must own its own connection");
			assert!(storage.inner.conn.lock().is_some(), "the single writer must be present");

			let first = storage.inner.readers.acquire();
			let second = storage.inner.readers.acquire();
			assert!(first.is_some() && second.is_some(), "two readers must be usable at the same time");

			Ok(())
		})
		.unwrap();
	}

	#[test]
	fn a_write_leaves_a_wal_companion_beside_a_fresh_database() {
		// WAL lives in the database header, so a regression survives every existing file and only shows up
		// here.
		temp_dir(|dir| {
			let storage = SqlitePersistentStorage::new(SqliteConfig::new(dir.join("single.db")));
			storage.set(vec![(EncodedKey::new(vec![1]), Some(CowVec::new(vec![2])))]).unwrap();

			assert!(
				dir.join("single.db-wal").exists(),
				"no -wal companion means the write-ahead log is gone"
			);
			assert!(
				dir.join("single.db-shm").exists(),
				"no -shm companion means the write-ahead log is gone"
			);

			Ok(())
		})
		.unwrap();
	}
}
