// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	iter::repeat_n,
	ops::Bound,
	sync::{
		Arc, OnceLock,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::bytes::{SHAPE_HEADER_SIZE, read_updated_at},
};
#[cfg(test)]
use reifydb_core::metrics::scan::ScanCounters;
use reifydb_core::{
	common::CommitVersion, error::diagnostic::internal::internal, interface::store::EntryKind,
	metrics::scan::record_page,
};
use reifydb_runtime::{
	shutdown::Shutdown,
	sync::{
		map::Map,
		mutex::{Mutex, MutexGuard},
	},
};
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard, pragma};
use reifydb_store::{
	filter::KeyFilter,
	metrics::PageCacheMetrics,
	sqlite::{OpenMessages, open, page_cache_metrics, pool::ReadPool},
};
use reifydb_store_commit::{
	MultiVersionScope, RangeBatch, RangeCursor, RangeStop, RawEntry, TierBatch, VersionedGetResult,
};
use reifydb_value::{Result, error, util::cowvec::CowVec, value::datetime::DateTime};
use rusqlite::{
	Connection, Error::QueryReturnedNoRows, Result as SqliteResult, Row, ToSql, Transaction, TransactionBehavior,
	params, params_from_iter,
};
use tracing::{instrument, warn};

use crate::{
	filter::{ARMED_CAPACITY_KEYS, MultiKeys},
	tier::{
		TierStorage,
		persistent::sqlite::{
			entry::{current_table_name, current_table_name_to_entry},
			query::{
				build_chunked_upsert_sql, build_create_current_sql, build_current_exists_sql,
				build_current_keys_sql, build_delete_below_version_sql, build_delete_current_sql,
				build_delete_keys_sql, build_expired_keys_sql, build_get_current_sql,
				build_get_many_current_sql, build_max_version_sql, build_range_current_sql,
				build_upsert_current_sql, prefix_upper_bound, version_from_bytes, version_to_bytes,
			},
		},
	},
};

const GET_MANY_CHUNK: usize = 900;

const UPSERT_CHUNK: usize = 100;

const GET_MANY_BUCKETS: [usize; 5] = [1, 8, 64, 512, GET_MANY_CHUNK];

fn bucket_key_count(len: usize) -> usize {
	for &bucket in GET_MANY_BUCKETS.iter() {
		if len <= bucket {
			return bucket;
		}
	}
	GET_MANY_CHUNK
}

#[derive(Clone)]
pub struct SqlitePersistentStorage {
	inner: Arc<SqlitePersistentStorageInner>,
}

struct SqlitePersistentStorageInner {
	conn: Mutex<Option<Connection>>,
	readers: ReadPool,
	table_sql: Map<EntryKind, Arc<TableSql>>,
	cache_hits: AtomicU64,
	cache_misses: AtomicU64,
	filter: KeyFilter<MultiKeys>,

	written_high_water: AtomicU64,
	opened_high_water: OnceLock<u64>,
}

struct TableSql {
	table_name: String,
	get_sql: String,
	upsert_sql: String,
	chunked_upsert_sql: String,
	delete_sql: String,
	chunked_delete_sql: String,
	create_sql: String,
}

impl TableSql {
	fn build(table: EntryKind) -> Self {
		let table_name = current_table_name(table);
		let get_sql = build_get_current_sql(&table_name);
		let upsert_sql = build_upsert_current_sql(&table_name);
		let chunked_upsert_sql = build_chunked_upsert_sql(&table_name, UPSERT_CHUNK);
		let delete_sql = build_delete_current_sql(&table_name, 1, false);
		let chunked_delete_sql = build_delete_current_sql(&table_name, UPSERT_CHUNK, true);
		let create_sql = build_create_current_sql(&table_name);
		Self {
			table_name,
			get_sql,
			upsert_sql,
			chunked_upsert_sql,
			delete_sql,
			chunked_delete_sql,
			create_sql,
		}
	}
}

const OPEN_MESSAGES: OpenMessages = OpenMessages {
	connect: "Failed to connect to persistent database",
	pragmas: "Failed to configure persistent SQLite pragmas",
	busy_timeout: "Failed to set persistent busy timeout",
	read_connect: "Failed to open persistent read connection",
	read_pragmas: "Failed to configure persistent read connection",
	read_busy_timeout: "Failed to set persistent read busy timeout",
};

impl SqlitePersistentStorage {
	#[instrument(name = "store::multi::persistent::sqlite::new", level = "debug", skip(config), fields(
		db_path = ?config.path,
		page_size = config.page_size.as_ref().map(|size| size.as_bytes()),
		read_pool_size = config.read_pool_size,
		journal_mode = config.journal_mode.as_ref().map(|mode| mode.as_str())
	))]
	pub fn new(config: SqliteConfig) -> Self {
		let (conn, readers) = open(&config, "persistent.db", &OPEN_MESSAGES);

		let filter = if any_current_row(&conn) {
			KeyFilter::<MultiKeys>::new()
		} else {
			KeyFilter::<MultiKeys>::armed(ARMED_CAPACITY_KEYS)
		};

		Self {
			inner: Arc::new(SqlitePersistentStorageInner {
				conn: Mutex::new(Some(conn)),
				readers,
				table_sql: Map::new(),
				cache_hits: AtomicU64::new(0),
				cache_misses: AtomicU64::new(0),
				filter,
				written_high_water: AtomicU64::new(0),
				opened_high_water: OnceLock::new(),
			}),
		}
	}

	pub fn install_floor(&self) -> Result<CommitVersion> {
		let opened = match self.inner.opened_high_water.get() {
			Some(opened) => *opened,
			None => {
				let probed = self.probe_high_water()?;
				*self.inner.opened_high_water.get_or_init(|| probed)
			}
		};
		Ok(CommitVersion(opened.max(self.inner.written_high_water.load(Ordering::SeqCst))))
	}

	fn probe_high_water(&self) -> Result<u64> {
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Err(error!(internal(
				"Persistent storage is shut down; refusing to probe the install floor, whose \
				 fallback would gate coverage materializes on a version no read ever observed"
					.to_string()
			)));
		};
		Ok(highest_current_version(conn))
	}

	fn record_written_version(&self, version: CommitVersion) {
		self.inner.written_high_water.fetch_max(version.0, Ordering::SeqCst);
	}

	pub fn filter(&self) -> &KeyFilter<MultiKeys> {
		&self.inner.filter
	}

	pub(crate) fn current_key_slice(
		&self,
		table: EntryKind,
		cursor: Option<&EncodedKey>,
		budget: usize,
	) -> Result<Vec<EncodedKey>> {
		if budget == 0 {
			return Ok(Vec::new());
		}
		let table_sql = self.table_sql(table);
		let sql = build_current_keys_sql(&table_sql.table_name, cursor.is_some());
		let limit = budget.min(i64::MAX as usize) as i64;
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(Vec::new());
		};
		let mut stmt = match conn.prepare_cached(&sql) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
			Err(e) => {
				return Err(error!(internal(format!("Failed to prepare current key scan: {}", e))));
			}
		};
		let mut rows = match cursor {
			Some(key) => stmt.query(params![key.as_slice(), limit]),
			None => stmt.query(params![limit]),
		}
		.map_err(|e| error!(internal(format!("Failed to scan current keys: {}", e))))?;

		let mut out = Vec::with_capacity(budget);
		while let Some(row) =
			rows.next().map_err(|e| error!(internal(format!("Failed to read current key: {}", e))))?
		{
			let key: Vec<u8> = row
				.get(0)
				.map_err(|e| error!(internal(format!("Failed to decode current key: {}", e))))?;
			out.push(EncodedKey::new(key));
		}
		Ok(out)
	}

	pub fn page_cache_metrics(&self) -> PageCacheMetrics {
		page_cache_metrics(
			&self.inner.conn,
			&self.inner.readers,
			&self.inner.cache_hits,
			&self.inner.cache_misses,
		)
	}

	#[instrument(name = "store::multi::sqlite::conn_acquire", level = "debug", skip(self))]
	fn lock_conn(&self) -> MutexGuard<'_, Option<Connection>> {
		self.inner.conn.lock()
	}

	pub fn set_checkpoint_threshold(&self, frames: u32) {
		let guard = self.lock_conn();
		if let Some(conn) = guard.as_ref()
			&& let Err(e) = conn.pragma_update(None, "wal_autocheckpoint", frames)
		{
			warn!(error = %e, "failed to update wal_autocheckpoint pragma");
		}
	}

	pub fn in_memory() -> (Self, SqliteTempPathGuard) {
		let (config, guard) = SqliteConfig::in_memory();
		(Self::new(config), guard)
	}

	fn table_sql(&self, table: EntryKind) -> Arc<TableSql> {
		self.inner.table_sql.get_or_insert_with(table, || Arc::new(TableSql::build(table)))
	}

	pub fn count_current(&self, table: EntryKind) -> Result<u64> {
		let table_sql = self.table_sql(table);
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(0);
		};
		let sql = format!("SELECT COUNT(*) FROM \"{}\"", table_sql.table_name);
		match conn.query_row(&sql, [], |row| row.get::<_, i64>(0)) {
			Ok(c) => Ok(c as u64),
			Err(e) if e.to_string().contains("no such table") => Ok(0),
			Err(e) => Err(error!(internal(format!("Failed to count persistent current: {}", e)))),
		}
	}

	pub fn delete_below_version(
		&self,
		table: EntryKind,
		cutoff_version: CommitVersion,
		prefix: Option<&[u8]>,
		cursor: Option<&[u8]>,
		limit: usize,
	) -> Result<(Vec<EncodedKey>, Option<EncodedKey>)> {
		if limit == 0 {
			return Ok((Vec::new(), None));
		}
		let limit = limit.min(i64::MAX as usize);
		let table_sql = self.table_sql(table);
		let sql = build_delete_below_version_sql(
			&table_sql.table_name,
			prefix.is_some(),
			cursor.is_some(),
			limit,
		);
		let cutoff = version_to_bytes(cutoff_version);
		let upper = prefix.map(prefix_upper_bound);
		let guard = self.lock_conn();
		let Some(conn) = guard.as_ref() else {
			return Ok((Vec::new(), None));
		};
		let mut stmt = match conn.prepare_cached(&sql) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => return Ok((Vec::new(), None)),
			Err(e) => {
				return Err(error!(internal(format!(
					"Failed to prepare delete expired for {}: {}",
					table_sql.table_name, e
				))));
			}
		};
		let mut binds: Vec<&[u8]> = Vec::with_capacity(4);
		binds.push(cutoff.as_slice());
		if let Some(prefix) = prefix {
			binds.push(prefix);
			binds.push(upper.as_deref().expect("upper bound is present when a prefix is present"));
		}
		if let Some(cursor) = cursor {
			binds.push(cursor);
		}
		let map_key = |row: &Row| row.get::<_, Vec<u8>>(0);
		let rows = match stmt.query_map(params_from_iter(binds), map_key) {
			Ok(rows) => rows,
			Err(e) if e.to_string().contains("no such table") => return Ok((Vec::new(), None)),
			Err(e) => {
				return Err(error!(internal(format!(
					"Failed to delete expired persistent rows from {}: {}",
					table_sql.table_name, e
				))));
			}
		};
		let mut deleted = Vec::new();
		for row in rows {
			match row {
				Ok(key) => deleted.push(EncodedKey::new(key)),
				Err(e) => {
					return Err(error!(internal(format!(
						"Failed to read deleted key from {}: {}",
						table_sql.table_name, e
					))));
				}
			}
		}
		let next_cursor = if deleted.len() == limit {
			deleted.iter().max().cloned()
		} else {
			None
		};
		Ok((deleted, next_cursor))
	}

	pub fn delete_keys(&self, table: EntryKind, keys: &[EncodedKey]) -> Result<u64> {
		if keys.is_empty() {
			return Ok(0);
		}
		let table_sql = self.table_sql(table);
		let guard = self.lock_conn();
		let Some(conn) = guard.as_ref() else {
			return Ok(0);
		};
		let mut total = 0u64;
		for chunk in keys.chunks(GET_MANY_CHUNK) {
			let sql = build_delete_keys_sql(&table_sql.table_name, chunk.len());
			match conn.execute(&sql, params_from_iter(chunk.iter().map(|k| k.as_slice()))) {
				Ok(n) => total += n as u64,
				Err(e) if e.to_string().contains("no such table") => return Ok(total),
				Err(e) => {
					return Err(error!(internal(format!(
						"Failed to delete keys from {}: {}",
						table_sql.table_name, e
					))));
				}
			}
		}
		Ok(total)
	}

	pub fn list_current_entries(&self) -> Result<Vec<EntryKind>> {
		let guard = self.lock_conn();
		let Some(conn) = guard.as_ref() else {
			return Ok(Vec::new());
		};
		let mut stmt = conn
			.prepare_cached(
				"SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
			)
			.map_err(|e| error!(internal(format!("Failed to prepare table listing: {}", e))))?;
		let names = stmt
			.query_map([], |row| row.get::<_, String>(0))
			.map_err(|e| error!(internal(format!("Failed to list current tables: {}", e))))?;
		let mut out = Vec::new();
		for name in names {
			let name = name.map_err(|e| error!(internal(format!("Failed to read table name: {}", e))))?;
			if let Some(kind) = current_table_name_to_entry(&name) {
				out.push(kind);
			}
		}
		Ok(out)
	}

	pub fn expired_keys(
		&self,
		table: EntryKind,
		cutoff: DateTime,
		cursor: Option<(DateTime, &[u8])>,
		limit: usize,
	) -> Result<Vec<(EncodedKey, DateTime)>> {
		if limit == 0 {
			return Ok(Vec::new());
		}
		let table_sql = self.table_sql(table);
		let sql = build_expired_keys_sql(&table_sql.table_name, cursor.is_some(), limit.min(i64::MAX as usize));
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(Vec::new());
		};
		let mut stmt = match conn.prepare_cached(&sql) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
			Err(e) => {
				return Err(error!(internal(format!(
					"Failed to prepare expired keys for {}: {}",
					table_sql.table_name, e
				))));
			}
		};
		let mut params: Vec<Box<dyn ToSql>> = vec![Box::new(cutoff.to_nanos() as i64)];
		if let Some((at, key)) = cursor {
			params.push(Box::new(at.to_nanos() as i64));
			params.push(Box::new(key.to_vec()));
		}
		let rows = match stmt.query_map(params_from_iter(params), |row| {
			let key: Vec<u8> = row.get(0)?;
			let nanos: i64 = row.get(1)?;
			Ok((EncodedKey::new(key), DateTime::from_nanos(nanos as u64)))
		}) {
			Ok(rows) => rows,
			Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
			Err(e) => {
				return Err(error!(internal(format!(
					"Failed to scan expired keys from {}: {}",
					table_sql.table_name, e
				))));
			}
		};
		let mut out = Vec::new();
		for row in rows {
			out.push(row.map_err(|e| {
				error!(internal(format!(
					"Failed to read expired key from {}: {}",
					table_sql.table_name, e
				)))
			})?);
		}
		Ok(out)
	}

	fn upsert_entries_collecting_accepted(
		&self,
		tx: &Transaction,
		table: EntryKind,
		table_sql: &TableSql,
		version: CommitVersion,
		entries: &[(EncodedKey, Option<CowVec<u8>>)],
		accepted: &mut Vec<EncodedKey>,
	) -> Result<()> {
		self.record_written_version(version);
		let new_version_bytes = version_to_bytes(version);
		let mut chunk_stmt = tx
			.prepare_cached(&table_sql.chunked_upsert_sql)
			.map_err(|e| error!(internal(format!("Failed to prepare chunked persistent upsert: {}", e))))?;
		let mut single_stmt = tx
			.prepare_cached(&table_sql.upsert_sql)
			.map_err(|e| error!(internal(format!("Failed to prepare persistent upsert: {}", e))))?;

		let mut sets: Vec<&(EncodedKey, Option<CowVec<u8>>)> = Vec::with_capacity(entries.len());
		let mut removals: Vec<&EncodedKey> = Vec::new();
		for entry in entries {
			if entry.1.is_some() {
				sets.push(entry);
			} else {
				removals.push(&entry.0);
			}
		}

		let mut chunks = sets.chunks_exact(UPSERT_CHUNK);
		for chunk in chunks.by_ref() {
			let mut boxed: Vec<Box<dyn ToSql>> = Vec::with_capacity(chunk.len() * 4);
			for (key, value) in chunk.iter().copied() {
				self.inner.filter.add((table, key));
				boxed.push(Box::new(key.as_slice().to_vec()));
				boxed.push(Box::new(new_version_bytes.to_vec()));
				boxed.push(Box::new(value.as_ref().map(|v| v.as_slice().to_vec())));
				boxed.push(Box::new(
					expiry_stamp(table, value.as_ref()).map(|at| at.to_nanos() as i64),
				));
			}
			let flat: Vec<&dyn ToSql> = boxed.iter().map(|p| p.as_ref()).collect();
			let returned = chunk_stmt
				.query_map(params_from_iter(flat), |row| row.get::<_, Vec<u8>>(0))
				.map_err(|e| error!(internal(format!("Failed to upsert persistent rows: {}", e))))?;
			for key_bytes in returned {
				let key_bytes = key_bytes.map_err(|e| {
					error!(internal(format!("Failed to read accepted persistent key: {}", e)))
				})?;
				accepted.push(EncodedKey::new(key_bytes));
			}
		}

		for (key, value) in chunks.remainder().iter().copied() {
			self.inner.filter.add((table, key));
			let value_slice = value.as_ref().map(|v| v.as_slice());
			let affected = single_stmt
				.execute(params![
					key.as_slice(),
					new_version_bytes.as_slice(),
					value_slice,
					expiry_stamp(table, value.as_ref()).map(|at| at.to_nanos() as i64)
				])
				.map_err(|e| error!(internal(format!("Failed to upsert persistent row: {}", e))))?;
			if affected > 0 {
				accepted.push(key.clone());
			}
		}

		self.delete_entries_collecting_accepted(tx, table_sql, &new_version_bytes, &removals, accepted)?;

		Ok(())
	}

	fn delete_entries_collecting_accepted(
		&self,
		tx: &Transaction,
		table_sql: &TableSql,
		version_bytes: &[u8],
		removals: &[&EncodedKey],
		accepted: &mut Vec<EncodedKey>,
	) -> Result<()> {
		if removals.is_empty() {
			return Ok(());
		}
		let mut chunk_stmt = tx
			.prepare_cached(&table_sql.chunked_delete_sql)
			.map_err(|e| error!(internal(format!("Failed to prepare chunked persistent delete: {}", e))))?;
		let mut single_stmt = tx
			.prepare_cached(&table_sql.delete_sql)
			.map_err(|e| error!(internal(format!("Failed to prepare persistent delete: {}", e))))?;

		let mut chunks = removals.chunks_exact(UPSERT_CHUNK);
		for chunk in chunks.by_ref() {
			let mut boxed: Vec<Box<dyn ToSql>> = Vec::with_capacity(chunk.len() + 1);
			for key in chunk.iter() {
				boxed.push(Box::new(key.as_slice().to_vec()));
			}
			boxed.push(Box::new(version_bytes.to_vec()));
			let flat: Vec<&dyn ToSql> = boxed.iter().map(|p| p.as_ref()).collect();
			let returned = chunk_stmt
				.query_map(params_from_iter(flat), |row| row.get::<_, Vec<u8>>(0))
				.map_err(|e| error!(internal(format!("Failed to delete persistent rows: {}", e))))?;
			for key_bytes in returned {
				let key_bytes = key_bytes.map_err(|e| {
					error!(internal(format!("Failed to read deleted persistent key: {}", e)))
				})?;
				accepted.push(EncodedKey::new(key_bytes));
			}
		}

		for key in chunks.remainder().iter().copied() {
			let affected = single_stmt
				.execute(params![key.as_slice(), version_bytes])
				.map_err(|e| error!(internal(format!("Failed to delete persistent row: {}", e))))?;
			if affected > 0 {
				accepted.push((*key).clone());
			}
		}

		Ok(())
	}

	#[instrument(name = "store::multi::persistent::sqlite::set", level = "debug", skip(self, batches), fields(table_count = batches.len(), version = version.0))]
	pub fn set_collecting_accepted(&self, version: CommitVersion, batches: TierBatch) -> Result<Vec<EncodedKey>> {
		let mut accepted = Vec::new();
		if batches.is_empty() {
			return Ok(accepted);
		}

		let guard = self.lock_conn();
		let Some(conn) = guard.as_ref() else {
			return Ok(accepted);
		};
		let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
			.map_err(|e| error!(internal(format!("Failed to start persistent transaction: {}", e))))?;

		for (table, entries) in batches {
			let table_sql = self.table_sql(table);
			Self::create_table_if_needed(&tx, &table_sql.create_sql)
				.map_err(|e| error!(internal(format!("Failed to ensure persistent table: {}", e))))?;

			self.upsert_entries_collecting_accepted(
				&tx,
				table,
				&table_sql,
				version,
				&entries,
				&mut accepted,
			)?;
		}

		tx.commit().map_err(|e| error!(internal(format!("Failed to commit persistent transaction: {}", e))))?;
		Ok(accepted)
	}

	#[instrument(name = "store::multi::persistent::sqlite::persist_sweep", level = "debug", skip(self, batches), fields(batch_count = batches.len()))]
	pub fn persist_sweep(&self, batches: Vec<(CommitVersion, TierBatch)>) -> Result<Vec<EncodedKey>> {
		let mut accepted = Vec::new();
		if batches.iter().all(|(_, batch)| batch.is_empty()) {
			return Ok(accepted);
		}

		let guard = self.lock_conn();
		let Some(conn) = guard.as_ref() else {
			return Err(error!(internal(
				"Persistent storage is shut down; refusing to acknowledge a flush sweep whose \
				 writes would then be dropped from the commit buffer unpersisted"
					.to_string()
			)));
		};
		let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
			.map_err(|e| error!(internal(format!("Failed to start persistent transaction: {}", e))))?;

		let mut ensured: HashSet<EntryKind> = HashSet::new();
		for (version, batch) in batches {
			for (table, entries) in batch {
				let table_sql = self.table_sql(table);
				if ensured.insert(table) {
					Self::create_table_if_needed(&tx, &table_sql.create_sql).map_err(|e| {
						error!(internal(format!("Failed to ensure persistent table: {}", e)))
					})?;
				}

				self.upsert_entries_collecting_accepted(
					&tx,
					table,
					&table_sql,
					version,
					&entries,
					&mut accepted,
				)?;
			}
		}

		tx.commit().map_err(|e| error!(internal(format!("Failed to commit persistent transaction: {}", e))))?;
		Ok(accepted)
	}

	fn create_table_if_needed(conn: &Connection, create_sql: &str) -> SqliteResult<()> {
		conn.execute_batch(create_sql)?;
		Ok(())
	}

	fn range_chunk(&self, cursor: &mut RangeCursor, req: RangeChunkRequest<'_>) -> Result<RangeBatch> {
		if cursor.is_exhausted() {
			return Ok(RangeBatch::empty());
		}

		let table_sql = self.table_sql(req.table);
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Err(error!(internal(
				"Persistent storage is shut down; refusing to report a range chunk exhausted \
				 having read nothing, which hands the caller a short scan reported as a \
				 complete one"
					.to_string()
			)));
		};

		let sql = build_range_current_sql(
			&table_sql.table_name,
			bound_shape(req.start),
			bound_shape(req.end),
			cursor.last_key().is_some(),
			req.descending,
		);

		let mut stmt = match conn.prepare_cached(&sql) {
			Ok(s) => s,
			Err(e) if e.to_string().contains("no such table") => {
				cursor.finish_with(RangeStop::AbsentTable);
				return Ok(RangeBatch::empty());
			}
			Err(e) => return Err(error!(internal(format!("Failed to prepare persistent range: {}", e)))),
		};

		let version_bytes = version_to_bytes(req.scope.read()).to_vec();
		let limit_i64 = req.batch_size as i64;
		let mut params: Vec<Box<dyn ToSql>> = Vec::new();
		match req.start {
			Bound::Included(s) | Bound::Excluded(s) => params.push(Box::new(s.to_vec())),
			Bound::Unbounded => {}
		}
		match req.end {
			Bound::Included(e) | Bound::Excluded(e) => params.push(Box::new(e.to_vec())),
			Bound::Unbounded => {}
		}
		if let Some(k) = cursor.last_key().map(|k| k.as_slice()) {
			params.push(Box::new(k.to_vec()));
		}
		params.push(Box::new(version_bytes));
		params.push(Box::new(limit_i64));

		let raw: Vec<RawEntry> = match stmt.query_map(params_from_iter(params), |row| {
			let key: Vec<u8> = row.get(0)?;
			let version_blob: Vec<u8> = row.get(1)?;
			let value: Option<Vec<u8>> = row.get(2)?;
			Ok(RawEntry {
				key: EncodedKey::new(key),
				version: version_from_bytes(&version_blob),
				value: value.map(CowVec::new),
			})
		}) {
			Ok(rows) => rows
				.collect::<SqliteResult<Vec<_>>>()
				.map_err(|e| error!(internal(format!("Failed to read persistent row: {}", e))))?,
			Err(e) if e.to_string().contains("no such table") => {
				cursor.finish_with(RangeStop::AbsentTable);
				return Ok(RangeBatch::empty());
			}
			Err(e) => return Err(error!(internal(format!("Failed to scan persistent range: {}", e)))),
		};
		let page_was_full = raw.len() >= req.batch_size;
		let last_scanned = raw.last().map(|e| e.key.clone());
		record_page(raw.len() as u64, raw.iter().filter(|e| e.value.is_none()).count() as u64);
		let entries: Vec<RawEntry> = raw.into_iter().filter(|e| req.scope.contains(e.version)).collect();

		if let Some(last) = last_scanned {
			cursor.advance(last);
		}
		if !page_was_full {
			cursor.finish_with(RangeStop::Scanned);
		}

		let has_more = !cursor.is_exhausted();
		Ok(RangeBatch {
			entries,
			has_more,
		})
	}
}

fn any_current_row(conn: &Connection) -> bool {
	let mut stmt = conn
		.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
		.expect("persistent table listing could not be prepared");
	let names: Vec<String> = stmt
		.query_map([], |row| row.get::<_, String>(0))
		.expect("persistent table listing failed")
		.collect::<SqliteResult<Vec<String>>>()
		.expect("persistent table name could not be read");
	drop(stmt);

	for name in names {
		if current_table_name_to_entry(&name).is_none() {
			continue;
		}
		let exists: i64 = conn
			.query_row(&build_current_exists_sql(&name), [], |row| row.get(0))
			.expect("persistent existence probe failed");
		if exists != 0 {
			return true;
		}
	}
	false
}

fn highest_current_version(conn: &Connection) -> u64 {
	let mut stmt = conn
		.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
		.expect("persistent table listing could not be prepared");
	let names: Vec<String> = stmt
		.query_map([], |row| row.get::<_, String>(0))
		.expect("persistent table listing failed")
		.collect::<SqliteResult<Vec<String>>>()
		.expect("persistent table name could not be read");
	drop(stmt);

	let mut highest = 0u64;
	for name in names {
		if current_table_name_to_entry(&name).is_none() {
			continue;
		}
		let blob: Option<Vec<u8>> = conn
			.query_row(&build_max_version_sql(&name), [], |row| row.get(0))
			.expect("persistent high water probe failed");
		if let Some(blob) = blob {
			highest = highest.max(version_from_bytes(&blob).0);
		}
	}
	highest
}

fn expiry_stamp(table: EntryKind, value: Option<&CowVec<u8>>) -> Option<DateTime> {
	match (table, value) {
		(EntryKind::Source(_) | EntryKind::PartitionedSource(_), Some(row))
			if row.len() >= SHAPE_HEADER_SIZE =>
		{
			Some(read_updated_at(row))
		}
		_ => None,
	}
}

fn bound_shape(b: Bound<&[u8]>) -> Bound<()> {
	match b {
		Bound::Included(_) => Bound::Included(()),
		Bound::Excluded(_) => Bound::Excluded(()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

struct RangeChunkRequest<'a> {
	table: EntryKind,
	start: Bound<&'a [u8]>,
	end: Bound<&'a [u8]>,
	scope: MultiVersionScope,
	batch_size: usize,
	descending: bool,
}

impl SqlitePersistentStorage {
	#[instrument(name = "store::multi::persistent::sqlite::get::source", level = "trace", skip(self), fields(key_len = key.len(), version = version.0))]
	fn get_source(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult> {
		self.get_impl(table, key, version)
	}

	#[instrument(name = "store::multi::persistent::sqlite::get::multi", level = "trace", skip(self), fields(key_len = key.len(), version = version.0))]
	fn get_multi(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult> {
		self.get_impl(table, key, version)
	}

	fn get_impl(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult> {
		let table_sql = self.table_sql(table);
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(VersionedGetResult::NotFound);
		};

		let result = match conn.prepare_cached(&table_sql.get_sql) {
			Ok(mut stmt) => stmt.query_row(params![key], |row| {
				let version_bytes: Vec<u8> = row.get(0)?;
				let value: Option<Vec<u8>> = row.get(1)?;
				Ok((version_from_bytes(&version_bytes), value))
			}),
			Err(e) if e.to_string().contains("no such table") => Err(QueryReturnedNoRows),
			Err(e) => return Err(error!(internal(format!("Failed to prepare persistent get: {}", e)))),
		};

		match result {
			Ok((stored_version, value)) if stored_version <= version => Ok(match value {
				Some(v) => VersionedGetResult::Value {
					value: CowVec::new(v),
					version: stored_version,
				},
				None => VersionedGetResult::Tombstone,
			}),
			Ok(_) => Ok(VersionedGetResult::NotFound),
			Err(QueryReturnedNoRows) => Ok(VersionedGetResult::NotFound),
			Err(e) if e.to_string().contains("no such table") => Ok(VersionedGetResult::NotFound),
			Err(e) => Err(error!(internal(format!("Failed to read persistent: {}", e)))),
		}
	}

	#[instrument(name = "store::multi::persistent::sqlite::get_many::source", level = "trace", skip(self, keys), fields(key_count = keys.len(), version = version.0))]
	fn get_many_source(
		&self,
		table: EntryKind,
		keys: &[&[u8]],
		version: CommitVersion,
	) -> Result<Vec<VersionedGetResult>> {
		self.get_many_impl(table, keys, version)
	}

	#[instrument(name = "store::multi::persistent::sqlite::get_many::multi", level = "trace", skip(self, keys), fields(key_count = keys.len(), version = version.0))]
	fn get_many_multi(
		&self,
		table: EntryKind,
		keys: &[&[u8]],
		version: CommitVersion,
	) -> Result<Vec<VersionedGetResult>> {
		self.get_many_impl(table, keys, version)
	}

	fn get_many_impl(
		&self,
		table: EntryKind,
		keys: &[&[u8]],
		version: CommitVersion,
	) -> Result<Vec<VersionedGetResult>> {
		let mut out = vec![VersionedGetResult::NotFound; keys.len()];
		if keys.is_empty() {
			return Ok(out);
		}

		let index: HashMap<&[u8], usize> = keys.iter().enumerate().map(|(i, &k)| (k, i)).collect();
		let table_sql = self.table_sql(table);
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(out);
		};

		for chunk in keys.chunks(GET_MANY_CHUNK) {
			let bucket = bucket_key_count(chunk.len());
			let sql = build_get_many_current_sql(&table_sql.table_name, bucket);
			let mut stmt = match conn.prepare_cached(&sql) {
				Ok(stmt) => stmt,
				Err(e) if e.to_string().contains("no such table") => return Ok(out),
				Err(e) => {
					return Err(error!(internal(format!(
						"Failed to prepare persistent get_many: {}",
						e
					))));
				}
			};

			let pad_key = chunk[0];
			let padded = chunk.iter().copied().chain(repeat_n(pad_key, bucket - chunk.len()));
			let mut rows = stmt
				.query(params_from_iter(padded))
				.map_err(|e| error!(internal(format!("Failed to query persistent get_many: {}", e))))?;

			while let Some(row) = rows.next().map_err(|e| {
				error!(internal(format!("Failed to read persistent get_many row: {}", e)))
			})? {
				let key_ref = row.get_ref(0).map_err(|e| {
					error!(internal(format!("Failed to read persistent get_many key: {}", e)))
				})?;
				let key = key_ref.as_blob().map_err(|e| {
					error!(internal(format!("Failed to decode persistent get_many key: {}", e)))
				})?;
				let Some(&i) = index.get(key) else {
					continue;
				};
				let version_ref = row.get_ref(1).map_err(|e| {
					error!(internal(format!("Failed to read persistent get_many version: {}", e)))
				})?;
				let version_bytes = version_ref.as_blob().map_err(|e| {
					error!(internal(format!("Failed to decode persistent get_many version: {}", e)))
				})?;
				let stored_version = version_from_bytes(version_bytes);
				if stored_version > version {
					continue;
				}
				let value: Option<Vec<u8>> = row.get(2).map_err(|e| {
					error!(internal(format!("Failed to read persistent get_many value: {}", e)))
				})?;
				out[i] = match value {
					Some(v) => VersionedGetResult::Value {
						value: CowVec::new(v),
						version: stored_version,
					},
					None => VersionedGetResult::Tombstone,
				};
			}
		}

		Ok(out)
	}
}

impl TierStorage for SqlitePersistentStorage {
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult> {
		match table {
			EntryKind::Source(_) => self.get_source(table, key, version),
			_ => self.get_multi(table, key, version),
		}
	}

	fn get_many(
		&self,
		table: EntryKind,
		keys: &[&[u8]],
		version: CommitVersion,
	) -> Result<Vec<VersionedGetResult>> {
		match table {
			EntryKind::Source(_) => self.get_many_source(table, keys, version),
			_ => self.get_many_multi(table, keys, version),
		}
	}

	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<()> {
		self.set_collecting_accepted(version, batches)?;
		Ok(())
	}

	#[instrument(name = "store::multi::persistent::sqlite::range", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size))]
	fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch> {
		self.range_chunk(
			cursor,
			RangeChunkRequest {
				table,
				start,
				end,
				scope,
				batch_size,
				descending: false,
			},
		)
	}

	#[instrument(name = "store::multi::persistent::sqlite::range_rev", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size))]
	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch> {
		self.range_chunk(
			cursor,
			RangeChunkRequest {
				table,
				start,
				end,
				scope,
				batch_size,
				descending: true,
			},
		)
	}

	fn ensure_table(&self, table: EntryKind) -> Result<()> {
		let table_sql = self.table_sql(table);
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(());
		};
		Self::create_table_if_needed(conn, &table_sql.create_sql)
			.map_err(|e| error!(internal(format!("Failed to ensure persistent table: {}", e))))
	}

	fn clear_table(&self, table: EntryKind) -> Result<()> {
		let table_sql = self.table_sql(table);
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(());
		};
		let result = conn.execute(&format!("DELETE FROM \"{}\"", table_sql.table_name), []);
		if let Err(e) = result
			&& !e.to_string().contains("no such table")
		{
			return Err(error!(internal(format!(
				"Failed to clear persistent {}: {}",
				table_sql.table_name, e
			))));
		}
		Ok(())
	}
}

impl Shutdown for SqlitePersistentStorage {
	fn shutdown(&self) {
		if let Some(conn) = self.inner.conn.lock().take() {
			if let Err(e) = pragma::shutdown(&conn) {
				warn!(error = %e, "persistent close: pragma shutdown failed");
			}
			drop(conn);
		}
		self.inner.readers.shutdown();
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use reifydb_core::interface::catalog::{id::TableId, storage::StorageId};

	use super::*;

	fn table() -> EntryKind {
		EntryKind::Source(StorageId::Table(TableId(1)))
	}

	fn key(n: u64) -> EncodedKey {
		EncodedKey::new(n.to_be_bytes())
	}

	fn row(payload: &[u8]) -> CowVec<u8> {
		CowVec::new(payload.to_vec())
	}

	fn stamped(nanos: u64) -> CowVec<u8> {
		// A body shorter than a full shape header must never be read as carrying an expiry stamp.
		let mut bytes = vec![0u8; SHAPE_HEADER_SIZE];
		bytes[16..24].copy_from_slice(&nanos.to_le_bytes());
		CowVec::new(bytes)
	}

	fn at(nanos: u64) -> DateTime {
		DateTime::from_nanos(nanos)
	}

	fn expired_at(s: &SqlitePersistentStorage, kind: EntryKind, cutoff: u64) -> Vec<u64> {
		s.expired_keys(kind, at(cutoff), None, 100)
			.unwrap()
			.into_iter()
			.map(|(key, _)| u64::from_be_bytes(key.as_slice().try_into().unwrap()))
			.collect()
	}

	fn reap(s: &SqlitePersistentStorage, keys: &[u64]) -> u64 {
		let keys: Vec<EncodedKey> = keys.iter().map(|n| key(*n)).collect();
		s.delete_keys(table(), &keys).unwrap()
	}

	fn stored_keys(s: &SqlitePersistentStorage) -> Vec<u64> {
		// Reads the raw table so a physical delete is distinguishable from a tombstone.
		let table_name = s.table_sql(table()).table_name.clone();
		let guard = s.inner.conn.lock();
		let conn = guard.as_ref().expect("write connection is present");
		let mut stmt = conn.prepare(&format!("SELECT key FROM \"{}\" ORDER BY key", table_name)).unwrap();
		let keys: Vec<u64> = stmt
			.query_map([], |row| row.get::<_, Vec<u8>>(0))
			.unwrap()
			.map(|key| u64::from_be_bytes(key.unwrap().as_slice().try_into().unwrap()))
			.collect();
		keys
	}

	fn visible(s: &SqlitePersistentStorage, k: &EncodedKey) -> bool {
		s.get(table(), k.as_slice(), CommitVersion(u64::MAX)).unwrap().value().is_some()
	}

	#[test]
	fn a_range_does_not_fetch_deleted_rows_because_the_delete_removed_them() {
		// The timer probe asks for one row and used to receive every dead row in the prefix,
		// because LIMIT applies after the WHERE: measured at 1,837 rows fetched per probe, 100%
		// of them dead. A removal now deletes the row outright rather than rewriting it to a
		// none value, so the dead rows are not there to be scanned past in the first place.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		let mut writes = Vec::new();
		for i in 1..=50u64 {
			writes.push((key(i), Some(row(b"doomed"))));
		}
		s.set(CommitVersion(1), HashMap::from([(table(), writes)])).unwrap();
		let mut deletes = Vec::new();
		for i in 1..=50u64 {
			deletes.push((key(i), None));
		}
		s.set(CommitVersion(2), HashMap::from([(table(), deletes)])).unwrap();
		s.set(CommitVersion(3), HashMap::from([(table(), vec![(key(200), Some(row(b"alive")))])])).unwrap();
		assert_eq!(
			s.count_current(table()).unwrap(),
			1,
			"the 50 removals must delete their rows outright; rewriting them to a none value \
			 leaves 50 dead rows for every later scan to page through and discard"
		);

		let before = ScanCounters::sample();
		let mut cursor = RangeCursor::default();
		let batch = s
			.range_next(
				table(),
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: CommitVersion(10),
				},
				1024,
			)
			.unwrap();
		let scanned = before.since();

		assert_eq!(
			batch.entries.iter().map(|e| e.key.clone()).collect::<Vec<_>>(),
			vec![key(200)],
			"a deleted key must not surface, and the one live row must"
		);
		assert_eq!(scanned.fetched, 1, "the 50 deleted rows must never cross into Rust");
		assert_eq!(scanned.tombstones, 0);
	}

	#[test]
	fn a_page_the_scope_filter_empties_does_not_end_the_scan() {
		// The SQL only bounds version <= read, but Between also demands version > after, so the
		// surviving-row count is not evidence about whether sqlite has more rows. Deciding
		// exhaustion from it stops the scan on the first page that filters out, silently dropping
		// every later match; resuming from the last surviving key instead of the last scanned key
		// would re-read the filtered rows forever. Keys 1-2 fill a whole page and all fail the
		// filter, so a correct cursor must still reach keys 3-4.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(
			CommitVersion(1),
			HashMap::from([(table(), vec![(key(1), Some(row(b"a"))), (key(2), Some(row(b"b")))])]),
		)
		.unwrap();
		s.set(
			CommitVersion(5),
			HashMap::from([(table(), vec![(key(3), Some(row(b"c"))), (key(4), Some(row(b"d")))])]),
		)
		.unwrap();

		let scope = MultiVersionScope::Between {
			after: CommitVersion(1),
			read: CommitVersion(10),
		};
		let mut cursor = RangeCursor::default();
		let mut seen: Vec<EncodedKey> = Vec::new();
		loop {
			let batch = s
				.range_next(table(), &mut cursor, Bound::Unbounded, Bound::Unbounded, scope, 2)
				.unwrap();
			seen.extend(batch.entries.iter().map(|e| e.key.clone()));
			if !batch.has_more {
				break;
			}
		}

		assert_eq!(
			seen,
			vec![key(3), key(4)],
			"rows newer than `after` must survive a page that filtered out entirely"
		);
	}

	#[test]
	fn page_cache_metrics_accumulates_hits_and_misses_across_sweeps() {
		// A sweep drains the per-connection counters take-and-reset into store totals, so the reported
		// counts must be monotone; raw per-connection reads would report a sawtooth, not a hit rate.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(1), HashMap::from([(table(), vec![(key(1), Some(row(b"a")))])])).unwrap();
		assert!(visible(&s, &key(1)));

		let first = s.page_cache_metrics();
		assert_eq!(
			first.connections_sampled, first.connections_total,
			"an idle pool must have every connection sampled"
		);
		assert!(
			first.hits.as_u64() + first.misses.as_u64() > 0,
			"writing and reading a row must touch the page cache, got {first:?}"
		);
		assert!(first.used.as_bytes() > 0, "connections holding pages must report used bytes");

		assert!(visible(&s, &key(1)));
		let second = s.page_cache_metrics();
		assert!(
			second.hits.as_u64() >= first.hits.as_u64(),
			"hit totals must accumulate, got {} then {}",
			first.hits.as_u64(),
			second.hits.as_u64()
		);
		assert!(
			second.misses.as_u64() >= first.misses.as_u64(),
			"miss totals must accumulate, got {} then {}",
			first.misses.as_u64(),
			second.misses.as_u64()
		);
	}

	#[test]
	fn delete_below_version_removes_rows_at_or_below_cutoff() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		// Each key written at a distinct commit version (separate set calls).
		s.set(CommitVersion(1), HashMap::from([(table(), vec![(key(1), Some(row(b"a")))])])).unwrap();
		s.set(CommitVersion(2), HashMap::from([(table(), vec![(key(2), Some(row(b"b")))])])).unwrap();
		s.set(CommitVersion(3), HashMap::from([(table(), vec![(key(3), Some(row(b"c")))])])).unwrap();
		assert_eq!(s.count_current(table()).unwrap(), 3);

		let (deleted, _) = s.delete_below_version(table(), CommitVersion(2), None, None, usize::MAX).unwrap();

		assert_eq!(deleted.len(), 2, "rows whose version is <= cutoff(2) must be physically deleted");
		assert_eq!(
			s.count_current(table()).unwrap(),
			1,
			"deletion must reclaim sqlite rows, not tombstone them"
		);
		assert!(!visible(&s, &key(1)));
		assert!(!visible(&s, &key(2)));
		assert!(visible(&s, &key(3)), "a row written after the cutoff version must survive");
	}

	#[test]
	fn create_table_leaves_the_version_column_unindexed() {
		// No query plan selects a bare version index, so recreating it costs a b-tree write per row on every
		// source table for nothing.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(1), HashMap::from([(table(), vec![(key(1), Some(row(b"a")))])])).unwrap();

		let table_name = s.table_sql(table()).table_name.clone();
		let guard = s.inner.conn.lock();
		let conn = guard.as_ref().expect("write connection is present");

		let indices: Vec<String> = conn
			.prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ?1")
			.unwrap()
			.query_map([table_name.as_str()], |r| r.get::<_, String>(0))
			.unwrap()
			.map(|r| r.unwrap())
			.collect();

		assert!(
			!indices.contains(&format!("{table_name}__version")),
			"the bare version index must not be created; it is written on every row and read by no query plan, got {indices:?}"
		);
		assert!(
			!indices.contains(&format!("{table_name}__tombstone")),
			"the tombstone index must not be created; a removal deletes its row outright, so nothing \
			 writes a valueless row for it to index, got {indices:?}"
		);
		assert!(
			!indices.iter().any(|n| n.ends_with("__created_nanos") || n.ends_with("__updated_nanos")),
			"the dropped timestamp indices must not be recreated, got {indices:?}"
		);
	}

	#[test]
	fn delete_below_version_keeps_rows_written_after_the_cutoff() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(2), HashMap::from([(table(), vec![(key(2), Some(row(b"stale")))])])).unwrap();
		s.set(CommitVersion(5), HashMap::from([(table(), vec![(key(1), Some(row(b"fresh")))])])).unwrap();

		let (deleted, _) = s.delete_below_version(table(), CommitVersion(3), None, None, usize::MAX).unwrap();

		assert_eq!(deleted.len(), 1, "only the row whose last write is at or below the cutoff is evicted");
		assert!(visible(&s, &key(1)), "a row written after the cutoff version must NOT be evicted");
		assert!(!visible(&s, &key(2)));
	}

	#[test]
	fn delete_below_version_boundary_is_inclusive() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(5), HashMap::from([(table(), vec![(key(1), Some(row(b"v5")))])])).unwrap();

		let (deleted, _) = s.delete_below_version(table(), CommitVersion(5), None, None, usize::MAX).unwrap();
		assert_eq!(
			deleted.len(),
			1,
			"a row whose version equals the cutoff is evicted (the bound is inclusive)"
		);
		assert!(!visible(&s, &key(1)));
	}

	#[test]
	fn delete_below_version_on_missing_table_is_noop() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		let (deleted, _) = s
			.delete_below_version(
				EntryKind::Source(StorageId::Table(TableId(999))),
				CommitVersion(100),
				None,
				None,
				usize::MAX,
			)
			.unwrap();
		assert_eq!(deleted.len(), 0);
	}

	#[test]
	fn delete_below_version_with_prefix_only_touches_matching_keys() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		let left = EncodedKey::new(vec![0x01, 0xAA]);
		let right = EncodedKey::new(vec![0x02, 0xBB]);
		s.set(
			CommitVersion(1),
			HashMap::from([(
				table(),
				vec![(left.clone(), Some(row(b"l"))), (right.clone(), Some(row(b"r")))],
			)]),
		)
		.unwrap();

		let (deleted, _) =
			s.delete_below_version(table(), CommitVersion(2), Some(&[0x01]), None, usize::MAX).unwrap();

		assert_eq!(deleted.len(), 1, "only the 0x01-prefixed (left) row should be deleted");
		assert!(!visible(&s, &left));
		assert!(visible(&s, &right), "the 0x02-prefixed (right) row must survive a left-only prefix sweep");
	}

	#[test]
	fn delete_below_version_returns_exactly_the_deleted_keys() {
		// GC invalidates the read cache per-key from this return value, so a wrong or empty key set
		// silently leaves stale entries or over-clears the cache.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(1), HashMap::from([(table(), vec![(key(1), Some(row(b"a")))])])).unwrap();
		s.set(CommitVersion(2), HashMap::from([(table(), vec![(key(2), Some(row(b"b")))])])).unwrap();
		s.set(CommitVersion(3), HashMap::from([(table(), vec![(key(3), Some(row(b"c")))])])).unwrap();

		let mut got: Vec<Vec<u8>> = s
			.delete_below_version(table(), CommitVersion(2), None, None, usize::MAX)
			.unwrap()
			.0
			.iter()
			.map(|k| k.to_vec())
			.collect();
		got.sort();
		let mut want = vec![key(1).to_vec(), key(2).to_vec()];
		want.sort();
		assert_eq!(
			got, want,
			"delete_below_version must return every key it physically deleted, and only those"
		);
		assert!(visible(&s, &key(3)), "the row newer than the cutoff must neither be deleted nor returned");
	}

	#[test]
	fn delete_below_version_caps_one_call_and_reports_a_resume_cursor() {
		// One call deletes at most `limit` rows and hands back a cursor, so the sole write connection is
		// never held for an unbounded delete.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		for n in 1..=5u64 {
			s.set(CommitVersion(n), HashMap::from([(table(), vec![(key(n), Some(row(b"x")))])])).unwrap();
		}
		assert_eq!(s.count_current(table()).unwrap(), 5);

		let (deleted, cursor) = s.delete_below_version(table(), CommitVersion(5), None, None, 2).unwrap();

		assert_eq!(deleted.len(), 2, "a limit of 2 must delete exactly two rows in one call");
		assert_eq!(s.count_current(table()).unwrap(), 3, "only the two capped rows may be physically gone");
		assert_eq!(
			cursor,
			Some(key(2)),
			"hitting the cap must return the largest deleted key so the next slice resumes above it"
		);
		assert!(!visible(&s, &key(1)));
		assert!(!visible(&s, &key(2)));
		assert!(visible(&s, &key(3)), "the first uncapped key must still be present");
	}

	#[test]
	fn delete_below_version_resumes_from_cursor_and_drains_without_gaps() {
		// Threading the returned cursor must walk the eligible set exactly once in key order, never
		// skipping a key between batches.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		for n in 1..=5u64 {
			s.set(CommitVersion(n), HashMap::from([(table(), vec![(key(n), Some(row(b"x")))])])).unwrap();
		}

		let mut cursor = None;
		let mut all: Vec<Vec<u8>> = Vec::new();
		let mut calls = 0;
		loop {
			let (deleted, next) =
				s.delete_below_version(table(), CommitVersion(5), None, cursor.as_deref(), 2).unwrap();
			calls += 1;
			all.extend(deleted.iter().map(|k| k.to_vec()));
			match next {
				Some(k) => cursor = Some(k.to_vec()),
				None => break,
			}
		}

		let mut want = (1..=5u64).map(|n| key(n).to_vec()).collect::<Vec<_>>();
		want.sort();
		all.sort();
		assert_eq!(all, want, "resuming from the cursor must delete every eligible key exactly once, no gaps");
		assert_eq!(calls, 3, "5 rows at limit 2 must drain in ceil(5/2) = 3 calls (2 + 2 + 1)");
		assert_eq!(s.count_current(table()).unwrap(), 0);
	}

	#[test]
	fn a_key_written_again_after_a_removal_comes_back() {
		// A removal must leave no row behind, or the parked one wins the CAS and strands the key absent.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(2), HashMap::from([(table(), vec![(key(1), None)])])).unwrap();

		s.set(
			CommitVersion(3),
			HashMap::from([(table(), vec![(key(1), Some(row(b"back"))), (key(2), Some(row(b"new")))])]),
		)
		.unwrap();

		assert!(visible(&s, &key(1)), "a fresh write after a removal must land");
		assert!(visible(&s, &key(2)), "an unrelated key in the same batch must land too");
	}

	#[test]
	fn a_chunked_upsert_batch_reports_exactly_the_keys_that_won_their_cas() {
		// a key marked accepted despite losing its CAS is evicted from memory while never persisted
		let (s, _guard) = SqlitePersistentStorage::in_memory();

		let evens: Vec<_> = (0..170u64).step_by(2).map(|i| (key(i), Some(row(b"seed-even")))).collect();
		let odds: Vec<_> = (1..170u64).step_by(2).map(|i| (key(i), Some(row(b"seed-odd")))).collect();
		s.set_collecting_accepted(CommitVersion(200), HashMap::from([(table(), evens)])).unwrap();
		s.set_collecting_accepted(CommitVersion(50), HashMap::from([(table(), odds)])).unwrap();

		let attempt: Vec<_> = (0..170u64).map(|i| (key(i), Some(row(b"attempt")))).collect();
		let accepted =
			s.set_collecting_accepted(CommitVersion(150), HashMap::from([(table(), attempt)])).unwrap();

		let mut accepted_ids: Vec<u64> =
			accepted.iter().map(|k| u64::from_be_bytes(k.as_slice().try_into().unwrap())).collect();
		accepted_ids.sort();
		let expected_odds: Vec<u64> = (1..170u64).step_by(2).collect();
		assert_eq!(
			accepted_ids, expected_odds,
			"only the keys whose stored version (50) lost to this batch's version (150) may be reported \
			 accepted"
		);

		for i in (0..170u64).step_by(2) {
			let value = s.get(table(), key(i).as_slice(), CommitVersion(u64::MAX)).unwrap().value();
			assert_eq!(
				value.as_ref().map(|v| v.as_slice()),
				Some(&b"seed-even"[..]),
				"key {i} lost its CAS (stored version 200 >= batch version 150) and must be untouched"
			);
		}
		for i in (1..170u64).step_by(2) {
			let value = s.get(table(), key(i).as_slice(), CommitVersion(u64::MAX)).unwrap().value();
			assert_eq!(
				value.as_ref().map(|v| v.as_slice()),
				Some(&b"attempt"[..]),
				"key {i} won its CAS (stored version 50 < batch version 150) and must carry this batch's \
				 value"
			);
		}
	}

	#[test]
	fn a_write_below_a_removals_version_still_lands_because_versions_arrive_out_of_order() {
		// Batches reach this tier unordered, so a lower version after a removal is an ordinary write.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(5), HashMap::from([(table(), vec![(key(1), None)])])).unwrap();

		s.set(CommitVersion(4), HashMap::from([(table(), vec![(key(1), Some(row(b"earlier")))])])).unwrap();

		assert!(visible(&s, &key(1)), "the row must land: nothing on disk outranks it after the removal");
	}

	#[test]
	fn persist_sweep_errors_when_storage_is_shut_down() {
		// The sweep hands over the only copy of a row: the commit buffer drops it on the strength of this
		// call returning Ok. A shut-down storage that reported success would lose the row silently.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.shutdown();

		let batches = vec![(CommitVersion(1), HashMap::from([(table(), vec![(key(1), Some(row(b"v")))])]))];

		assert!(
			s.persist_sweep(batches).is_err(),
			"a shut-down persistent tier must refuse the sweep loudly so the buffer is not dropped"
		);
	}

	#[test]
	fn expired_keys_returns_rows_at_or_below_the_cutoff_oldest_first() {
		// Eviction drains from the head, so youngest-first would strand the oldest rows forever.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(
			CommitVersion(1),
			HashMap::from([(
				table(),
				vec![
					(key(1), Some(stamped(300))),
					(key(2), Some(stamped(100))),
					(key(3), Some(stamped(200))),
					(key(4), Some(stamped(500))),
				],
			)]),
		)
		.unwrap();

		assert_eq!(
			expired_at(&s, table(), 300),
			vec![2, 3, 1],
			"candidates must come back ordered by their own stamp, oldest first, cutoff inclusive"
		);
		assert_eq!(expired_at(&s, table(), 99), Vec::<u64>::new(), "a cutoff below every stamp yields nothing");
	}

	#[test]
	fn expired_keys_never_returns_a_tombstone() {
		// Otherwise the evictor re-deletes a dead key forever and never advances past it.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(1), HashMap::from([(table(), vec![(key(1), Some(stamped(100)))])])).unwrap();
		s.set(CommitVersion(2), HashMap::from([(table(), vec![(key(1), None)])])).unwrap();

		assert_eq!(
			expired_at(&s, table(), 1_000),
			Vec::<u64>::new(),
			"a valueless row must not surface as an expiry candidate"
		);
	}

	#[test]
	fn a_fresh_write_clears_an_earlier_expiry_stamp() {
		// Without this the index is unsound under UPDATE: a row rewritten inside its ttl still dies.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(1), HashMap::from([(table(), vec![(key(1), Some(stamped(100)))])])).unwrap();
		assert_eq!(expired_at(&s, table(), 150), vec![1], "precondition: the row starts out expired");

		s.set(CommitVersion(2), HashMap::from([(table(), vec![(key(1), Some(stamped(900)))])])).unwrap();

		assert_eq!(
			expired_at(&s, table(), 150),
			Vec::<u64>::new(),
			"rewriting the row must carry its new stamp into the index, not leave the stale one"
		);
		assert_eq!(expired_at(&s, table(), 900), vec![1], "and the row expires again against the new stamp");
	}

	#[test]
	fn expired_keys_ignores_entries_whose_rows_carry_no_stamp() {
		// Otherwise catalog bytes read as a timestamp hand the evictor arbitrary rows to delete.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(1), HashMap::from([(EntryKind::Multi, vec![(key(1), Some(stamped(100)))])]))
			.unwrap();

		assert_eq!(
			expired_at(&s, EntryKind::Multi, 1_000),
			Vec::<u64>::new(),
			"a non-row entry must never produce expiry candidates, whatever its bytes look like"
		);
	}

	#[test]
	fn expired_keys_resumes_from_the_cursor_without_gaps_or_repeats() {
		// A candidate the evictor cannot remove must never stall every older row behind it.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(
			CommitVersion(1),
			HashMap::from([(
				table(),
				vec![
					(key(1), Some(stamped(100))),
					(key(2), Some(stamped(100))),
					(key(3), Some(stamped(200))),
				],
			)]),
		)
		.unwrap();

		let mut seen = Vec::new();
		let mut cursor: Option<(DateTime, EncodedKey)> = None;
		loop {
			let batch = s
				.expired_keys(table(), at(1_000), cursor.as_ref().map(|(a, k)| (*a, k.as_slice())), 1)
				.unwrap();
			let Some((k, a)) = batch.last().cloned() else {
				break;
			};
			seen.push(u64::from_be_bytes(k.as_slice().try_into().unwrap()));
			cursor = Some((a, k));
		}

		assert_eq!(
			seen,
			vec![1, 2, 3],
			"threading the cursor must walk every candidate exactly once, in order"
		);
	}

	#[test]
	fn delete_keys_removes_the_row_outright_leaving_no_tombstone() {
		// A reaped row must vanish, not become a tombstone the reaper would then have to clear again.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(
			CommitVersion(1),
			HashMap::from([(
				table(),
				vec![
					(key(1), Some(stamped(100))),
					(key(2), Some(stamped(200))),
					(key(3), Some(stamped(500))),
				],
			)]),
		)
		.unwrap();

		assert_eq!(reap(&s, &[1, 2]), 2, "every named key must be removed");
		assert_eq!(stored_keys(&s), vec![3], "a reaped key must leave no row behind, not even a tombstone");
		assert_eq!(
			expired_at(&s, table(), 1_000),
			vec![3],
			"a reaped key must not resurface as an expiry candidate"
		);
	}

	#[test]
	fn expiry_discovery_uses_the_partial_index() {
		// Without the index this full-scans the live set on every batch, the exact cost it removes.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		for n in 1..=200u64 {
			s.set(CommitVersion(n), HashMap::from([(table(), vec![(key(n), Some(stamped(n)))])])).unwrap();
		}
		let table_name = s.table_sql(table()).table_name.clone();

		let guard = s.inner.conn.lock();
		let conn = guard.as_ref().expect("write connection is present");
		conn.execute_batch("ANALYZE").unwrap();
		let sql = format!("EXPLAIN QUERY PLAN {}", build_expired_keys_sql(&table_name, false, 100));
		let details: Vec<String> = conn
			.prepare(&sql)
			.unwrap()
			.query_map([0i64], |r| r.get::<_, String>(3))
			.unwrap()
			.map(|r| r.unwrap())
			.collect();

		assert!(
			details.iter().any(|d| d.contains(&format!("{table_name}__expiry"))),
			"expiry discovery must use the partial expiry index; query plan was {details:?}"
		);
	}
}
