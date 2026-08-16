// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	iter::repeat_n,
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicU64, AtomicUsize, Ordering},
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
use reifydb_sqlite::{
	SqliteConfig, SqliteTempPathGuard,
	connection::{connect, convert_flags, resolve_db_path},
	memory::sweep_connection_cache,
	pragma,
};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	count::Count,
	error, reifydb_assertions,
	util::cowvec::CowVec,
	value::{datetime::DateTime, duration::Duration},
};
use rusqlite::{
	Connection, Error::QueryReturnedNoRows, Result as SqliteResult, Row, ToSql, Transaction, TransactionBehavior,
	params, params_from_iter,
};
use tracing::{instrument, warn};

use crate::{
	MultiVersionScope,
	tier::{
		DisplacedValues, RangeBatch, RangeCursor, RawEntry, TierBackend, TierBatch, TierStorage,
		VersionedGetResult,
		persistent::sqlite::{
			entry::{current_table_name, current_table_name_to_entry},
			query::{
				build_create_current_sql, build_delete_below_version_sql, build_delete_keys_sql,
				build_expired_keys_sql, build_get_current_sql, build_get_many_current_sql,
				build_range_consistent_sql, build_range_current_sql, build_reap_tombstones_sql,
				build_upsert_current_sql, prefix_upper_bound, version_from_bytes, version_to_bytes,
			},
		},
	},
};

const GET_MANY_CHUNK: usize = 900;

const GET_MANY_BUCKETS: [usize; 5] = [1, 8, 64, 512, GET_MANY_CHUNK];

fn bucket_key_count(len: usize) -> usize {
	for &bucket in GET_MANY_BUCKETS.iter() {
		if len <= bucket {
			return bucket;
		}
	}
	GET_MANY_CHUNK
}

const BUSY_TIMEOUT: Duration = Duration::from_milliseconds_const(200);

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
	reaped_high_water: Map<EntryKind, Arc<AtomicU64>>,
	resurrections: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SqlitePageCacheMetrics {
	pub used: ByteSize,
	pub hits: Count,
	pub misses: Count,
	pub connections_sampled: Count,
	pub connections_total: Count,
}

struct TableSql {
	table_name: String,
	get_sql: String,
	upsert_sql: String,
	create_sql: String,
}

impl TableSql {
	fn build(table: EntryKind) -> Self {
		let table_name = current_table_name(table);
		let get_sql = build_get_current_sql(&table_name);
		let upsert_sql = build_upsert_current_sql(&table_name);
		let create_sql = build_create_current_sql(&table_name);
		Self {
			table_name,
			get_sql,
			upsert_sql,
			create_sql,
		}
	}
}

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

impl SqlitePersistentStorage {
	#[instrument(name = "store::multi::persistent::sqlite::new", level = "debug", skip(config), fields(
		db_path = ?config.path,
		page_size = config.page_size.as_ref().map(|size| size.as_bytes()),
		read_pool_size = config.read_pool_size,
		journal_mode = config.journal_mode.as_ref().map(|mode| mode.as_str())
	))]
	pub fn new(config: SqliteConfig) -> Self {
		let db_path = resolve_db_path(config.path.clone(), "persistent.db");
		let flags = convert_flags(&config.flags);

		let conn = connect(&db_path, flags).expect("Failed to connect to persistent database");
		pragma::apply(&conn, &config).expect("Failed to configure persistent SQLite pragmas");
		conn.busy_timeout(BUSY_TIMEOUT.to_std()).expect("Failed to set persistent busy timeout");

		let pool_size = config.read_pool_size.max(1) as usize;
		let mut conns = Vec::with_capacity(pool_size);
		for _ in 0..pool_size {
			let reader = connect(&db_path, flags).expect("Failed to open persistent read connection");
			pragma::apply_read_only(&reader, &config)
				.expect("Failed to configure persistent read connection");
			reader.busy_timeout(BUSY_TIMEOUT.to_std()).expect("Failed to set persistent read busy timeout");
			conns.push(Mutex::new(Some(reader)));
		}

		Self {
			inner: Arc::new(SqlitePersistentStorageInner {
				conn: Mutex::new(Some(conn)),
				readers: ReadPool {
					conns,
					next: AtomicUsize::new(0),
				},
				table_sql: Map::new(),
				cache_hits: AtomicU64::new(0),
				cache_misses: AtomicU64::new(0),
				reaped_high_water: Map::new(),
				resurrections: AtomicU64::new(0),
			}),
		}
	}

	pub fn page_cache_metrics(&self) -> SqlitePageCacheMetrics {
		let mut used = 0u64;
		let mut sampled = 0u64;
		let mut sweep = |conn: &Connection| {
			let swept = sweep_connection_cache(conn);
			self.inner.cache_hits.fetch_add(swept.hits.as_u64(), Ordering::Relaxed);
			self.inner.cache_misses.fetch_add(swept.misses.as_u64(), Ordering::Relaxed);
			used += swept.used.as_bytes();
			sampled += 1;
		};
		if let Some(guard) = self.inner.conn.try_lock()
			&& let Some(conn) = guard.as_ref()
		{
			sweep(conn);
		}
		for slot in &self.inner.readers.conns {
			if let Some(guard) = slot.try_lock()
				&& let Some(conn) = guard.as_ref()
			{
				sweep(conn);
			}
		}
		SqlitePageCacheMetrics {
			used: ByteSize::from_bytes(used),
			hits: Count::new(self.inner.cache_hits.load(Ordering::Relaxed)),
			misses: Count::new(self.inner.cache_misses.load(Ordering::Relaxed)),
			connections_sampled: Count::new(sampled),
			connections_total: Count::new(1 + self.inner.readers.conns.len() as u64),
		}
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

	pub fn reap_tombstones(
		&self,
		kind: EntryKind,
		cutoff_version: CommitVersion,
		limit: usize,
	) -> Result<(u64, bool)> {
		if limit == 0 {
			return Ok((0, false));
		}
		let limit = limit.min(i64::MAX as usize);
		let table_name = &current_table_name(kind);
		let sql = build_reap_tombstones_sql(table_name, limit);
		let cutoff = version_to_bytes(cutoff_version);
		let guard = self.lock_conn();
		let Some(conn) = guard.as_ref() else {
			return Ok((0, false));
		};
		let reaped = match conn.execute(&sql, params![cutoff.as_slice()]) {
			Ok(n) => n as u64,
			Err(e) if e.to_string().contains("no such table") => return Ok((0, false)),
			Err(e) => {
				return Err(error!(internal(format!(
					"Failed to reap tombstones from {}: {}",
					table_name, e
				))));
			}
		};
		self.inner
			.reaped_high_water
			.get_or_insert_with(kind, || Arc::new(AtomicU64::new(0)))
			.fetch_max(cutoff_version.0, Ordering::Relaxed);

		Ok((reaped, reaped == limit as u64))
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

	pub fn resurrections(&self) -> u64 {
		self.inner.resurrections.load(Ordering::Relaxed)
	}

	#[cfg(reifydb_assertions)]
	fn assert_no_resurrection(
		&self,
		tx: &Transaction,
		kind: EntryKind,
		table_sql: &TableSql,
		key: &EncodedKey,
		version: CommitVersion,
	) {
		let Some(high_water) = self.inner.reaped_high_water.get(&kind) else {
			return;
		};
		let high_water = high_water.load(Ordering::Relaxed);
		if version.0 > high_water {
			return;
		}
		let exists = tx
			.prepare_cached(&table_sql.get_sql)
			.and_then(|mut check| check.exists(params![key.as_slice()]))
			.unwrap_or(true);
		if !exists {
			self.inner.resurrections.fetch_add(1, Ordering::Relaxed);
		}
		assert!(
			exists,
			"resurrection: flush inserted an absent key into {} at version {} at or below that entry's own \
			 reaped-tombstone high-water {}; every version <= a reap cutoff was already durable for that \
			 entry when the reap ran (TombstoneReap floors on the per-kind flush watermark), so this write \
			 can only rematerialize a reaped removal - the floor contract or flush monotonicity is broken \
			 (key={:?})",
			table_sql.table_name,
			version.0,
			high_water,
			key.as_slice()
		);
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

		let new_version_bytes = version_to_bytes(version);

		for (table, entries) in batches {
			let table_sql = self.table_sql(table);
			Self::create_table_if_needed(&tx, &table_sql.create_sql)
				.map_err(|e| error!(internal(format!("Failed to ensure persistent table: {}", e))))?;

			let mut stmt = tx
				.prepare_cached(&table_sql.upsert_sql)
				.map_err(|e| error!(internal(format!("Failed to prepare persistent upsert: {}", e))))?;

			for (key, value) in entries {
				reifydb_assertions! {
					self.assert_no_resurrection(&tx, table, &table_sql, &key, version);
				}
				let value_slice = value.as_ref().map(|v| v.as_slice());
				let affected = stmt
					.execute(params![
						key.as_slice(),
						new_version_bytes.as_slice(),
						value_slice,
						expiry_stamp(table, value.as_ref()).map(|at| at.to_nanos() as i64)
					])
					.map_err(|e| {
						error!(internal(format!("Failed to upsert persistent row: {}", e)))
					})?;
				if affected > 0 {
					accepted.push(key);
				}
			}
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

		for (version, batch) in batches {
			let new_version_bytes = version_to_bytes(version);
			for (table, entries) in batch {
				let table_sql = self.table_sql(table);
				Self::create_table_if_needed(&tx, &table_sql.create_sql).map_err(|e| {
					error!(internal(format!("Failed to ensure persistent table: {}", e)))
				})?;

				let mut stmt = tx.prepare_cached(&table_sql.upsert_sql).map_err(|e| {
					error!(internal(format!("Failed to prepare persistent upsert: {}", e)))
				})?;

				for (key, value) in entries {
					reifydb_assertions! {
						self.assert_no_resurrection(&tx, table, &table_sql, &key, version);
					}
					let value_slice = value.as_ref().map(|v| v.as_slice());
					let affected = stmt
						.execute(params![
							key.as_slice(),
							new_version_bytes.as_slice(),
							value_slice,
							expiry_stamp(table, value.as_ref())
								.map(|at| at.to_nanos() as i64)
						])
						.map_err(|e| {
							error!(internal(format!(
								"Failed to upsert persistent row: {}",
								e
							)))
						})?;
					if affected > 0 {
						accepted.push(key);
					}
				}
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
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let table_sql = self.table_sql(req.table);
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			cursor.exhausted = true;
			return Ok(RangeBatch::empty());
		};

		let sql = build_range_current_sql(
			&table_sql.table_name,
			bound_shape(req.start),
			bound_shape(req.end),
			cursor.last_key.is_some(),
			req.descending,
		);

		let mut stmt = match conn.prepare_cached(&sql) {
			Ok(s) => s,
			Err(e) if e.to_string().contains("no such table") => {
				cursor.exhausted = true;
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
		if let Some(k) = cursor.last_key.as_deref() {
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
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
			Err(e) => return Err(error!(internal(format!("Failed to scan persistent range: {}", e)))),
		};
		let page_was_full = raw.len() >= req.batch_size;
		let last_scanned = raw.last().map(|e| e.key.clone());
		record_page(raw.len() as u64, raw.iter().filter(|e| e.value.is_none()).count() as u64);
		let entries: Vec<RawEntry> = raw.into_iter().filter(|e| req.scope.contains(e.version)).collect();

		if !page_was_full {
			cursor.exhausted = true;
		}
		if let Some(last) = last_scanned {
			cursor.last_key = Some(last);
		}

		let has_more = !cursor.exhausted;
		Ok(RangeBatch {
			entries,
			has_more,
		})
	}

	#[instrument(name = "store::multi::persistent::sqlite::load_consistent", level = "debug", skip_all, fields(table = ?table))]
	pub fn load_range_consistent(
		&self,
		table: EntryKind,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		read: CommitVersion,
		limit: Option<usize>,
	) -> Result<Vec<RawEntry>> {
		let table_sql = self.table_sql(table);
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(Vec::new());
		};

		let sql = build_range_consistent_sql(&table_sql.table_name, bound_shape(start), bound_shape(end));

		let mut stmt = match conn.prepare_cached(&sql) {
			Ok(s) => s,
			Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
			Err(e) => {
				return Err(error!(internal(format!(
					"Failed to prepare persistent consistent range: {}",
					e
				))));
			}
		};

		let version_bytes = version_to_bytes(read).to_vec();
		let mut params: Vec<Box<dyn ToSql>> = Vec::new();
		match start {
			Bound::Included(s) | Bound::Excluded(s) => params.push(Box::new(s.to_vec())),
			Bound::Unbounded => {}
		}
		match end {
			Bound::Included(e) | Bound::Excluded(e) => params.push(Box::new(e.to_vec())),
			Bound::Unbounded => {}
		}
		params.push(Box::new(version_bytes));

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
			Ok(rows) => {
				let mut collected = Vec::new();
				for row in rows {
					let entry = row.map_err(|e| {
						error!(internal(format!(
							"Failed to read persistent consistent row: {}",
							e
						)))
					})?;
					collected.push(entry);
					if limit.is_some_and(|l| collected.len() >= l) {
						break;
					}
				}
				collected
			}
			Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
			Err(e) => {
				return Err(error!(internal(format!(
					"Failed to scan persistent consistent range: {}",
					e
				))));
			}
		};

		Ok(raw)
	}
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

	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<DisplacedValues> {
		self.set_collecting_accepted(version, batches)?;
		Ok(DisplacedValues::new())
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

impl TierBackend for SqlitePersistentStorage {}

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

	fn visible(s: &SqlitePersistentStorage, k: &EncodedKey) -> bool {
		s.get(table(), k.as_slice(), CommitVersion(u64::MAX)).unwrap().value().is_some()
	}

	#[test]
	fn a_range_does_not_fetch_the_tombstones_it_would_only_discard() {
		// The timer probe asks for one row and used to receive every tombstone in the prefix,
		// because LIMIT applies after the WHERE: measured at 1,837 rows fetched per probe, 100%
		// of them dead. Filtering in SQL is only sound because `collected_to_batch` drops a None
		// value for every scope, and the commit buffer only ever holds versions ABOVE what was
		// flushed to persistent - so a persistent tombstone has no lower tier left to shadow.
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
		assert_eq!(s.count_current(table()).unwrap(), 51, "precondition: 50 tombstones are physically present");

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
			"a tombstoned key must not surface, and the one live row must"
		);
		assert_eq!(scanned.fetched, 1, "the 50 tombstones must never cross into Rust");
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
	fn create_table_indexes_the_version_column() {
		// The version-anchored TTL delete needs an index on `version` or GC full-scans the live set on
		// every tick; the retired timestamp indices must stay gone.
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
			indices.contains(&format!("{table_name}__version")),
			"the version column must be indexed so the TTL delete seeks instead of scanning, got {indices:?}"
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
	fn reap_then_flush_of_newer_versions_records_no_resurrection() {
		// A reap floors on the flush watermark, so every later flush carries a higher version; a
		// re-insert of the reaped key is a legitimate fresh write and must not trip the tripwire.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(2), HashMap::from([(table(), vec![(key(1), None)])])).unwrap();
		s.reap_tombstones(table(), CommitVersion(2), 100).unwrap();

		s.set(
			CommitVersion(3),
			HashMap::from([(table(), vec![(key(1), Some(row(b"back"))), (key(2), Some(row(b"new")))])]),
		)
		.unwrap();

		assert_eq!(
			s.resurrections(),
			0,
			"writes above the reap high-water are ordinary flushes; counting them would make the tripwire \
			 fire on every healthy re-insert"
		);
		assert!(visible(&s, &key(1)), "a fresh write after a reaped removal must land");
	}

	#[test]
	fn a_reap_high_water_does_not_carry_across_entry_kinds() {
		// An entry with nothing pending reaps ahead of one still buffering, so a cutoff must never be charged
		// to another entry's first-time insert.
		let other = EntryKind::Source(StorageId::Table(TableId(2)));
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(5), HashMap::from([(other, vec![(key(1), None)])])).unwrap();
		s.reap_tombstones(other, CommitVersion(5), 100).unwrap();

		s.set(CommitVersion(4), HashMap::from([(table(), vec![(key(9), Some(row(b"fresh")))])])).unwrap();

		assert_eq!(
			s.resurrections(),
			0,
			"a first-ever insert into an unreaped entry is absent by definition; charging it against \
			 another entry's cutoff makes the tripwire fire on healthy writes"
		);
		assert!(visible(&s, &key(9)), "the fresh row must land");
	}

	#[cfg(reifydb_assertions)]
	#[test]
	#[should_panic(expected = "resurrection")]
	fn flush_below_the_reap_high_water_of_an_absent_key_trips_the_resurrection_assertion() {
		// Positive control: silence in the test above could otherwise mean the tripwire is wired to
		// nothing. Flushing below the reap high-water is what a reap outrunning the flush watermark does.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(5), HashMap::from([(table(), vec![(key(1), None)])])).unwrap();
		s.reap_tombstones(table(), CommitVersion(5), 100).unwrap();

		s.set(CommitVersion(4), HashMap::from([(table(), vec![(key(1), Some(row(b"ghost")))])])).unwrap();
	}

	#[cfg(reifydb_assertions)]
	#[test]
	#[should_panic(expected = "resurrection")]
	fn sweep_below_the_reap_high_water_of_an_absent_key_trips_the_resurrection_assertion() {
		// The sweep flush has its own upsert loop, so the tripwire must exist there independently or a
		// sweep-only resurrection passes silently.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(5), HashMap::from([(table(), vec![(key(1), None)])])).unwrap();
		s.reap_tombstones(table(), CommitVersion(5), 100).unwrap();

		s.persist_sweep(vec![(
			CommitVersion(4),
			HashMap::from([(table(), vec![(key(1), Some(row(b"ghost")))])]),
		)])
		.unwrap();
	}

	#[test]
	fn reap_tombstones_removes_null_valued_rows_and_leaves_live_rows() {
		// A tombstone is a row stored with no value; the reaper must physically delete only those, never
		// a live row, even one below the cutoff.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(1), HashMap::from([(table(), vec![(key(1), Some(row(b"live")))])])).unwrap();
		s.set(CommitVersion(2), HashMap::from([(table(), vec![(key(2), None)])])).unwrap();
		s.set(CommitVersion(3), HashMap::from([(table(), vec![(key(3), Some(row(b"live")))])])).unwrap();
		assert_eq!(s.count_current(table()).unwrap(), 3, "the tombstone counts as a physical row until reaped");

		let (reaped, more) = s.reap_tombstones(table(), CommitVersion(10), 100).unwrap();

		assert_eq!(reaped, 1, "only the NULL-valued row is a tombstone");
		assert!(!more, "a batch below the limit reports no remaining backlog");
		assert_eq!(s.count_current(table()).unwrap(), 2, "the tombstone row must be physically gone");
		assert!(visible(&s, &key(1)), "a live row must never be reaped");
		assert!(visible(&s, &key(3)), "a live row above the tombstone must never be reaped");
	}

	#[test]
	fn reap_tombstones_respects_the_cutoff() {
		// The flush-watermark cutoff exists to stop a tombstone being reaped while its superseding write
		// may still be unflushed.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(5), HashMap::from([(table(), vec![(key(1), None)])])).unwrap();

		let (below, _) = s.reap_tombstones(table(), CommitVersion(4), 100).unwrap();
		assert_eq!(below, 0, "a tombstone at version 5 must not be reaped under a cutoff of 4");
		assert_eq!(s.count_current(table()).unwrap(), 1, "the tombstone must still be present");

		let (at, _) = s.reap_tombstones(table(), CommitVersion(5), 100).unwrap();
		assert_eq!(at, 1, "the cutoff is inclusive: version 5 is reapable at cutoff 5");
		assert_eq!(s.count_current(table()).unwrap(), 0);
	}

	#[test]
	fn reap_tombstones_is_bounded_by_limit_and_reports_more() {
		// One reap call may physically delete at most `limit` tombstones so the write connection is never held
		// for an unbounded delete; the remaining tombstones are reported as backlog and drain on later calls.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		for n in 1..=3u64 {
			s.set(CommitVersion(n), HashMap::from([(table(), vec![(key(n), None)])])).unwrap();
		}

		let (first, more_first) = s.reap_tombstones(table(), CommitVersion(10), 2).unwrap();
		assert_eq!(first, 2, "a limit of 2 caps the first call at two tombstones");
		assert!(more_first, "hitting the limit must report backlog remaining");

		let (second, more_second) = s.reap_tombstones(table(), CommitVersion(10), 2).unwrap();
		assert_eq!(second, 1, "the third tombstone drains on the next call");
		assert!(!more_second, "a sub-limit batch reports no further backlog");
		assert_eq!(s.count_current(table()).unwrap(), 0);
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

	#[test]
	fn tombstone_discovery_uses_the_partial_index() {
		// The plain version index would scan the whole live set once most rows are below the cutoff; the
		// partial index over valueless rows keeps discovery proportional to the garbage. Asserted through
		// the query plan, never by timing.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		for n in 1..=200u64 {
			s.set(CommitVersion(n), HashMap::from([(table(), vec![(key(n), Some(row(b"live")))])]))
				.unwrap();
		}
		for n in 201..=203u64 {
			s.set(CommitVersion(n), HashMap::from([(table(), vec![(key(n), None)])])).unwrap();
		}
		let table_name = s.table_sql(table()).table_name.clone();

		let guard = s.inner.conn.lock();
		let conn = guard.as_ref().expect("write connection is present");
		conn.execute_batch("ANALYZE").unwrap();
		let sql = format!(
			"EXPLAIN QUERY PLAN SELECT key FROM \"{0}\" WHERE value IS NULL AND version <= ?1 LIMIT 100",
			table_name
		);
		let zero = [0u8; 8];
		let details: Vec<String> = conn
			.prepare(&sql)
			.unwrap()
			.query_map([zero.as_slice()], |r| r.get::<_, String>(3))
			.unwrap()
			.map(|r| r.unwrap())
			.collect();

		assert!(
			details.iter().any(|d| d.contains(&format!("{table_name}__tombstone"))),
			"reap discovery must use the partial tombstone index; query plan was {details:?}"
		);
	}
}
