// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	iter::repeat_n,
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, error::diagnostic::internal::internal, interface::store::EntryKind};
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
	pragma,
};
use reifydb_value::{Result, error, util::cowvec::CowVec, value::duration::Duration};
use rusqlite::{
	Connection, Error::QueryReturnedNoRows, Result as SqliteResult, Row, ToSql, Transaction, TransactionBehavior,
	params, params_from_iter,
};
use tracing::{instrument, warn};

use crate::{
	MultiVersionScope,
	tier::{
		HistoricalCursor, RangeBatch, RangeCursor, RawEntry, TierBackend, TierBatch, TierStorage,
		VersionedGetResult,
		persistent::{
			CheckpointOutcome,
			sqlite::{
				entry::current_table_name,
				query::{
					build_create_current_sql, build_delete_below_version_sql,
					build_delete_keys_sql, build_get_current_sql, build_get_many_current_sql,
					build_range_consistent_sql, build_range_current_sql, build_upsert_current_sql,
					prefix_upper_bound, version_from_bytes, version_to_bytes,
				},
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
	checkpoint_threshold_frames: u32,
	table_sql: Map<EntryKind, Arc<TableSql>>,
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
		page_size = config.page_size.as_bytes(),
		journal_mode = %config.journal_mode.as_str()
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
				checkpoint_threshold_frames: config.wal_autocheckpoint,
				table_sql: Map::new(),
			}),
		}
	}

	pub fn maybe_checkpoint(&self) -> Result<CheckpointOutcome> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(CheckpointOutcome {
				log_frames: 0,
				restarted: false,
			});
		};

		let mut log_frames: i64 = 0;
		conn.pragma(None, "wal_checkpoint", "PASSIVE", |row| {
			log_frames = row.get(1)?;
			Ok(())
		})
		.map_err(|e| error!(internal(format!("Failed to query persistent WAL size: {}", e))))?;

		let log_frames = log_frames.max(0) as u32;
		if log_frames <= self.inner.checkpoint_threshold_frames {
			return Ok(CheckpointOutcome {
				log_frames,
				restarted: false,
			});
		}

		let mut busy: i64 = 1;
		if let Err(e) = conn.pragma(None, "wal_checkpoint", "RESTART", |row| {
			busy = row.get(0)?;
			Ok(())
		}) {
			warn!(error = %e, "persistent checkpoint: RESTART failed");
		}

		Ok(CheckpointOutcome {
			log_frames,
			restarted: busy == 0,
		})
	}

	pub fn reclaim(&self) -> Result<()> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(());
		};
		pragma::incremental_vacuum(conn)
			.map_err(|e| error!(internal(format!("Failed to reclaim persistent free pages: {}", e))))?;
		Ok(())
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
	) -> Result<Vec<EncodedKey>> {
		let table_sql = self.table_sql(table);
		let sql = build_delete_below_version_sql(&table_sql.table_name, prefix.is_some());
		let cutoff = version_to_bytes(cutoff_version);
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(Vec::new());
		};
		let mut stmt = match conn.prepare_cached(&sql) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
			Err(e) => {
				return Err(error!(internal(format!(
					"Failed to prepare delete expired for {}: {}",
					table_sql.table_name, e
				))));
			}
		};
		let map_key = |row: &Row| row.get::<_, Vec<u8>>(0);
		let rows = match prefix {
			Some(prefix) => {
				let upper = prefix_upper_bound(prefix);
				stmt.query_map(params![cutoff.as_slice(), prefix, upper.as_slice()], map_key)
			}
			None => stmt.query_map(params![cutoff.as_slice()], map_key),
		};
		let rows = match rows {
			Ok(rows) => rows,
			Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
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
		Ok(deleted)
	}

	pub fn delete_keys(&self, table: EntryKind, keys: &[EncodedKey]) -> Result<u64> {
		if keys.is_empty() {
			return Ok(0);
		}
		let table_sql = self.table_sql(table);
		let guard = self.inner.conn.lock();
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

	#[instrument(name = "store::multi::persistent::sqlite::set", level = "debug", skip(self, batches), fields(table_count = batches.len(), version = version.0))]
	pub fn set_collecting_accepted(&self, version: CommitVersion, batches: TierBatch) -> Result<Vec<EncodedKey>> {
		let mut accepted = Vec::new();
		if batches.is_empty() {
			return Ok(accepted);
		}

		let guard = self.inner.conn.lock();
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
				let value_slice = value.as_ref().map(|v| v.as_slice());
				let affected = stmt
					.execute(params![key.as_slice(), new_version_bytes.as_slice(), value_slice])
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
		let entries: Vec<RawEntry> = raw.into_iter().filter(|e| req.scope.contains(e.version)).collect();

		if entries.len() < req.batch_size {
			cursor.exhausted = true;
		}
		if let Some(last) = entries.last() {
			cursor.last_key = Some(last.key.clone());
		}

		let has_more = !cursor.exhausted;
		Ok(RangeBatch {
			entries,
			has_more,
		})
	}

	pub fn load_range_consistent(
		&self,
		table: EntryKind,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		read: CommitVersion,
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
			Ok(rows) => rows.collect::<SqliteResult<Vec<_>>>().map_err(|e| {
				error!(internal(format!("Failed to read persistent consistent row: {}", e)))
			})?,
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
	#[instrument(name = "store::multi::persistent::sqlite::get::operator", level = "trace", skip(self), fields(key_len = key.len(), version = version.0))]
	fn get_operator(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult> {
		self.get_impl(table, key, version)
	}

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

	#[instrument(name = "store::multi::persistent::sqlite::get_many::operator", level = "trace", skip(self, keys), fields(key_count = keys.len(), version = version.0))]
	fn get_many_operator(
		&self,
		table: EntryKind,
		keys: &[&[u8]],
		version: CommitVersion,
	) -> Result<Vec<VersionedGetResult>> {
		self.get_many_impl(table, keys, version)
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
			EntryKind::Operator(_) => self.get_operator(table, key, version),
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
			EntryKind::Operator(_) => self.get_many_operator(table, keys, version),
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

	fn drop(&self, _batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>>) -> Result<()> {
		// TODO: change the TierStorage interface so persistent doesn't have to expose

		panic!("SqlitePersistentStorage::drop: persistent tier has no historical chain to drop versions from");
	}

	#[instrument(name = "store::multi::persistent::sqlite::get_all_versions", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len()))]
	fn get_all_versions(&self, table: EntryKind, key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		// TODO: change the TierStorage interface to remove the

		let table_sql = self.table_sql(table);
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(Vec::new());
		};

		let result = match conn.prepare_cached(&table_sql.get_sql) {
			Ok(mut stmt) => stmt.query_row(params![key], |row| {
				let version_bytes: Vec<u8> = row.get(0)?;
				let value: Option<Vec<u8>> = row.get(1)?;
				Ok((version_from_bytes(&version_bytes), value.map(CowVec::new)))
			}),
			Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
			Err(e) => {
				return Err(error!(internal(format!(
					"Failed to prepare persistent get_all_versions: {}",
					e
				))));
			}
		};

		match result {
			Ok(row) => Ok(vec![row]),
			Err(QueryReturnedNoRows) => Ok(Vec::new()),
			Err(e) if e.to_string().contains("no such table") => Ok(Vec::new()),
			Err(e) => Err(error!(internal(format!("Failed to read persistent versions: {}", e)))),
		}
	}

	fn scan_historical_below(
		&self,
		_table: EntryKind,
		_cutoff: CommitVersion,
		_cursor: &mut HistoricalCursor,
		_batch_size: usize,
	) -> Result<Vec<(EncodedKey, CommitVersion)>> {
		// TODO: change the TierStorage interface so persistent doesn't have to expose

		panic!("SqlitePersistentStorage::scan_historical_below: persistent tier has no historical chain");
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

	use reifydb_core::interface::catalog::{id::TableId, shape::ShapeId};

	use super::*;

	fn table() -> EntryKind {
		EntryKind::Source(ShapeId::Table(TableId(1)))
	}

	fn key(n: u64) -> EncodedKey {
		EncodedKey::new(n.to_be_bytes().to_vec())
	}

	fn row(payload: &[u8]) -> CowVec<u8> {
		CowVec::new(payload.to_vec())
	}

	fn visible(s: &SqlitePersistentStorage, k: &EncodedKey) -> bool {
		s.get(table(), k.as_slice(), CommitVersion(u64::MAX)).unwrap().value().is_some()
	}

	#[test]
	fn delete_below_version_removes_rows_at_or_below_cutoff() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		// Each key written at a distinct commit version (separate set calls).
		s.set(CommitVersion(1), HashMap::from([(table(), vec![(key(1), Some(row(b"a")))])])).unwrap();
		s.set(CommitVersion(2), HashMap::from([(table(), vec![(key(2), Some(row(b"b")))])])).unwrap();
		s.set(CommitVersion(3), HashMap::from([(table(), vec![(key(3), Some(row(b"c")))])])).unwrap();
		assert_eq!(s.count_current(table()).unwrap(), 3);

		let deleted = s.delete_below_version(table(), CommitVersion(2), None).unwrap();

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
		// Phase 2b: after the created_nanos/updated_nanos indices were dropped, the version-anchored TTL
		// delete (DELETE WHERE version <= cutoff) needs an index on `version`, or GC full-scans the live
		// set on every tick. The two timestamp indices must stay gone.
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

		let deleted = s.delete_below_version(table(), CommitVersion(3), None).unwrap();

		assert_eq!(deleted.len(), 1, "only the row whose last write is at or below the cutoff is evicted");
		assert!(visible(&s, &key(1)), "a row written after the cutoff version must NOT be evicted");
		assert!(!visible(&s, &key(2)));
	}

	#[test]
	fn delete_below_version_boundary_is_inclusive() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(5), HashMap::from([(table(), vec![(key(1), Some(row(b"v5")))])])).unwrap();

		// Cutoff exactly equal to the row's version: the row IS deleted (version <= cutoff).
		let deleted = s.delete_below_version(table(), CommitVersion(5), None).unwrap();
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
		let deleted = s
			.delete_below_version(EntryKind::Source(ShapeId::Table(TableId(999))), CommitVersion(100), None)
			.unwrap();
		assert_eq!(deleted.len(), 0);
	}

	#[test]
	fn delete_below_version_with_prefix_only_touches_matching_keys() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		// Two "sides" distinguished by a leading prefix byte, both written at v1.
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

		let deleted = s.delete_below_version(table(), CommitVersion(2), Some(&[0x01])).unwrap();

		assert_eq!(deleted.len(), 1, "only the 0x01-prefixed (left) row should be deleted");
		assert!(!visible(&s, &left));
		assert!(visible(&s, &right), "the 0x02-prefixed (right) row must survive a left-only prefix sweep");
	}

	#[test]
	fn delete_below_version_returns_exactly_the_deleted_keys() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(1), HashMap::from([(table(), vec![(key(1), Some(row(b"a")))])])).unwrap();
		s.set(CommitVersion(2), HashMap::from([(table(), vec![(key(2), Some(row(b"b")))])])).unwrap();
		s.set(CommitVersion(3), HashMap::from([(table(), vec![(key(3), Some(row(b"c")))])])).unwrap();

		// The surgical GC invalidation depends on delete_below_version returning the exact keys it deleted,
		// so the read cache is invalidated per-key instead of cleared wholesale. A wrong/empty key set
		// would silently leave stale entries (or over-clear) and this assertion would catch it.
		let mut got: Vec<Vec<u8>> = s
			.delete_below_version(table(), CommitVersion(2), None)
			.unwrap()
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
}
