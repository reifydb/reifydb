// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::Instant,
};

use reifydb_core::{
	common::CommitVersion, encoded::key::EncodedKey, error::diagnostic::internal::internal,
	interface::store::EntryKind,
};
use reifydb_runtime::sync::mutex::{Mutex, MutexGuard};
use reifydb_sqlite::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	pragma,
};
use reifydb_type::{Result, error, util::cowvec::CowVec};
use rusqlite::{Connection, Error::QueryReturnedNoRows, Result as SqliteResult, ToSql, params, params_from_iter};
use tracing::instrument;

use super::{
	entry::warm_current_table_name,
	query::{
		build_create_warm_current_sql, build_get_many_warm_current_sql, build_get_warm_current_sql,
		build_range_warm_current_sql, build_upsert_warm_current_sql, version_from_bytes, version_to_bytes,
	},
};
use crate::tier::{
	HistoricalCursor, RangeBatch, RangeCursor, RawEntry, TierBackend, TierBatch, TierStorage, VersionedGetResult,
};

const SQLITE_SET_DEBUG_THRESHOLD_US: u128 = 5_000;

const GET_MANY_CHUNK: usize = 900;

struct ReadTimer(Instant);

impl ReadTimer {
	fn start() -> Self {
		Self(Instant::now())
	}
}

impl Drop for ReadTimer {
	fn drop(&mut self) {
		use std::sync::atomic::Ordering::Relaxed;
		crate::debug_counters::SQLITE_READ_NANOS.fetch_add(self.0.elapsed().as_nanos() as u64, Relaxed);
		crate::debug_counters::SQLITE_READ_COUNT.fetch_add(1, Relaxed);
	}
}

#[derive(Clone)]
pub struct SqlitePersistentStorage {
	inner: Arc<SqlitePersistentStorageInner>,
}

struct SqlitePersistentStorageInner {
	conn: Mutex<Connection>,
	readers: ReadPool,
}

struct ReadPool {
	conns: Vec<Mutex<Connection>>,
	next: AtomicUsize,
}

impl ReadPool {
	fn acquire(&self) -> MutexGuard<'_, Connection> {
		let n = self.conns.len();
		let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
		for i in 0..n {
			if let Some(guard) = self.conns[(start + i) % n].try_lock() {
				return guard;
			}
		}
		self.conns[start].lock()
	}
}

impl SqlitePersistentStorage {
	#[instrument(name = "store::multi::persistent::sqlite::new", level = "debug", skip(config), fields(
		db_path = ?config.path,
		page_size = config.page_size,
		journal_mode = %config.journal_mode.as_str()
	))]
	pub fn new(config: SqliteConfig) -> Self {
		let db_path = resolve_db_path(config.path.clone(), "persistent.db");
		let flags = convert_flags(&config.flags);

		let conn = connect(&db_path, flags).expect("Failed to connect to persistent database");
		pragma::apply(&conn, &config).expect("Failed to configure persistent SQLite pragmas");

		let pool_size = config.read_pool_size.max(1) as usize;
		let mut conns = Vec::with_capacity(pool_size);
		for _ in 0..pool_size {
			let reader = connect(&db_path, flags).expect("Failed to open persistent read connection");
			pragma::apply_read_only(&reader, &config)
				.expect("Failed to configure persistent read connection");
			conns.push(Mutex::new(reader));
		}

		Self {
			inner: Arc::new(SqlitePersistentStorageInner {
				conn: Mutex::new(conn),
				readers: ReadPool {
					conns,
					next: AtomicUsize::new(0),
				},
			}),
		}
	}

	pub fn in_memory() -> Self {
		Self::new(SqliteConfig::in_memory())
	}

	pub fn count_current(&self, table: EntryKind) -> Result<u64> {
		let table_name = warm_current_table_name(table);
		let conn = self.inner.readers.acquire();
		let sql = format!("SELECT COUNT(*) FROM \"{}\"", table_name);
		match conn.query_row(&sql, [], |row| row.get::<_, i64>(0)) {
			Ok(c) => Ok(c as u64),
			Err(e) if e.to_string().contains("no such table") => Ok(0),
			Err(e) => Err(error!(internal(format!("Failed to count persistent current: {}", e)))),
		}
	}

	fn create_table_if_needed(conn: &Connection, table_name: &str) -> SqliteResult<()> {
		conn.execute(&build_create_warm_current_sql(table_name), [])?;
		Ok(())
	}

	fn range_chunk(&self, cursor: &mut RangeCursor, req: RangeChunkRequest<'_>) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let _read_timer = ReadTimer::start();
		let table_name = warm_current_table_name(req.table);
		let conn = self.inner.readers.acquire();

		let sql = build_range_warm_current_sql(
			&table_name,
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

		let version_bytes = version_to_bytes(req.version).to_vec();
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

		let entries = match stmt.query_map(params_from_iter(params), |row| {
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
	version: CommitVersion,
	batch_size: usize,
	descending: bool,
}

impl TierStorage for SqlitePersistentStorage {
	#[instrument(name = "store::multi::persistent::sqlite::get", level = "trace", skip(self), fields(table = ?table, key_len = key.len(), version = version.0))]
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult> {
		let _read_timer = ReadTimer::start();
		let table_name = warm_current_table_name(table);
		let conn = self.inner.readers.acquire();
		let sql = build_get_warm_current_sql(&table_name);

		let result = match conn.prepare_cached(&sql) {
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

	fn get_many(
		&self,
		table: EntryKind,
		keys: &[&[u8]],
		version: CommitVersion,
	) -> Result<HashMap<Vec<u8>, VersionedGetResult>> {
		let _read_timer = ReadTimer::start();
		let mut out = HashMap::with_capacity(keys.len());
		if keys.is_empty() {
			return Ok(out);
		}

		let table_name = warm_current_table_name(table);
		let conn = self.inner.readers.acquire();

		for chunk in keys.chunks(GET_MANY_CHUNK) {
			let sql = build_get_many_warm_current_sql(&table_name, chunk.len());
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

			let rows = stmt
				.query_map(params_from_iter(chunk.iter().copied()), |row| {
					let key: Vec<u8> = row.get(0)?;
					let version_bytes: Vec<u8> = row.get(1)?;
					let value: Option<Vec<u8>> = row.get(2)?;
					Ok((key, version_from_bytes(&version_bytes), value))
				})
				.map_err(|e| error!(internal(format!("Failed to query persistent get_many: {}", e))))?;

			for row in rows {
				let (key, stored_version, value) = row.map_err(|e| {
					error!(internal(format!("Failed to read persistent get_many row: {}", e)))
				})?;
				if stored_version <= version {
					let resolved = match value {
						Some(v) => VersionedGetResult::Value {
							value: CowVec::new(v),
							version: stored_version,
						},
						None => VersionedGetResult::Tombstone,
					};
					out.insert(key, resolved);
				}
			}
		}

		Ok(out)
	}

	#[instrument(name = "store::multi::persistent::sqlite::set", level = "debug", skip(self, batches), fields(table_count = batches.len(), version = version.0))]
	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<()> {
		if batches.is_empty() {
			return Ok(());
		}

		let lock_start = Instant::now();
		let conn = self.inner.conn.lock();
		let lock_wait = lock_start.elapsed();
		let tx = conn
			.unchecked_transaction()
			.map_err(|e| error!(internal(format!("Failed to start persistent transaction: {}", e))))?;

		let new_version_bytes = version_to_bytes(version);

		for (table, entries) in batches {
			let table_name = warm_current_table_name(table);
			Self::create_table_if_needed(&tx, &table_name)
				.map_err(|e| error!(internal(format!("Failed to ensure persistent table: {}", e))))?;

			let upsert_sql = build_upsert_warm_current_sql(&table_name);
			let mut stmt = tx
				.prepare_cached(&upsert_sql)
				.map_err(|e| error!(internal(format!("Failed to prepare persistent upsert: {}", e))))?;

			for (key, value) in entries {
				let key_slice = key.as_slice();
				let value_slice = value.as_ref().map(|v| v.as_slice());
				stmt.execute(params![key_slice, new_version_bytes.as_slice(), value_slice]).map_err(
					|e| error!(internal(format!("Failed to upsert persistent row: {}", e))),
				)?;
			}
		}

		let commit_start = Instant::now();
		let result = tx
			.commit()
			.map_err(|e| error!(internal(format!("Failed to commit persistent transaction: {}", e))));
		let txn_commit = commit_start.elapsed();

		if lock_wait.as_micros() + txn_commit.as_micros() >= SQLITE_SET_DEBUG_THRESHOLD_US {
			println!(
				"[dbg:sqlite-set] ts_ms={} version={} lock_wait={}us txn_commit={}us",
				std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.map(|d| d.as_millis())
					.unwrap_or(0),
				version.0,
				lock_wait.as_micros(),
				txn_commit.as_micros()
			);
		}

		result
	}

	fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch> {
		self.range_chunk(
			cursor,
			RangeChunkRequest {
				table,
				start,
				end,
				version,
				batch_size,
				descending: false,
			},
		)
	}

	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch> {
		self.range_chunk(
			cursor,
			RangeChunkRequest {
				table,
				start,
				end,
				version,
				batch_size,
				descending: true,
			},
		)
	}

	fn ensure_table(&self, table: EntryKind) -> Result<()> {
		let table_name = warm_current_table_name(table);
		let conn = self.inner.conn.lock();
		Self::create_table_if_needed(&conn, &table_name)
			.map_err(|e| error!(internal(format!("Failed to ensure persistent table: {}", e))))
	}

	fn clear_table(&self, table: EntryKind) -> Result<()> {
		let table_name = warm_current_table_name(table);
		let conn = self.inner.conn.lock();
		let result = conn.execute(&format!("DELETE FROM \"{}\"", table_name), []);
		if let Err(e) = result
			&& !e.to_string().contains("no such table")
		{
			return Err(error!(internal(format!("Failed to clear persistent {}: {}", table_name, e))));
		}
		Ok(())
	}

	fn drop(&self, _batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>>) -> Result<()> {
		// TODO: change the TierStorage interface so persistent doesn't have to expose

		panic!("SqlitePersistentStorage::drop: persistent tier has no historical chain to drop versions from");
	}

	fn get_all_versions(&self, table: EntryKind, key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		// TODO: change the TierStorage interface to remove the

		let table_name = warm_current_table_name(table);
		let conn = self.inner.readers.acquire();
		let sql = build_get_warm_current_sql(&table_name);

		let result = match conn.prepare_cached(&sql) {
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
