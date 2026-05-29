// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	iter::repeat_n,
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::Duration,
};

use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	error::diagnostic::internal::internal,
	interface::store::EntryKind,
	row::TtlAnchor,
};
use reifydb_runtime::sync::{
	map::Map,
	mutex::{Mutex, MutexGuard},
};
use reifydb_sqlite::{
	SqliteConfig, SqliteTempPathGuard,
	connection::{connect, convert_flags, resolve_db_path},
	pragma,
};
use reifydb_value::{Result, error, util::cowvec::CowVec};
use rusqlite::{Connection, Error::QueryReturnedNoRows, Result as SqliteResult, ToSql, params, params_from_iter};
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
					build_create_current_sql, build_delete_expired_sql, build_delete_keys_sql,
					build_get_current_sql, build_get_many_current_sql, build_range_current_sql,
					build_upsert_current_sql, prefix_upper_bound, version_from_bytes,
					version_to_bytes,
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

const WRITER_BUSY_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Clone)]
pub struct SqlitePersistentStorage {
	inner: Arc<SqlitePersistentStorageInner>,
}

struct SqlitePersistentStorageInner {
	conn: Mutex<Connection>,
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
		conn.busy_timeout(WRITER_BUSY_TIMEOUT).expect("Failed to set persistent busy timeout");

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
				checkpoint_threshold_frames: config.wal_autocheckpoint,
				table_sql: Map::new(),
			}),
		}
	}

	pub fn maybe_checkpoint(&self) -> Result<CheckpointOutcome> {
		let conn = self.inner.conn.lock();

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

	pub fn in_memory() -> (Self, SqliteTempPathGuard) {
		let (config, guard) = SqliteConfig::in_memory();
		(Self::new(config), guard)
	}

	fn table_sql(&self, table: EntryKind) -> Arc<TableSql> {
		self.inner.table_sql.get_or_insert_with(table, || Arc::new(TableSql::build(table)))
	}

	pub fn count_current(&self, table: EntryKind) -> Result<u64> {
		let table_sql = self.table_sql(table);
		let conn = self.inner.readers.acquire();
		let sql = format!("SELECT COUNT(*) FROM \"{}\"", table_sql.table_name);
		match conn.query_row(&sql, [], |row| row.get::<_, i64>(0)) {
			Ok(c) => Ok(c as u64),
			Err(e) if e.to_string().contains("no such table") => Ok(0),
			Err(e) => Err(error!(internal(format!("Failed to count persistent current: {}", e)))),
		}
	}

	pub fn delete_expired(
		&self,
		table: EntryKind,
		anchor: TtlAnchor,
		cutoff_nanos: u64,
		prefix: Option<&[u8]>,
	) -> Result<u64> {
		let table_sql = self.table_sql(table);
		let anchor_column = match anchor {
			TtlAnchor::Created => "created_nanos",
			TtlAnchor::Updated => "updated_nanos",
		};
		let sql = build_delete_expired_sql(&table_sql.table_name, anchor_column, prefix.is_some());
		let conn = self.inner.conn.lock();
		let result = match prefix {
			Some(prefix) => {
				let upper = prefix_upper_bound(prefix);
				conn.execute(&sql, params![cutoff_nanos as i64, prefix, upper.as_slice()])
			}
			None => conn.execute(&sql, params![cutoff_nanos as i64]),
		};
		match result {
			Ok(n) => Ok(n as u64),
			Err(e) if e.to_string().contains("no such table") => Ok(0),
			Err(e) => Err(error!(internal(format!(
				"Failed to delete expired persistent rows from {}: {}",
				table_sql.table_name, e
			)))),
		}
	}

	pub fn delete_keys(&self, table: EntryKind, keys: &[EncodedKey]) -> Result<u64> {
		if keys.is_empty() {
			return Ok(0);
		}
		let table_sql = self.table_sql(table);
		let conn = self.inner.conn.lock();
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

	fn create_table_if_needed(conn: &Connection, create_sql: &str) -> SqliteResult<()> {
		conn.execute_batch(create_sql)?;
		Ok(())
	}

	fn range_chunk(&self, cursor: &mut RangeCursor, req: RangeChunkRequest<'_>) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let table_sql = self.table_sql(req.table);
		let conn = self.inner.readers.acquire();

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

impl TierStorage for SqlitePersistentStorage {
	#[instrument(name = "store::multi::persistent::sqlite::get", level = "trace", skip(self), fields(table = ?table, key_len = key.len(), version = version.0))]
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult> {
		let table_sql = self.table_sql(table);
		let conn = self.inner.readers.acquire();

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

	fn get_many(
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
		let conn = self.inner.readers.acquire();

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

	#[instrument(name = "store::multi::persistent::sqlite::set", level = "debug", skip(self, batches), fields(table_count = batches.len(), version = version.0))]
	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<()> {
		if batches.is_empty() {
			return Ok(());
		}

		let conn = self.inner.conn.lock();
		let tx = conn
			.unchecked_transaction()
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
				let key_slice = key.as_slice();
				let value_slice = value.as_ref().map(|v| v.as_slice());
				let (created_nanos, updated_nanos) = match &value {
					Some(v) if v.len() >= 24 => {
						let row = EncodedRow(v.clone());
						(row.created_at_nanos() as i64, row.updated_at_nanos() as i64)
					}
					_ => (0i64, 0i64),
				};
				stmt.execute(params![
					key_slice,
					new_version_bytes.as_slice(),
					value_slice,
					created_nanos,
					updated_nanos
				])
				.map_err(|e| error!(internal(format!("Failed to upsert persistent row: {}", e))))?;
			}
		}

		tx.commit().map_err(|e| error!(internal(format!("Failed to commit persistent transaction: {}", e))))
	}

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
		let conn = self.inner.conn.lock();
		Self::create_table_if_needed(&conn, &table_sql.create_sql)
			.map_err(|e| error!(internal(format!("Failed to ensure persistent table: {}", e))))
	}

	fn clear_table(&self, table: EntryKind) -> Result<()> {
		let table_sql = self.table_sql(table);
		let conn = self.inner.conn.lock();
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

	fn get_all_versions(&self, table: EntryKind, key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		// TODO: change the TierStorage interface to remove the

		let table_sql = self.table_sql(table);
		let conn = self.inner.readers.acquire();

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

	fn row(created_nanos: u64, updated_nanos: u64, payload: &[u8]) -> CowVec<u8> {
		let mut buf = vec![0u8; 24 + payload.len()];
		buf[8..16].copy_from_slice(&created_nanos.to_le_bytes());
		buf[16..24].copy_from_slice(&updated_nanos.to_le_bytes());
		buf[24..].copy_from_slice(payload);
		CowVec::new(buf)
	}

	fn visible(s: &SqlitePersistentStorage, k: &EncodedKey) -> bool {
		s.get(table(), k.as_slice(), CommitVersion(u64::MAX)).unwrap().value().is_some()
	}

	#[test]
	fn delete_expired_created_anchor_removes_only_rows_at_or_below_cutoff() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(
			CommitVersion(1),
			HashMap::from([(
				table(),
				vec![
					(key(1), Some(row(100, 100, b"a"))),
					(key(2), Some(row(200, 200, b"b"))),
					(key(3), Some(row(300, 300, b"c"))),
				],
			)]),
		)
		.unwrap();
		assert_eq!(s.count_current(table()).unwrap(), 3);

		let deleted = s.delete_expired(table(), TtlAnchor::Created, 200, None).unwrap();

		assert_eq!(deleted, 2, "rows created at <= cutoff(200) must be physically deleted");
		assert_eq!(
			s.count_current(table()).unwrap(),
			1,
			"deletion must reclaim sqlite rows, not tombstone them"
		);
		assert!(!visible(&s, &key(1)));
		assert!(!visible(&s, &key(2)));
		assert!(visible(&s, &key(3)), "row newer than the TTL cutoff must survive");
	}

	#[test]
	fn delete_expired_updated_anchor_keeps_recently_updated_rows() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(
			CommitVersion(1),
			HashMap::from([(
				table(),
				vec![
					(key(1), Some(row(10, 500, b"created-old-updated-fresh"))),
					(key(2), Some(row(10, 50, b"created-old-updated-stale"))),
				],
			)]),
		)
		.unwrap();

		let deleted = s.delete_expired(table(), TtlAnchor::Updated, 100, None).unwrap();

		assert_eq!(deleted, 1, "Updated anchor must key eviction on updated_nanos, not created_nanos");
		assert!(
			visible(&s, &key(1)),
			"a row updated after the cutoff must NOT be evicted even if created long ago"
		);
		assert!(!visible(&s, &key(2)));
	}

	#[test]
	fn delete_expired_skips_rows_with_unset_anchor() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(
			CommitVersion(1),
			HashMap::from([(table(), vec![(key(1), None), (key(2), Some(row(0, 0, b"no-anchor")))])]),
		)
		.unwrap();
		let before = s.count_current(table()).unwrap();

		let deleted = s.delete_expired(table(), TtlAnchor::Created, u64::MAX, None).unwrap();

		assert_eq!(deleted, 0, "rows whose anchor is 0 (tombstones / undatable) must never be mass-deleted");
		assert_eq!(s.count_current(table()).unwrap(), before);
	}

	#[test]
	fn delete_expired_on_missing_table_is_noop() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		let deleted = s
			.delete_expired(EntryKind::Source(ShapeId::Table(TableId(999))), TtlAnchor::Created, 100, None)
			.unwrap();
		assert_eq!(deleted, 0);
	}

	#[test]
	fn delete_expired_with_prefix_only_touches_matching_keys() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		// Two "sides" distinguished by a leading prefix byte, both stale by the cutoff.
		let left = EncodedKey::new(vec![0x01, 0xAA]);
		let right = EncodedKey::new(vec![0x02, 0xBB]);
		s.set(
			CommitVersion(1),
			HashMap::from([(
				table(),
				vec![(left.clone(), Some(row(10, 10, b"l"))), (right.clone(), Some(row(10, 10, b"r")))],
			)]),
		)
		.unwrap();

		let deleted = s.delete_expired(table(), TtlAnchor::Updated, 100, Some(&[0x01])).unwrap();

		assert_eq!(deleted, 1, "only the 0x01-prefixed (left) row should be deleted");
		assert!(!visible(&s, &left));
		assert!(visible(&s, &right), "the 0x02-prefixed (right) row must survive a left-only prefix sweep");
	}
}
