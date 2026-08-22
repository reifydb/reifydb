// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::Bound,
	iter::repeat_n,
	sync::{
		Arc,
		atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
	},
};

use postcard::{from_bytes, to_stdvec};
use reifydb_codec::cdc;
use reifydb_core::{
	common::CommitVersion,
	event::metric::CdcEviction,
	interface::cdc::{Cdc, CdcBatch},
};
use reifydb_runtime::sync::mutex::{Mutex, MutexGuard};
use reifydb_sqlite::{
	SqliteConfig, SqliteTempPathGuard,
	connection::{connect, convert_flags, resolve_db_path},
	pragma,
};
use reifydb_value::{
	byte_size::ByteSize,
	reifydb_assertions,
	value::{datetime::DateTime, duration::Duration},
};
use rusqlite::{
	Connection, Error::QueryReturnedNoRows, OptionalExtension, Transaction, params, params_from_iter,
	types::Value as SqlValue,
};
use tracing::instrument;

use crate::{
	compact::{CompactBlockSummary, cache::BlockCache},
	error::CdcError,
	storage::{
		CdcStorage, CdcStorageResult, DropBeforeResult, aggregate_evictions, merge_evictions,
		normalize_range_inclusive, total_evicted_count,
	},
};

const ROW_ZSTD_LEVEL: i32 = 1;

#[derive(Clone)]
pub struct SqliteCdcStorage {
	inner: Arc<Inner>,
}

struct Inner {
	conn: Mutex<Option<Connection>>,
	readers: ReadPool,
	block_cache: BlockCache,
	last_zstd_level: AtomicU8,
	truncated_before: AtomicU64,
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

type CompactionCandidates = (Vec<Cdc>, Vec<Vec<u8>>);

type RangeSnapshot = (Vec<(Vec<u8>, Vec<u8>)>, Option<CommitVersion>, Vec<Vec<u8>>);

type BlockIndexScan = (Vec<(Vec<u8>, Vec<u8>)>, Option<CommitVersion>);

struct FullBlockScan {
	entries: Vec<CdcEviction>,
	pks: Vec<Vec<u8>>,
	max_deleted: Option<u64>,
}

struct StraddleScan {
	entries: Vec<CdcEviction>,
	actions: Vec<(Vec<u8>, BlockOutcome)>,
	max_deleted: Option<u64>,
}

struct LiveScan {
	entries: Vec<CdcEviction>,
	max_deleted: Option<u64>,
}

enum BlockOutcome {
	Delete,
	Rewrite {
		survivors: Vec<Cdc>,
	},
}

impl SqliteCdcStorage {
	pub fn new(config: SqliteConfig) -> Self {
		Self::new_with_cache_capacity(config, BlockCache::DEFAULT_CAPACITY)
	}

	pub fn new_with_cache_capacity(config: SqliteConfig, cache_capacity: usize) -> Self {
		let conn = open_connection(&config);
		let truncated_before = read_truncated_before(&conn);
		let pool_size = (config.read_pool_size.max(1)) as usize;
		let mut conns = Vec::with_capacity(pool_size);
		for _ in 0..pool_size {
			conns.push(Mutex::new(Some(open_read_connection(&config))));
		}
		Self {
			inner: Arc::new(Inner {
				conn: Mutex::new(Some(conn)),
				readers: ReadPool {
					conns,
					next: AtomicUsize::new(0),
				},
				block_cache: BlockCache::new(cache_capacity),
				last_zstd_level: AtomicU8::new(3),
				truncated_before: AtomicU64::new(truncated_before),
			}),
		}
	}

	pub fn block_cache_capacity(&self) -> usize {
		self.inner.block_cache.capacity()
	}

	pub fn in_memory() -> (Self, SqliteTempPathGuard) {
		let (config, guard) = SqliteConfig::in_memory();
		(Self::new(config), guard)
	}

	fn ensure_schema(conn: &Connection) {
		create_cdc_table(conn);
		create_cdc_created_at_index(conn);
		create_cdc_block_table(conn);
		create_block_timestamp_index(conn);
		create_cdc_meta_table(conn);
	}

	pub fn shrink_memory(&self) {
		let guard = self.inner.conn.lock();
		if let Some(conn) = guard.as_ref() {
			let _ = pragma::shrink_memory(conn);
		}
	}

	pub fn shutdown(&self) {
		self.inner.readers.shutdown();
		if let Some(conn) = self.inner.conn.lock().take() {
			let _ = pragma::shutdown(&conn);
			drop(conn);
		}
	}

	fn read_from_blocks(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		let v_bytes = version_to_bytes(version);
		let Some((max_bytes, payload)) = self.find_block_for_version(&v_bytes)? else {
			return Ok(None);
		};
		let block_max = bytes_to_version(&max_bytes)?;
		let entries = self.load_block_cached(block_max, &payload)?;
		Ok(entries.iter().find(|c| c.version == version).cloned())
	}

	#[inline]
	fn find_block_for_version(&self, v_bytes: &[u8; 8]) -> CdcStorageResult<Option<(Vec<u8>, Vec<u8>)>> {
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(None);
		};
		conn.prepare_cached(
			r#"SELECT max_version, payload FROM "cdc_block"
			   WHERE max_version >= ?1 AND min_version <= ?1
			   ORDER BY max_version ASC LIMIT 1"#,
		)
		.map_err(|e| CdcError::Internal(format!("read_from_blocks prepare: {e}")))?
		.query_row(params![v_bytes.as_slice()], |row| {
			Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
		})
		.map(Some)
		.or_else(|e| match e {
			QueryReturnedNoRows => Ok(None),
			e => Err(CdcError::Internal(format!("read_from_blocks: {e}"))),
		})
	}

	fn load_block_cached(&self, block_max: CommitVersion, payload: &[u8]) -> CdcStorageResult<Arc<Vec<Cdc>>> {
		if let Some(hit) = self.inner.block_cache.get(block_max) {
			return Ok(hit);
		}
		let entries = cdc::decode::<Vec<Cdc>>(payload)?;
		let arc = Arc::new(entries);
		self.inner.block_cache.put(block_max, arc.clone());
		Ok(arc)
	}

	pub fn compact_oldest(
		&self,
		target_size: usize,
		safety_lag: u64,
		zstd_level: u8,
		producer_watermark: CommitVersion,
	) -> CdcStorageResult<Option<CompactBlockSummary>> {
		self.compact_oldest_inner(target_size, safety_lag, false, zstd_level, producer_watermark)
	}

	pub fn compact_all(
		&self,
		target_size: usize,
		zstd_level: u8,
		producer_watermark: CommitVersion,
	) -> CdcStorageResult<Vec<CompactBlockSummary>> {
		let mut out = Vec::new();
		while let Some(s) = self.compact_oldest_inner(target_size, 0, false, zstd_level, producer_watermark)? {
			out.push(s);
		}
		if let Some(tail) = self.compact_oldest_inner(target_size, 0, true, zstd_level, producer_watermark)? {
			out.push(tail);
		}
		Ok(out)
	}

	fn compact_oldest_inner(
		&self,
		target_size: usize,
		safety_lag: u64,
		allow_partial: bool,
		zstd_level: u8,
		producer_watermark: CommitVersion,
	) -> CdcStorageResult<Option<CompactBlockSummary>> {
		if target_size == 0 {
			return Ok(None);
		}
		self.inner.last_zstd_level.store(zstd_level, Ordering::Relaxed);

		let Some((entries, version_blobs)) =
			self.select_oldest_eligible(target_size, safety_lag, allow_partial, producer_watermark)?
		else {
			return Ok(None);
		};

		assert_block_ordered(&entries);
		let payload = cdc::encode(&entries, zstd_level as i32)?;
		let compressed_bytes = payload.len();
		let rollup = aggregate_evictions(entries.iter().flat_map(|c| c.changes.iter()));
		let rollup_bytes = to_stdvec(&rollup)
			.map_err(|e| CdcError::Codec(format!("postcard encode block rollup: {e}")))?;
		let (min_ts_nanos, max_ts_nanos) = summarize_timestamps(&entries);
		let min_version = entries.first().unwrap().version;
		let max_version = entries.last().unwrap().version;

		let committed = self.commit_block_swap(
			&version_blobs,
			&payload,
			&rollup_bytes,
			min_version,
			max_version,
			min_ts_nanos,
			max_ts_nanos,
			entries.len(),
		)?;
		if !committed {
			return Ok(None);
		}
		Ok(Some(build_block_summary(&entries, min_version, max_version, compressed_bytes)))
	}

	fn select_oldest_eligible(
		&self,
		target_size: usize,
		safety_lag: u64,
		allow_partial: bool,
		producer_watermark: CommitVersion,
	) -> CdcStorageResult<Option<CompactionCandidates>> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(None);
		};
		let Some(max_v) = query_max_live_version(conn)? else {
			return Ok(None);
		};
		let Some(eligible_max) = compute_eligible_max(max_v, safety_lag, producer_watermark) else {
			return Ok(None);
		};
		let (entries, version_blobs) = query_oldest_candidates(conn, eligible_max, target_size)?;
		if entries.is_empty() {
			return Ok(None);
		}
		if !allow_partial && entries.len() < target_size {
			return Ok(None);
		}
		Ok(Some((entries, version_blobs)))
	}

	#[allow(clippy::too_many_arguments)]
	fn commit_block_swap(
		&self,
		version_blobs: &[Vec<u8>],
		payload: &[u8],
		rollup_bytes: &[u8],
		min_version: CommitVersion,
		max_version: CommitVersion,
		min_ts_nanos: i64,
		max_ts_nanos: i64,
		num_entries: usize,
	) -> CdcStorageResult<bool> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(false);
		};
		let tx = conn
			.unchecked_transaction()
			.map_err(|e| CdcError::Internal(format!("compact tx begin: {e}")))?;

		if !delete_compacted_versions(&tx, version_blobs, num_entries)? {
			tx.rollback().map_err(|e| CdcError::Internal(format!("compact rollback: {e}")))?;
			return Ok(false);
		}
		insert_compacted_block(
			&tx,
			payload,
			rollup_bytes,
			min_version,
			max_version,
			min_ts_nanos,
			max_ts_nanos,
			num_entries,
		)?;
		tx.commit().map_err(|e| CdcError::Internal(format!("compact commit: {e}")))?;
		Ok(true)
	}

	#[inline]
	fn snapshot_block_and_live(
		&self,
		lo_inc: CommitVersion,
		hi_inc: CommitVersion,
		batch_size: u64,
	) -> CdcStorageResult<RangeSnapshot> {
		let lo_b = version_to_bytes(lo_inc);
		let hi_b = version_to_bytes(hi_inc);
		let limit = (batch_size as i64).saturating_add(1);
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok((Vec::new(), None, Vec::new()));
		};

		let (block_rows, block_frontier) = read_block_index_rows(conn, &lo_b, &hi_b, batch_size)?;
		let live_payloads = read_live_payloads(conn, &lo_b, &hi_b, limit)?;

		Ok((block_rows, block_frontier, live_payloads))
	}

	#[inline]
	fn decode_block_rows(
		&self,
		block_rows: Vec<(Vec<u8>, Vec<u8>)>,
		lo_inc: CommitVersion,
		hi_inc: CommitVersion,
	) -> CdcStorageResult<Vec<Cdc>> {
		let mut block_items: Vec<Cdc> = Vec::new();
		for (max_bytes, payload) in block_rows {
			let block_max = bytes_to_version(&max_bytes)?;
			let entries = self.load_block_cached(block_max, &payload)?;
			for cdc in entries.iter() {
				if cdc.version >= lo_inc && cdc.version <= hi_inc {
					block_items.push(cdc.clone());
				}
			}
		}
		Ok(block_items)
	}

	#[inline]
	fn scan_full_blocks_below(
		&self,
		conn: &Connection,
		version_bytes: &[u8; 8],
	) -> CdcStorageResult<FullBlockScan> {
		let mut stmt = conn
			.prepare_cached(
				r#"SELECT max_version, stats_rollup FROM "cdc_block"
				   WHERE max_version < ?1 ORDER BY max_version ASC"#,
			)
			.map_err(|e| CdcError::Internal(format!("drop blocks prepare: {e}")))?;
		let rows = stmt
			.query_map(params![version_bytes.as_slice()], |row| {
				Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
			})
			.map_err(|e| CdcError::Internal(format!("drop blocks rows: {e}")))?;
		let mut entries = Vec::new();
		let mut pks = Vec::new();
		let mut max_deleted = None;
		for row in rows {
			let (max_bytes, rollup) =
				row.map_err(|e| CdcError::Internal(format!("drop blocks row: {e}")))?;
			let block_max = bytes_to_version(&max_bytes)?;
			entries.extend(decode_rollup(&rollup)?);
			self.inner.block_cache.remove(block_max);
			max_deleted = Some(block_max.0);
			pks.push(max_bytes);
		}
		Ok(FullBlockScan {
			entries,
			pks,
			max_deleted,
		})
	}

	#[inline]
	fn scan_straddle_blocks(
		&self,
		conn: &Connection,
		version: CommitVersion,
		version_bytes: &[u8; 8],
	) -> CdcStorageResult<StraddleScan> {
		let mut stmt = conn
			.prepare_cached(
				r#"SELECT max_version, payload FROM "cdc_block"
				   WHERE min_version < ?1 AND max_version >= ?1
				   ORDER BY max_version ASC"#,
			)
			.map_err(|e| CdcError::Internal(format!("drop straddle prepare: {e}")))?;
		let rows = stmt
			.query_map(params![version_bytes.as_slice()], |row| {
				Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
			})
			.map_err(|e| CdcError::Internal(format!("drop straddle rows: {e}")))?;
		let mut entries = Vec::new();
		let mut actions = Vec::new();
		let mut max_deleted = None;
		for row in rows {
			let (max_bytes, payload) =
				row.map_err(|e| CdcError::Internal(format!("drop straddle row: {e}")))?;
			let block_max = bytes_to_version(&max_bytes)?;
			let decoded = cdc::decode::<Vec<Cdc>>(&payload)?;
			let mut survivors: Vec<Cdc> = Vec::with_capacity(decoded.len());
			let mut evicted: Vec<Cdc> = Vec::new();
			for cdc in decoded {
				if cdc.version < version {
					evicted.push(cdc);
				} else {
					survivors.push(cdc);
				}
			}
			if let Some(max_evicted) = evicted.iter().map(|c| c.version.0).max() {
				max_deleted = Some(max_deleted.map_or(max_evicted, |m: u64| m.max(max_evicted)));
			}
			entries.extend(aggregate_evictions(evicted.iter().flat_map(|c| c.changes.iter())));
			self.inner.block_cache.remove(block_max);
			let outcome = if survivors.is_empty() {
				BlockOutcome::Delete
			} else {
				BlockOutcome::Rewrite {
					survivors,
				}
			};
			actions.push((max_bytes, outcome));
		}
		Ok(StraddleScan {
			entries,
			actions,
			max_deleted,
		})
	}
}

fn open_connection(config: &SqliteConfig) -> Connection {
	let db_path = resolve_db_path(config.path.clone(), "cdc.db");
	let flags = convert_flags(&config.flags);
	let conn = connect(&db_path, flags).expect("Failed to connect to CDC SQLite database");
	pragma::apply(&conn, config).expect("Failed to configure CDC SQLite pragmas");
	conn.busy_timeout(WRITE_CONN_BUSY_TIMEOUT.to_std()).expect("Failed to set CDC write busy timeout");
	SqliteCdcStorage::ensure_schema(&conn);
	conn
}

const READ_CONN_CACHE_SIZE: ByteSize = ByteSize::from_mib(2);
const READ_CONN_BUSY_TIMEOUT: Duration = Duration::from_milliseconds_const(5_000);
const WRITE_CONN_BUSY_TIMEOUT: Duration = Duration::from_milliseconds_const(200);

fn open_read_connection(config: &SqliteConfig) -> Connection {
	let db_path = resolve_db_path(config.path.clone(), "cdc.db");
	let flags = convert_flags(&config.flags);
	let conn = connect(&db_path, flags).expect("Failed to open CDC read connection");
	let mut read_config = config.clone();
	read_config.cache_size = Some(match config.cache_size {
		Some(configured) => configured.min(READ_CONN_CACHE_SIZE),
		None => READ_CONN_CACHE_SIZE,
	});
	pragma::apply_read_only(&conn, &read_config).expect("Failed to configure CDC read connection");
	conn.busy_timeout(READ_CONN_BUSY_TIMEOUT.to_std()).expect("Failed to set CDC read busy timeout");
	conn
}

fn create_cdc_table(conn: &Connection) {
	conn.execute(
		r#"CREATE TABLE IF NOT EXISTS "cdc" (
			version BLOB PRIMARY KEY,
			payload BLOB NOT NULL,
			created_at INTEGER NOT NULL,
			stats_rollup BLOB NOT NULL
		) WITHOUT ROWID"#,
		[],
	)
	.expect("Failed to create cdc table");
}

fn create_cdc_created_at_index(conn: &Connection) {
	conn.execute(
		r#"CREATE INDEX IF NOT EXISTS "cdc_created_at_idx"
		   ON "cdc"(created_at)"#,
		[],
	)
	.expect("Failed to create cdc_created_at index");
}

fn create_cdc_block_table(conn: &Connection) {
	conn.execute(
		r#"CREATE TABLE IF NOT EXISTS "cdc_block" (
			max_version BLOB PRIMARY KEY,
			min_version BLOB NOT NULL,
			min_timestamp INTEGER NOT NULL,
			max_timestamp INTEGER NOT NULL,
			num_entries INTEGER NOT NULL,
			payload BLOB NOT NULL,
			stats_rollup BLOB NOT NULL
		) WITHOUT ROWID"#,
		[],
	)
	.expect("Failed to create cdc_block table");
}

fn create_block_timestamp_index(conn: &Connection) {
	conn.execute(
		r#"CREATE INDEX IF NOT EXISTS "cdc_block_max_ts_idx"
		   ON "cdc_block"(max_timestamp)"#,
		[],
	)
	.expect("Failed to create cdc_block_max_ts index");
}

fn create_cdc_meta_table(conn: &Connection) {
	conn.execute(
		r#"CREATE TABLE IF NOT EXISTS "cdc_meta" (
			key TEXT PRIMARY KEY,
			value BLOB NOT NULL
		) WITHOUT ROWID"#,
		[],
	)
	.expect("Failed to create cdc_meta table");
}

fn read_truncated_before(conn: &Connection) -> u64 {
	let row: Option<Vec<u8>> = conn
		.query_row(r#"SELECT value FROM "cdc_meta" WHERE key = 'truncated_before'"#, [], |row| row.get(0))
		.optional()
		.expect("Failed to read cdc_meta truncated_before");
	row.and_then(|bytes| bytes.try_into().ok().map(u64::from_be_bytes)).unwrap_or(0)
}

fn persist_truncated_before(tx: &Transaction<'_>, version_bytes: &[u8; 8]) -> CdcStorageResult<()> {
	tx.prepare_cached(
		r#"INSERT INTO "cdc_meta" (key, value) VALUES ('truncated_before', ?1)
		   ON CONFLICT(key) DO UPDATE SET value = excluded.value WHERE excluded.value > value"#,
	)
	.map_err(|e| CdcError::Internal(format!("truncated_before prepare: {e}")))?
	.execute(params![version_bytes.as_slice()])
	.map_err(|e| CdcError::Internal(format!("truncated_before write: {e}")))?;
	Ok(())
}

#[inline]
fn query_max_live_version(conn: &Connection) -> CdcStorageResult<Option<u64>> {
	let max_live: Option<Vec<u8>> = conn
		.prepare_cached(r#"SELECT MAX(version) FROM "cdc""#)
		.ok()
		.and_then(|mut stmt| stmt.query_row([], |row| row.get::<_, Option<Vec<u8>>>(0)).ok())
		.flatten();
	max_live.map(|b| bytes_to_version(&b).map(|v| v.0)).transpose()
}

#[inline]
fn compute_eligible_max(max_v: u64, safety_lag: u64, producer_watermark: CommitVersion) -> Option<CommitVersion> {
	if max_v < safety_lag {
		return None;
	}
	let safety_capped = max_v.saturating_sub(safety_lag);
	Some(CommitVersion(safety_capped.min(producer_watermark.0)))
}

#[inline]
fn query_oldest_candidates(
	conn: &Connection,
	eligible_max: CommitVersion,
	target_size: usize,
) -> CdcStorageResult<(Vec<Cdc>, Vec<Vec<u8>>)> {
	let eligible_max_bytes = version_to_bytes(eligible_max);
	let mut stmt = conn
		.prepare_cached(
			r#"SELECT version, payload FROM "cdc"
			   WHERE version <= ?1 ORDER BY version ASC LIMIT ?2"#,
		)
		.map_err(|e| CdcError::Internal(format!("compact prepare: {e}")))?;
	let limit = (target_size as i64).saturating_add(1);
	let rows = stmt
		.query_map(params![eligible_max_bytes.as_slice(), limit], |row| {
			Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
		})
		.map_err(|e| CdcError::Internal(format!("compact rows: {e}")))?;

	let mut version_blobs: Vec<Vec<u8>> = Vec::with_capacity(target_size);
	let mut entries: Vec<Cdc> = Vec::with_capacity(target_size);
	for row in rows {
		if entries.len() == target_size {
			break;
		}
		let (vb, pb) = row.map_err(|e| CdcError::Internal(format!("compact row: {e}")))?;
		version_blobs.push(vb);
		entries.push(cdc::decode::<Cdc>(&pb)?);
	}
	Ok((entries, version_blobs))
}

#[inline]
fn build_block_summary(
	entries: &[Cdc],
	min_version: CommitVersion,
	max_version: CommitVersion,
	compressed_bytes: usize,
) -> CompactBlockSummary {
	CompactBlockSummary {
		min_version,
		max_version,
		num_entries: entries.len(),
		compressed_bytes,
	}
}

#[inline]
fn query_min_block(conn: &Connection) -> CdcStorageResult<Option<CommitVersion>> {
	let r: Option<Vec<u8>> = conn
		.prepare_cached(r#"SELECT MIN(min_version) FROM "cdc_block""#)
		.ok()
		.and_then(|mut stmt| stmt.query_row([], |row| row.get::<_, Option<Vec<u8>>>(0)).ok())
		.flatten();
	r.map(|b| bytes_to_version(&b)).transpose()
}

#[inline]
fn live_cutoff_at_offset(
	conn: &Connection,
	cutoff_bytes: &[u8; 8],
	offset: usize,
) -> CdcStorageResult<Option<CommitVersion>> {
	let res = conn
		.prepare_cached(
			r#"SELECT version FROM "cdc" WHERE version < ?1 ORDER BY version ASC LIMIT 1 OFFSET ?2"#,
		)
		.map_err(|e| CdcError::Internal(format!("live_cutoff_at_offset prepare: {e}")))?
		.query_row(params![cutoff_bytes.as_slice(), offset as i64], |row| row.get::<_, Vec<u8>>(0));
	match res {
		Ok(b) => Ok(Some(bytes_to_version(&b)?)),
		Err(QueryReturnedNoRows) => Ok(None),
		Err(e) => Err(CdcError::Internal(format!("live_cutoff_at_offset: {e}"))),
	}
}

#[inline]
fn query_min_live(conn: &Connection) -> CdcStorageResult<Option<CommitVersion>> {
	let r: Option<Vec<u8>> = conn
		.prepare_cached(r#"SELECT MIN(version) FROM "cdc""#)
		.ok()
		.and_then(|mut stmt| stmt.query_row([], |row| row.get::<_, Option<Vec<u8>>>(0)).ok())
		.flatten();
	r.map(|b| bytes_to_version(&b)).transpose()
}

#[inline]
fn query_max_live(conn: &Connection) -> CdcStorageResult<Option<CommitVersion>> {
	let r: Option<Vec<u8>> = conn
		.prepare_cached(r#"SELECT MAX(version) FROM "cdc""#)
		.ok()
		.and_then(|mut stmt| stmt.query_row([], |row| row.get::<_, Option<Vec<u8>>>(0)).ok())
		.flatten();
	r.map(|b| bytes_to_version(&b)).transpose()
}

#[inline]
fn query_max_block(conn: &Connection) -> CdcStorageResult<Option<CommitVersion>> {
	let r: Option<Vec<u8>> = conn
		.prepare_cached(r#"SELECT MAX(max_version) FROM "cdc_block""#)
		.ok()
		.and_then(|mut stmt| stmt.query_row([], |row| row.get::<_, Option<Vec<u8>>>(0)).ok())
		.flatten();
	r.map(|b| bytes_to_version(&b)).transpose()
}

fn version_to_bytes(v: CommitVersion) -> [u8; 8] {
	v.0.to_be_bytes()
}

fn bytes_to_version(bytes: &[u8]) -> CdcStorageResult<CommitVersion> {
	let arr: [u8; 8] = bytes.try_into().map_err(|_| CdcError::Internal("bad version bytes".to_string()))?;
	Ok(CommitVersion(u64::from_be_bytes(arr)))
}

fn datetime_to_nanos(dt: &DateTime) -> i64 {
	dt.to_nanos() as i64
}

fn summarize_timestamps(entries: &[Cdc]) -> (i64, i64) {
	entries.iter().fold((i64::MAX, i64::MIN), |(lo, hi), c| {
		let n = datetime_to_nanos(&c.timestamp);
		(lo.min(n), hi.max(n))
	})
}

#[inline]
fn read_block_index_rows(
	conn: &Connection,
	lo_b: &[u8; 8],
	hi_b: &[u8; 8],
	batch_size: u64,
) -> CdcStorageResult<BlockIndexScan> {
	let mut stmt = conn
		.prepare_cached(
			r#"SELECT max_version, num_entries, payload FROM "cdc_block"
			   WHERE max_version >= ?1 AND min_version <= ?2
			   ORDER BY max_version ASC"#,
		)
		.map_err(|e| CdcError::Internal(format!("range blocks prepare: {e}")))?;
	let mut rows = stmt
		.query(params![lo_b.as_slice(), hi_b.as_slice()])
		.map_err(|e| CdcError::Internal(format!("range blocks rows: {e}")))?;
	let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
	let mut budget = batch_size.saturating_add(1);
	let mut frontier = None;
	while let Some(row) = rows.next().map_err(|e| CdcError::Internal(format!("range blocks row: {e}")))? {
		if budget == 0 {
			return Ok((out, frontier));
		}
		let max_bytes: Vec<u8> =
			row.get(0).map_err(|e| CdcError::Internal(format!("range blocks max_version: {e}")))?;
		let num_entries: i64 =
			row.get(1).map_err(|e| CdcError::Internal(format!("range blocks num_entries: {e}")))?;
		let payload: Vec<u8> =
			row.get(2).map_err(|e| CdcError::Internal(format!("range blocks payload: {e}")))?;
		frontier = Some(bytes_to_version(&max_bytes)?);
		out.push((max_bytes, payload));
		budget = budget.saturating_sub(num_entries.max(1) as u64);
	}
	Ok((out, None))
}

#[inline]
fn read_live_payloads(conn: &Connection, lo_b: &[u8; 8], hi_b: &[u8; 8], limit: i64) -> CdcStorageResult<Vec<Vec<u8>>> {
	let mut stmt = conn
		.prepare_cached(
			r#"SELECT payload FROM "cdc"
			   WHERE version >= ?1 AND version <= ?2
			   ORDER BY version ASC LIMIT ?3"#,
		)
		.map_err(|e| CdcError::Internal(format!("range live prepare: {e}")))?;
	let rows = stmt
		.query_map(params![lo_b.as_slice(), hi_b.as_slice(), limit], |row| row.get::<_, Vec<u8>>(0))
		.map_err(|e| CdcError::Internal(format!("range live rows: {e}")))?;
	let mut out = Vec::new();
	for r in rows {
		out.push(r.map_err(|e| CdcError::Internal(format!("range live row: {e}")))?);
	}
	Ok(out)
}

#[inline]
fn decode_live_payloads(payloads: Vec<Vec<u8>>) -> CdcStorageResult<Vec<Cdc>> {
	let mut live_items = Vec::with_capacity(payloads.len());
	for payload in payloads {
		live_items.push(cdc::decode::<Cdc>(&payload)?);
	}
	Ok(live_items)
}

#[inline]
fn merge_block_and_live(block_items: Vec<Cdc>, live_items: Vec<Cdc>) -> Vec<Cdc> {
	let mut merged: Vec<Cdc> = Vec::with_capacity(block_items.len() + live_items.len());
	let (mut bi, mut li) = (0usize, 0usize);
	while bi < block_items.len() && li < live_items.len() {
		let bv = block_items[bi].version;
		let lv = live_items[li].version;
		if bv < lv {
			merged.push(block_items[bi].clone());
			bi += 1;
		} else if bv > lv {
			merged.push(live_items[li].clone());
			li += 1;
		} else {
			merged.push(block_items[bi].clone());
			bi += 1;
			li += 1;
		}
	}
	while bi < block_items.len() {
		merged.push(block_items[bi].clone());
		bi += 1;
	}
	while li < live_items.len() {
		merged.push(live_items[li].clone());
		li += 1;
	}
	merged
}

#[inline]
fn scan_live_rows_below(conn: &Connection, version_bytes: &[u8; 8]) -> CdcStorageResult<LiveScan> {
	let mut stmt = conn
		.prepare_cached(r#"SELECT version, stats_rollup FROM "cdc" WHERE version < ?1 ORDER BY version ASC"#)
		.map_err(|e| CdcError::Internal(format!("drop_before prepare: {e}")))?;
	let rows = stmt
		.query_map(params![version_bytes.as_slice()], |row| {
			Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
		})
		.map_err(|e| CdcError::Internal(format!("drop_before rows: {e}")))?;
	let mut entries = Vec::new();
	let mut max_deleted = None;
	for row in rows {
		let (version_bytes, rollup) = row.map_err(|e| CdcError::Internal(format!("drop_before row: {e}")))?;
		max_deleted = Some(bytes_to_version(&version_bytes)?.0);
		entries.extend(decode_rollup(&rollup)?);
	}
	Ok(LiveScan {
		entries,
		max_deleted,
	})
}

#[inline]
fn apply_drop_before(
	conn: &Connection,
	full_block_pks: &[Vec<u8>],
	straddle_actions: &[(Vec<u8>, BlockOutcome)],
	version_bytes: &[u8; 8],
	zstd_level: u8,
	floor: Option<u64>,
) -> CdcStorageResult<()> {
	let tx = conn.unchecked_transaction().map_err(|e| CdcError::Internal(format!("drop_before tx begin: {e}")))?;

	{
		let mut del_block_stmt = tx
			.prepare_cached(r#"DELETE FROM "cdc_block" WHERE max_version = ?1"#)
			.map_err(|e| CdcError::Internal(format!("drop block delete prepare: {e}")))?;
		for pk in full_block_pks {
			del_block_stmt
				.execute(params![pk.as_slice()])
				.map_err(|e| CdcError::Internal(format!("drop block delete: {e}")))?;
		}
	}

	for (max_bytes, action) in straddle_actions {
		match action {
			BlockOutcome::Delete => {
				tx.prepare_cached(r#"DELETE FROM "cdc_block" WHERE max_version = ?1"#)
					.map_err(|e| CdcError::Internal(format!("drop straddle delete prepare: {e}")))?
					.execute(params![max_bytes.as_slice()])
					.map_err(|e| CdcError::Internal(format!("drop straddle delete: {e}")))?;
			}
			BlockOutcome::Rewrite {
				survivors,
			} => {
				rewrite_straddle_block(&tx, max_bytes, survivors, zstd_level)?;
			}
		}
	}

	tx.prepare_cached(r#"DELETE FROM "cdc" WHERE version < ?1"#)
		.map_err(|e| CdcError::Internal(format!("drop_before delete prepare: {e}")))?
		.execute(params![version_bytes.as_slice()])
		.map_err(|e| CdcError::Internal(format!("drop_before delete: {e}")))?;
	if let Some(floor) = floor {
		persist_truncated_before(&tx, &floor.to_be_bytes())?;
	}
	tx.commit().map_err(|e| CdcError::Internal(format!("drop_before commit: {e}")))?;
	Ok(())
}

#[inline]
#[cfg_attr(not(debug_assertions), allow(unused_variables))]
fn assert_block_ordered(entries: &[Cdc]) {
	reifydb_assertions! {
		assert!(!entries.is_empty(), "cannot encode an empty block");
		assert!(
			entries.windows(2).all(|w| w[0].version < w[1].version),
			"block entries must be strictly ascending by version"
		);
	}
}

fn rewrite_straddle_block(
	tx: &Transaction<'_>,
	max_bytes: &[u8],
	survivors: &[Cdc],
	zstd_level: u8,
) -> CdcStorageResult<()> {
	let new_min = survivors.first().unwrap().version;
	reifydb_assertions! {
		let new_max = survivors.last().unwrap().version;
		assert_eq!(new_max, bytes_to_version(max_bytes)?, "max_version is the block PK and must be preserved");
	}
	let (min_ts_nanos, max_ts_nanos) = summarize_timestamps(survivors);
	assert_block_ordered(survivors);
	let payload = cdc::encode(survivors, zstd_level as i32)?;
	let rollup = aggregate_evictions(survivors.iter().flat_map(|c| c.changes.iter()));
	let rollup_bytes =
		to_stdvec(&rollup).map_err(|e| CdcError::Codec(format!("postcard encode straddle rollup: {e}")))?;
	tx.prepare_cached(
		r#"INSERT OR REPLACE INTO "cdc_block"
		   (max_version, min_version, min_timestamp, max_timestamp, num_entries, payload, stats_rollup)
		   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
	)
	.map_err(|e| CdcError::Internal(format!("drop straddle rewrite prepare: {e}")))?
	.execute(params![
		max_bytes,
		version_to_bytes(new_min).as_slice(),
		min_ts_nanos,
		max_ts_nanos,
		survivors.len() as i64,
		payload.as_slice(),
		rollup_bytes.as_slice(),
	])
	.map_err(|e| CdcError::Internal(format!("drop straddle rewrite: {e}")))?;
	Ok(())
}

#[inline]
fn decode_rollup(bytes: &[u8]) -> CdcStorageResult<Vec<CdcEviction>> {
	from_bytes(bytes).map_err(|e| CdcError::Codec(format!("postcard decode rollup: {e}")))
}

#[inline]
fn delete_compacted_versions(
	tx: &Transaction<'_>,
	version_blobs: &[Vec<u8>],
	expected_count: usize,
) -> CdcStorageResult<bool> {
	let placeholders = repeat_n("?", version_blobs.len()).collect::<Vec<_>>().join(",");
	let del_sql = format!(r#"DELETE FROM "cdc" WHERE version IN ({})"#, placeholders);
	let del_params: Vec<SqlValue> = version_blobs.iter().map(|b| SqlValue::Blob(b.clone())).collect();
	let mut del_stmt =
		tx.prepare(&del_sql).map_err(|e| CdcError::Internal(format!("compact delete prepare: {e}")))?;
	let rows_deleted = del_stmt
		.execute(params_from_iter(del_params.iter()))
		.map_err(|e| CdcError::Internal(format!("compact delete execute: {e}")))?;
	Ok(rows_deleted == expected_count)
}

#[inline]
#[allow(clippy::too_many_arguments)]
fn insert_compacted_block(
	tx: &Transaction<'_>,
	payload: &[u8],
	rollup_bytes: &[u8],
	min_version: CommitVersion,
	max_version: CommitVersion,
	min_ts_nanos: i64,
	max_ts_nanos: i64,
	num_entries: usize,
) -> CdcStorageResult<()> {
	tx.prepare_cached(
		r#"INSERT INTO "cdc_block"
		   (max_version, min_version, min_timestamp, max_timestamp, num_entries, payload, stats_rollup)
		   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
	)
	.map_err(|e| CdcError::Internal(format!("compact insert block prepare: {e}")))?
	.execute(params![
		version_to_bytes(max_version).as_slice(),
		version_to_bytes(min_version).as_slice(),
		min_ts_nanos,
		max_ts_nanos,
		num_entries as i64,
		payload,
		rollup_bytes,
	])
	.map_err(|e| CdcError::Internal(format!("compact insert block: {e}")))?;
	Ok(())
}

impl CdcStorage for SqliteCdcStorage {
	#[instrument(name = "store::cdc::sqlite::write", level = "debug", skip_all)]
	fn write(&self, cdc: &Cdc) -> CdcStorageResult<()> {
		let bytes = cdc::encode(cdc, ROW_ZSTD_LEVEL)?;
		let rollup = aggregate_evictions(&cdc.changes);
		let rollup_bytes =
			to_stdvec(&rollup).map_err(|e| CdcError::Codec(format!("postcard encode rollup: {e}")))?;
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(());
		};
		conn.prepare_cached(
			r#"INSERT OR REPLACE INTO "cdc" (version, payload, created_at, stats_rollup) VALUES (?1, ?2, ?3, ?4)"#,
		)
		.map_err(|e| CdcError::Internal(format!("insert cdc prepare: {e}")))?
		.execute(params![
			version_to_bytes(cdc.version).as_slice(),
			bytes.as_slice(),
			datetime_to_nanos(&cdc.timestamp),
			rollup_bytes.as_slice()
		])
		.map_err(|e| CdcError::Internal(format!("insert cdc: {e}")))?;

		Ok(())
	}

	#[instrument(name = "store::cdc::sqlite::read", level = "debug", skip_all)]
	fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		self.read_with(version)
	}

	#[instrument(name = "store::cdc::sqlite::read_range", level = "debug", skip_all)]
	fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		self.read_range_with(start, end, batch_size)
	}

	fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		Ok(self.read(version)?.map(|c| c.changes.len()).unwrap_or(0))
	}

	fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(None);
		};
		let block_min = query_min_block(conn)?;
		let live_min = query_min_live(conn)?;
		Ok([block_min, live_min].into_iter().flatten().min())
	}

	fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(None);
		};
		if let Some(v) = query_max_live(conn)? {
			return Ok(Some(v));
		}
		query_max_block(conn)
	}

	fn truncated_before(&self) -> CdcStorageResult<CommitVersion> {
		Ok(CommitVersion(self.inner.truncated_before.load(Ordering::Acquire)))
	}

	#[instrument(name = "store::cdc::sqlite::drop_before", level = "debug", skip_all)]
	fn drop_before(&self, version: CommitVersion, limit: usize) -> CdcStorageResult<DropBeforeResult> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(DropBeforeResult::default());
		};

		let cutoff_bytes = version_to_bytes(version);
		let (effective, more_remaining) = if limit == usize::MAX {
			(version, false)
		} else {
			match live_cutoff_at_offset(conn, &cutoff_bytes, limit)? {
				Some(v) => (v, true),
				None => (version, false),
			}
		};

		let version_bytes = version_to_bytes(effective);
		let zstd_level = self.inner.last_zstd_level.load(Ordering::Relaxed);

		let full_blocks = self.scan_full_blocks_below(conn, &version_bytes)?;
		let straddle = self.scan_straddle_blocks(conn, effective, &version_bytes)?;
		let live = scan_live_rows_below(conn, &version_bytes)?;

		let max_deleted =
			[full_blocks.max_deleted, straddle.max_deleted, live.max_deleted].into_iter().flatten().max();
		let floor = max_deleted.map(|v| v.saturating_add(1));

		apply_drop_before(conn, &full_blocks.pks, &straddle.actions, &version_bytes, zstd_level, floor)?;

		if let Some(floor) = floor {
			self.inner.truncated_before.fetch_max(floor, Ordering::Release);
		}

		let mut entries = full_blocks.entries;
		entries.extend(straddle.entries);
		entries.extend(live.entries);
		let entries = merge_evictions(entries);
		let count = total_evicted_count(&entries);
		Ok(DropBeforeResult {
			count,
			entries,
			more_remaining,
		})
	}

	fn find_ttl_cutoff(&self, cutoff: DateTime) -> CdcStorageResult<Option<CommitVersion>> {
		let cutoff_nanos = datetime_to_nanos(&cutoff);
		if let Some(v) = self.try_block_index_cutoff(cutoff_nanos)? {
			return Ok(Some(v));
		}
		if let Some(v) = self.scan_live_cutoff_indexed(cutoff_nanos)? {
			return Ok(Some(v));
		}
		self.max_version().map(|opt| opt.map(|v| CommitVersion(v.0.saturating_add(1))))
	}
}

impl SqliteCdcStorage {
	#[instrument(name = "store::cdc::sqlite::read_range_hot", level = "debug", skip_all)]
	pub fn read_range_hot(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		self.read_range_with(start, end, batch_size)
	}

	fn read_with(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		if let Some(cdc) = self.read_live(version)? {
			return Ok(Some(cdc));
		}
		self.read_from_blocks(version)
	}

	fn read_range_with(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		reifydb_assertions! {
			assert!(
				batch_size > 0,
				"a zero batch size yields an empty batch with has_more set, which callers paginate on forever"
			);
		}
		let Some((lo_inc, hi_inc)) = normalize_range_inclusive(start, end) else {
			return Ok(CdcBatch {
				items: Vec::new(),
				has_more: false,
			});
		};
		let want = batch_size as usize;

		let (block_rows, block_frontier, live_payloads) =
			self.snapshot_block_and_live(lo_inc, hi_inc, batch_size)?;
		let block_items = self.decode_block_rows(block_rows, lo_inc, hi_inc)?;
		let live_items = decode_live_payloads(live_payloads)?;
		let mut merged = merge_block_and_live(block_items, live_items);

		if let Some(frontier) = block_frontier {
			let cut = merged.iter().position(|c| c.version > frontier).unwrap_or(merged.len());
			merged.truncate(cut);
		}

		let has_more = merged.len() > want || block_frontier.is_some();
		merged.truncate(want);

		Ok(CdcBatch {
			items: merged,
			has_more,
		})
	}

	#[inline]
	fn try_block_index_cutoff(&self, cutoff_nanos: i64) -> CdcStorageResult<Option<CommitVersion>> {
		let block_hit: Option<Vec<u8>> = {
			let guard = self.inner.readers.acquire();
			let Some(conn) = guard.as_ref() else {
				return Ok(None);
			};
			conn.prepare_cached(
				r#"SELECT min_version FROM "cdc_block"
				   WHERE max_timestamp >= ?1 ORDER BY max_timestamp ASC LIMIT 1"#,
			)
			.ok()
			.and_then(|mut stmt| stmt.query_row(params![cutoff_nanos], |row| row.get::<_, Vec<u8>>(0)).ok())
		};
		block_hit.map(|b| bytes_to_version(&b)).transpose()
	}

	#[inline]
	fn scan_live_cutoff_indexed(&self, cutoff_nanos: i64) -> CdcStorageResult<Option<CommitVersion>> {
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(None);
		};
		let result = conn
			.prepare_cached(
				r#"SELECT version FROM "cdc"
				   WHERE created_at >= ?1 ORDER BY created_at ASC LIMIT 1"#,
			)
			.map_err(|e| CdcError::Internal(format!("ttl cutoff prepare: {e}")))?
			.query_row(params![cutoff_nanos], |row| row.get::<_, Vec<u8>>(0));
		match result {
			Ok(bytes) => Ok(Some(bytes_to_version(&bytes)?)),
			Err(QueryReturnedNoRows) => Ok(None),
			Err(e) => Err(CdcError::Internal(format!("ttl cutoff query: {e}"))),
		}
	}

	#[inline]
	fn read_live(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(None);
		};
		let result = conn
			.prepare_cached(r#"SELECT payload FROM "cdc" WHERE version = ?1"#)
			.map_err(|e| CdcError::Internal(format!("read cdc prepare: {e}")))?
			.query_row(params![version_to_bytes(version).as_slice()], |row| row.get::<_, Vec<u8>>(0));
		match result {
			Ok(bytes) => Ok(Some(cdc::decode::<Cdc>(&bytes)?)),
			Err(QueryReturnedNoRows) => Ok(None),
			Err(e) => Err(CdcError::Internal(format!("read cdc: {e}"))),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
	use reifydb_core::interface::cdc::CdcChange;
	use reifydb_value::util::cowvec::CowVec;

	use super::*;

	fn cdc_at(version: u64) -> Cdc {
		Cdc::new(
			CommitVersion(version),
			DateTime::from_nanos(1_700_000_000_000_000_000 + version * 1_000_000),
			vec![CdcChange::Insert {
				key: EncodedKey::new(format!("k-{version}").into_bytes()),
				post: EncodedBytes(CowVec::new(format!("v-{version}").into_bytes())),
			}],
		)
	}

	fn decoded_block_count(store: &SqliteCdcStorage, blocks: u64, block_size: u64) -> usize {
		(1..=blocks).filter(|b| store.inner.block_cache.get(CommitVersion(b * block_size)).is_some()).count()
	}

	#[test]
	fn read_range_decodes_only_the_blocks_needed_for_the_batch() {
		// A wide range with a small batch must not drag every overlapping block through zstd decode.
		// Unbounded, one pull re-inflates the whole backlog and the lag feeds itself.
		let (config, _guard) = SqliteConfig::in_memory();
		let store = SqliteCdcStorage::new_with_cache_capacity(config, 4096);
		for v in 1..=2000u64 {
			store.write(&cdc_at(v)).unwrap();
		}
		assert_eq!(store.compact_all(10, 1, CommitVersion(u64::MAX)).unwrap().len(), 200);

		let batch = store
			.read_range(Bound::Excluded(CommitVersion(0)), Bound::Included(CommitVersion(u64::MAX)), 5)
			.unwrap();

		assert_eq!(batch.items.len(), 5);
		assert!(batch.has_more);
		assert_eq!(batch.items.iter().map(|c| c.version.0).collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);

		let decoded = decoded_block_count(&store, 200, 10);
		assert!(decoded <= 2, "expected at most 2 of 200 blocks decoded for a 5 item batch, got {decoded}");
	}

	#[test]
	fn wide_range_pagination_yields_every_entry_exactly_once() {
		// Live rows sort above every block, so a short block prefix must never let them jump the
		// versions still sitting in unread blocks: that would silently skip a stretch of the log.
		let (store, _guard) = SqliteCdcStorage::in_memory();
		for v in 1..=1000u64 {
			store.write(&cdc_at(v)).unwrap();
		}
		assert_eq!(store.compact_all(10, 1, CommitVersion(u64::MAX)).unwrap().len(), 100);
		for v in 1001..=1050u64 {
			store.write(&cdc_at(v)).unwrap();
		}

		let first = store
			.read_range(Bound::Excluded(CommitVersion(0)), Bound::Included(CommitVersion(u64::MAX)), 7)
			.unwrap();
		assert!(first.has_more, "a 7 item batch over 1050 entries must report more");
		assert_eq!(first.items.len(), 7);

		let mut seen: Vec<u64> = Vec::new();
		let mut cursor = Bound::Excluded(CommitVersion(0));
		for _ in 0..4000 {
			let batch = store.read_range(cursor, Bound::Included(CommitVersion(u64::MAX)), 7).unwrap();
			if batch.has_more {
				assert!(!batch.items.is_empty(), "has_more with no items stalls every consumer");
			}
			assert!(batch.items.len() <= 7, "batch must never exceed the requested size");
			seen.extend(batch.items.iter().map(|c| c.version.0));
			if !batch.has_more {
				break;
			}
			cursor = Bound::Excluded(batch.items.last().unwrap().version);
		}

		assert_eq!(
			seen,
			(1..=1050u64).collect::<Vec<_>>(),
			"pagination must yield every version once, in order"
		);
	}
}
