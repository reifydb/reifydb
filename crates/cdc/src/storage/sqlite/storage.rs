// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::Bound,
	iter::repeat_n,
	sync::{
		Arc,
		atomic::{AtomicU8, Ordering},
	},
};

use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcBatch, SystemChange},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sqlite::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	pragma,
};
use reifydb_type::value::datetime::DateTime;
use rusqlite::{
	Connection, Error::QueryReturnedNoRows, Result as RusqliteResult, Transaction, params, params_from_iter,
	types::Value as SqlValue,
};

use crate::{
	compact::{block, block::CompactBlockSummary, cache::BlockCache},
	error::CdcError,
	storage::{CdcStorage, CdcStorageResult, DropBeforeResult, DroppedCdcEntry, normalize_range_inclusive},
};

#[derive(Clone)]
pub struct SqliteCdcStorage {
	inner: Arc<Inner>,
}

struct Inner {
	conn: Mutex<Connection>,
	block_cache: BlockCache,
	last_zstd_level: AtomicU8,
}

/// `(decoded entries, raw version blobs)` returned from `select_oldest_eligible`.
type CompactionCandidates = (Vec<Cdc>, Vec<Vec<u8>>);

/// `(block index rows: (max_version_blob, payload), live row payloads)` returned
/// from `snapshot_block_and_live`. Block rows carry the PK so the caller can key
/// the cache; live rows are bare payloads.
type RangeSnapshot = (Vec<(Vec<u8>, Vec<u8>)>, Vec<Vec<u8>>);

struct FullBlockScan {
	cdc_count: usize,
	entries: Vec<DroppedCdcEntry>,
	pks: Vec<Vec<u8>>,
}

struct StraddleScan {
	cdc_count: usize,
	entries: Vec<DroppedCdcEntry>,
	actions: Vec<(Vec<u8>, BlockOutcome)>,
}

struct LiveScan {
	cdc_count: usize,
	entries: Vec<DroppedCdcEntry>,
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
		Self {
			inner: Arc::new(Inner {
				conn: Mutex::new(conn),
				block_cache: BlockCache::new(cache_capacity),
				last_zstd_level: AtomicU8::new(3),
			}),
		}
	}

	pub fn in_memory() -> Self {
		Self::new(SqliteConfig::in_memory())
	}

	fn ensure_schema(conn: &Connection) {
		create_cdc_table(conn);
		create_cdc_block_table(conn);
		create_block_timestamp_index(conn);
	}

	pub fn incremental_vacuum(&self) {
		let _ = pragma::incremental_vacuum(&self.inner.conn.lock());
	}

	pub fn shrink_memory(&self) {
		let _ = pragma::shrink_memory(&self.inner.conn.lock());
	}

	pub fn shutdown(&self) {
		let _ = pragma::shutdown(&self.inner.conn.lock());
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
		let conn = self.inner.conn.lock();
		conn.query_row(
			r#"SELECT max_version, payload FROM "cdc_block"
			   WHERE max_version >= ?1 AND min_version <= ?1
			   ORDER BY max_version ASC LIMIT 1"#,
			params![v_bytes.as_slice()],
			|row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
		)
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
		let entries = block::decode(payload)?;
		let arc = Arc::new(entries);
		self.inner.block_cache.put(block_max, arc.clone());
		Ok(arc)
	}

	fn read_range_live(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		let (lower_sql, lower_bytes) = lower_bind_clause(start);
		let (upper_sql, upper_bytes) = upper_bind_clause(end);
		let sql = format!(
			r#"SELECT payload FROM "cdc" WHERE 1=1{lower_sql}{upper_sql} ORDER BY version ASC LIMIT ?"#
		);
		let limit = (batch_size as i64).saturating_add(1);

		let conn = self.inner.conn.lock();
		let mut stmt = conn.prepare(&sql).map_err(|e| CdcError::Internal(format!("range prepare: {e}")))?;
		let values = build_range_params(lower_bytes, upper_bytes, limit);
		let rows = stmt
			.query_map(params_from_iter(values.iter()), |row| row.get::<_, Vec<u8>>(0))
			.map_err(|e| CdcError::Internal(format!("range rows: {e}")))?;

		let (items, has_more) = decode_payload_rows(rows, batch_size as usize)?;
		Ok(CdcBatch {
			items,
			has_more,
		})
	}

	fn min_version_live(&self) -> CdcStorageResult<Option<CommitVersion>> {
		let conn = self.inner.conn.lock();
		let r: Option<Vec<u8>> = conn
			.query_row(r#"SELECT MIN(version) FROM "cdc""#, [], |row| row.get::<_, Option<Vec<u8>>>(0))
			.ok()
			.flatten();
		r.map(|b| bytes_to_version(&b)).transpose()
	}

	fn max_version_blocks(&self) -> CdcStorageResult<Option<CommitVersion>> {
		let conn = self.inner.conn.lock();
		let r: Option<Vec<u8>> = conn
			.query_row(r#"SELECT MAX(max_version) FROM "cdc_block""#, [], |row| {
				row.get::<_, Option<Vec<u8>>>(0)
			})
			.ok()
			.flatten();
		r.map(|b| bytes_to_version(&b)).transpose()
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

		let payload = block::encode(&entries, zstd_level)?;
		let compressed_bytes = payload.len();
		let (min_ts_nanos, max_ts_nanos) = summarize_timestamps(&entries);
		let min_version = entries.first().unwrap().version;
		let max_version = entries.last().unwrap().version;

		let committed = self.commit_block_swap(
			&version_blobs,
			&payload,
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

	/// Phase A: short-lived read under the connection mutex. No txn.
	///
	/// Returns `Some((entries, version_blobs))` if a viable batch exists,
	/// or `None` when there is nothing to compact (no live entries, max
	/// below safety_lag, or fewer than target_size eligible rows when
	/// partial blocks are not allowed).
	fn select_oldest_eligible(
		&self,
		target_size: usize,
		safety_lag: u64,
		allow_partial: bool,
		producer_watermark: CommitVersion,
	) -> CdcStorageResult<Option<CompactionCandidates>> {
		let conn = self.inner.conn.lock();
		let Some(max_v) = query_max_live_version(&conn)? else {
			return Ok(None);
		};
		let Some(eligible_max) = compute_eligible_max(max_v, safety_lag, producer_watermark) else {
			return Ok(None);
		};
		let (entries, version_blobs) = query_oldest_candidates(&conn, eligible_max, target_size)?;
		if entries.is_empty() {
			return Ok(None);
		}
		if !allow_partial && entries.len() < target_size {
			return Ok(None);
		}
		Ok(Some((entries, version_blobs)))
	}

	/// Phase C: short-lived commit under the connection mutex.
	///
	/// DELETE first so we can detect a concurrent `drop_before` via
	/// rows_affected; rolls back and returns `Ok(false)` if the row count
	/// mismatches (next tick retries on a fresh snapshot). Returns
	/// `Ok(true)` after a successful swap.
	#[allow(clippy::too_many_arguments)]
	fn commit_block_swap(
		&self,
		version_blobs: &[Vec<u8>],
		payload: &[u8],
		min_version: CommitVersion,
		max_version: CommitVersion,
		min_ts_nanos: i64,
		max_ts_nanos: i64,
		num_entries: usize,
	) -> CdcStorageResult<bool> {
		let conn = self.inner.conn.lock();
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
			min_version,
			max_version,
			min_ts_nanos,
			max_ts_nanos,
			num_entries,
		)?;
		tx.commit().map_err(|e| CdcError::Internal(format!("compact commit: {e}")))?;
		Ok(true)
	}

	/// Snapshot both `cdc_block` and `cdc` under a single connection lock so the
	/// two reads are consistent. Without this, a concurrent compactor can move
	/// an entry from `cdc` into a new block between the two reads and the row
	/// goes missing in the merged output: we miss it in the block read (block
	/// didn't exist yet) and miss it in the live read (row already deleted from
	/// cdc).
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
		let conn = self.inner.conn.lock();

		let block_rows = read_block_index_rows(&conn, &lo_b, &hi_b)?;
		let live_payloads = read_live_payloads(&conn, &lo_b, &hi_b, limit)?;
		Ok((block_rows, live_payloads))
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
			.prepare(
				r#"SELECT max_version, payload FROM "cdc_block"
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
		let mut cdc_count = 0;
		for row in rows {
			let (max_bytes, payload) =
				row.map_err(|e| CdcError::Internal(format!("drop blocks row: {e}")))?;
			let block_max = bytes_to_version(&max_bytes)?;
			for cdc in &block::decode(&payload)? {
				cdc_count += 1;
				extend_dropped_entries(&mut entries, &cdc.system_changes);
			}
			self.inner.block_cache.remove(block_max);
			pks.push(max_bytes);
		}
		Ok(FullBlockScan {
			cdc_count,
			entries,
			pks,
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
			.prepare(
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
		let mut cdc_count = 0;
		for row in rows {
			let (max_bytes, payload) =
				row.map_err(|e| CdcError::Internal(format!("drop straddle row: {e}")))?;
			let block_max = bytes_to_version(&max_bytes)?;
			let decoded = block::decode(&payload)?;
			let mut survivors: Vec<Cdc> = Vec::with_capacity(decoded.len());
			for cdc in decoded {
				if cdc.version < version {
					cdc_count += 1;
					extend_dropped_entries(&mut entries, &cdc.system_changes);
				} else {
					survivors.push(cdc);
				}
			}
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
			cdc_count,
			entries,
			actions,
		})
	}
}

fn open_connection(config: &SqliteConfig) -> Connection {
	let db_path = resolve_db_path(config.path.clone(), "cdc.db");
	let flags = convert_flags(&config.flags);
	let conn = connect(&db_path, flags).expect("Failed to connect to CDC SQLite database");
	pragma::apply(&conn, config).expect("Failed to configure CDC SQLite pragmas");
	SqliteCdcStorage::ensure_schema(&conn);
	conn
}

fn create_cdc_table(conn: &Connection) {
	conn.execute(
		r#"CREATE TABLE IF NOT EXISTS "cdc" (
			version BLOB PRIMARY KEY,
			payload BLOB NOT NULL
		) WITHOUT ROWID"#,
		[],
	)
	.expect("Failed to create cdc table");
}

fn create_cdc_block_table(conn: &Connection) {
	conn.execute(
		r#"CREATE TABLE IF NOT EXISTS "cdc_block" (
			max_version BLOB PRIMARY KEY,
			min_version BLOB NOT NULL,
			min_timestamp INTEGER NOT NULL,
			max_timestamp INTEGER NOT NULL,
			num_entries INTEGER NOT NULL,
			payload BLOB NOT NULL
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

#[inline]
fn lower_bind_clause(start: Bound<CommitVersion>) -> (&'static str, Option<[u8; 8]>) {
	match start {
		Bound::Included(v) => (" AND version >= ?", Some(version_to_bytes(v))),
		Bound::Excluded(v) => (" AND version > ?", Some(version_to_bytes(v))),
		Bound::Unbounded => ("", None),
	}
}

#[inline]
fn upper_bind_clause(end: Bound<CommitVersion>) -> (&'static str, Option<[u8; 8]>) {
	match end {
		Bound::Included(v) => (" AND version <= ?", Some(version_to_bytes(v))),
		Bound::Excluded(v) => (" AND version < ?", Some(version_to_bytes(v))),
		Bound::Unbounded => ("", None),
	}
}

#[inline]
fn build_range_params(lower_bytes: Option<[u8; 8]>, upper_bytes: Option<[u8; 8]>, limit: i64) -> Vec<SqlValue> {
	let mut values: Vec<SqlValue> = Vec::new();
	if let Some(b) = lower_bytes {
		values.push(SqlValue::Blob(b.to_vec()));
	}
	if let Some(b) = upper_bytes {
		values.push(SqlValue::Blob(b.to_vec()));
	}
	values.push(SqlValue::Integer(limit));
	values
}

#[inline]
fn decode_payload_rows<I>(rows: I, batch_size: usize) -> CdcStorageResult<(Vec<Cdc>, bool)>
where
	I: IntoIterator<Item = RusqliteResult<Vec<u8>>>,
{
	let mut items: Vec<Cdc> = Vec::new();
	for row in rows {
		let bytes = row.map_err(|e| CdcError::Internal(format!("range row: {e}")))?;
		let cdc: Cdc =
			from_bytes(&bytes).map_err(|e| CdcError::Codec(format!("postcard decode range: {e}")))?;
		items.push(cdc);
	}
	let has_more = items.len() > batch_size;
	if has_more {
		items.truncate(batch_size);
	}
	Ok((items, has_more))
}

#[inline]
fn query_max_live_version(conn: &Connection) -> CdcStorageResult<Option<u64>> {
	let max_live: Option<Vec<u8>> = conn
		.query_row(r#"SELECT MAX(version) FROM "cdc""#, [], |row| row.get::<_, Option<Vec<u8>>>(0))
		.ok()
		.flatten();
	max_live.map(|b| bytes_to_version(&b).map(|v| v.0)).transpose()
}

/// Cap eligibility at the CDC producer's commit watermark. Below this watermark,
/// every PostCommitEvent has been fully processed by the producer actor, so the
/// cdc table contains the complete set of entries for those versions. Above the
/// watermark, an in-flight producer write could still land at a version we are
/// about to pack, breaking the invariant that for any block, every CDC version
/// in [block.min, block.max] is contained in that block.
///
/// Returns `None` if `max_v < safety_lag` (nothing eligible yet).
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
		.prepare(
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
		let cdc: Cdc =
			from_bytes(&pb).map_err(|e| CdcError::Codec(format!("postcard decode in compact: {e}")))?;
		version_blobs.push(vb);
		entries.push(cdc);
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
		.query_row(r#"SELECT MIN(min_version) FROM "cdc_block""#, [], |row| row.get::<_, Option<Vec<u8>>>(0))
		.ok()
		.flatten();
	r.map(|b| bytes_to_version(&b)).transpose()
}

#[inline]
fn query_min_live(conn: &Connection) -> CdcStorageResult<Option<CommitVersion>> {
	let r: Option<Vec<u8>> = conn
		.query_row(r#"SELECT MIN(version) FROM "cdc""#, [], |row| row.get::<_, Option<Vec<u8>>>(0))
		.ok()
		.flatten();
	r.map(|b| bytes_to_version(&b)).transpose()
}

#[inline]
fn query_max_live(conn: &Connection) -> CdcStorageResult<Option<CommitVersion>> {
	let r: Option<Vec<u8>> = conn
		.query_row(r#"SELECT MAX(version) FROM "cdc""#, [], |row| row.get::<_, Option<Vec<u8>>>(0))
		.ok()
		.flatten();
	r.map(|b| bytes_to_version(&b)).transpose()
}

#[inline]
fn query_max_block(conn: &Connection) -> CdcStorageResult<Option<CommitVersion>> {
	let r: Option<Vec<u8>> = conn
		.query_row(r#"SELECT MAX(max_version) FROM "cdc_block""#, [], |row| row.get::<_, Option<Vec<u8>>>(0))
		.ok()
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

/// Fold an entry slice's timestamps into `(min, max)` nanos. Timestamps are
/// not guaranteed monotonic with version (clock skew, batched commits) so we
/// compute the range explicitly rather than taking first/last.
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
) -> CdcStorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
	let mut stmt = conn
		.prepare(
			r#"SELECT max_version, payload FROM "cdc_block"
			   WHERE max_version >= ?1 AND min_version <= ?2
			   ORDER BY max_version ASC"#,
		)
		.map_err(|e| CdcError::Internal(format!("range blocks prepare: {e}")))?;
	let rows = stmt
		.query_map(params![lo_b.as_slice(), hi_b.as_slice()], |row| {
			Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
		})
		.map_err(|e| CdcError::Internal(format!("range blocks rows: {e}")))?;
	let mut out = Vec::new();
	for r in rows {
		out.push(r.map_err(|e| CdcError::Internal(format!("range blocks row: {e}")))?);
	}
	Ok(out)
}

#[inline]
fn read_live_payloads(conn: &Connection, lo_b: &[u8; 8], hi_b: &[u8; 8], limit: i64) -> CdcStorageResult<Vec<Vec<u8>>> {
	let mut stmt = conn
		.prepare(
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
		let cdc: Cdc = from_bytes(&payload)
			.map_err(|e| CdcError::Codec(format!("postcard decode range live: {e}")))?;
		live_items.push(cdc);
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
			// Same version in both (compactor swap raced with our read);
			// keep the block copy and skip the live duplicate.
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
		.prepare(r#"SELECT payload FROM "cdc" WHERE version < ?1 ORDER BY version ASC"#)
		.map_err(|e| CdcError::Internal(format!("drop_before prepare: {e}")))?;
	let rows = stmt
		.query_map(params![version_bytes.as_slice()], |row| row.get::<_, Vec<u8>>(0))
		.map_err(|e| CdcError::Internal(format!("drop_before rows: {e}")))?;
	let mut entries = Vec::new();
	let mut cdc_count = 0;
	for row in rows {
		let bytes = row.map_err(|e| CdcError::Internal(format!("drop_before row: {e}")))?;
		let cdc: Cdc =
			from_bytes(&bytes).map_err(|e| CdcError::Codec(format!("postcard decode drop_before: {e}")))?;
		cdc_count += 1;
		extend_dropped_entries(&mut entries, &cdc.system_changes);
	}
	Ok(LiveScan {
		cdc_count,
		entries,
	})
}

#[inline]
fn apply_drop_before(
	conn: &Connection,
	full_block_pks: &[Vec<u8>],
	straddle_actions: &[(Vec<u8>, BlockOutcome)],
	version_bytes: &[u8; 8],
	zstd_level: u8,
) -> CdcStorageResult<()> {
	let tx = conn.unchecked_transaction().map_err(|e| CdcError::Internal(format!("drop_before tx begin: {e}")))?;

	for pk in full_block_pks {
		tx.execute(r#"DELETE FROM "cdc_block" WHERE max_version = ?1"#, params![pk.as_slice()])
			.map_err(|e| CdcError::Internal(format!("drop block delete: {e}")))?;
	}

	for (max_bytes, action) in straddle_actions {
		match action {
			BlockOutcome::Delete => {
				tx.execute(
					r#"DELETE FROM "cdc_block" WHERE max_version = ?1"#,
					params![max_bytes.as_slice()],
				)
				.map_err(|e| CdcError::Internal(format!("drop straddle delete: {e}")))?;
			}
			BlockOutcome::Rewrite {
				survivors,
			} => {
				rewrite_straddle_block(&tx, max_bytes, survivors, zstd_level)?;
			}
		}
	}

	tx.execute(r#"DELETE FROM "cdc" WHERE version < ?1"#, params![version_bytes.as_slice()])
		.map_err(|e| CdcError::Internal(format!("drop_before delete: {e}")))?;
	tx.commit().map_err(|e| CdcError::Internal(format!("drop_before commit: {e}")))?;
	Ok(())
}

#[inline]
fn rewrite_straddle_block(
	tx: &Transaction<'_>,
	max_bytes: &[u8],
	survivors: &[Cdc],
	zstd_level: u8,
) -> CdcStorageResult<()> {
	let new_min = survivors.first().unwrap().version;
	let new_max = survivors.last().unwrap().version;
	debug_assert_eq!(new_max, bytes_to_version(max_bytes)?, "max_version is the block PK and must be preserved");
	let (min_ts_nanos, max_ts_nanos) = summarize_timestamps(survivors);
	let payload = block::encode(survivors, zstd_level)?;
	tx.execute(
		r#"INSERT OR REPLACE INTO "cdc_block"
		   (max_version, min_version, min_timestamp, max_timestamp, num_entries, payload)
		   VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
		params![
			max_bytes,
			version_to_bytes(new_min).as_slice(),
			min_ts_nanos,
			max_ts_nanos,
			survivors.len() as i64,
			payload.as_slice(),
		],
	)
	.map_err(|e| CdcError::Internal(format!("drop straddle rewrite: {e}")))?;
	Ok(())
}

#[inline]
fn extend_dropped_entries(out: &mut Vec<DroppedCdcEntry>, system_changes: &[SystemChange]) {
	for sys_change in system_changes {
		out.push(DroppedCdcEntry {
			key: sys_change.key().clone(),
			value_bytes: sys_change.value_bytes() as u64,
		});
	}
}

/// Delete the live CDC rows whose payloads are now folded into a block.
/// Returns `true` if the row count matched `expected_count` (caller commits),
/// `false` if a concurrent `drop_before` already removed some (caller rolls
/// back so the next compactor tick retries on a fresh snapshot).
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
fn insert_compacted_block(
	tx: &Transaction<'_>,
	payload: &[u8],
	min_version: CommitVersion,
	max_version: CommitVersion,
	min_ts_nanos: i64,
	max_ts_nanos: i64,
	num_entries: usize,
) -> CdcStorageResult<()> {
	tx.execute(
		r#"INSERT INTO "cdc_block"
		   (max_version, min_version, min_timestamp, max_timestamp, num_entries, payload)
		   VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
		params![
			version_to_bytes(max_version).as_slice(),
			version_to_bytes(min_version).as_slice(),
			min_ts_nanos,
			max_ts_nanos,
			num_entries as i64,
			payload,
		],
	)
	.map_err(|e| CdcError::Internal(format!("compact insert block: {e}")))?;
	Ok(())
}

impl CdcStorage for SqliteCdcStorage {
	fn write(&self, cdc: &Cdc) -> CdcStorageResult<()> {
		let bytes = to_stdvec(cdc).map_err(|e| CdcError::Codec(format!("postcard encode: {e}")))?;
		let conn = self.inner.conn.lock();
		conn.execute(
			r#"INSERT OR REPLACE INTO "cdc" (version, payload) VALUES (?1, ?2)"#,
			params![version_to_bytes(cdc.version).as_slice(), bytes.as_slice()],
		)
		.map_err(|e| CdcError::Internal(format!("insert cdc: {e}")))?;
		Ok(())
	}

	fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		if let Some(cdc) = self.read_live(version)? {
			return Ok(Some(cdc));
		}
		self.read_from_blocks(version)
	}

	fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		let Some((lo_inc, hi_inc)) = normalize_range_inclusive(start, end) else {
			return Ok(CdcBatch {
				items: Vec::new(),
				has_more: false,
			});
		};
		let want = batch_size as usize;

		let (block_rows, live_payloads) = self.snapshot_block_and_live(lo_inc, hi_inc, batch_size)?;
		let block_items = self.decode_block_rows(block_rows, lo_inc, hi_inc)?;
		let live_items = decode_live_payloads(live_payloads)?;
		let mut merged = merge_block_and_live(block_items, live_items);

		let has_more = merged.len() > want;
		merged.truncate(want);
		Ok(CdcBatch {
			items: merged,
			has_more,
		})
	}

	fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		Ok(self.read(version)?.map(|c| c.system_changes.len()).unwrap_or(0))
	}

	fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		let conn = self.inner.conn.lock();
		let block_min = query_min_block(&conn)?;
		let live_min = query_min_live(&conn)?;
		Ok([block_min, live_min].into_iter().flatten().min())
	}

	fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		let conn = self.inner.conn.lock();
		if let Some(v) = query_max_live(&conn)? {
			return Ok(Some(v));
		}
		query_max_block(&conn)
	}

	fn drop_before(&self, version: CommitVersion) -> CdcStorageResult<DropBeforeResult> {
		let conn = self.inner.conn.lock();
		let version_bytes = version_to_bytes(version);
		let zstd_level = self.inner.last_zstd_level.load(Ordering::Relaxed);

		let full_blocks = self.scan_full_blocks_below(&conn, &version_bytes)?;
		let straddle = self.scan_straddle_blocks(&conn, version, &version_bytes)?;
		let live = scan_live_rows_below(&conn, &version_bytes)?;

		apply_drop_before(&conn, &full_blocks.pks, &straddle.actions, &version_bytes, zstd_level)?;
		let _ = pragma::incremental_vacuum(&conn);

		let mut entries = full_blocks.entries;
		entries.extend(straddle.entries);
		entries.extend(live.entries);
		Ok(DropBeforeResult {
			count: full_blocks.cdc_count + straddle.cdc_count + live.cdc_count,
			entries,
		})
	}

	fn find_ttl_cutoff(&self, cutoff: DateTime) -> CdcStorageResult<Option<CommitVersion>> {
		let cutoff_nanos = datetime_to_nanos(&cutoff);
		if let Some(v) = self.try_block_index_cutoff(cutoff_nanos)? {
			return Ok(Some(v));
		}
		let Some(start) = self.pick_scan_start()? else {
			return self.max_version_blocks().map(|opt| opt.map(|v| CommitVersion(v.0.saturating_add(1))));
		};
		self.scan_live_for_cutoff(start, cutoff_nanos)
	}
}

impl SqliteCdcStorage {
	/// Try the indexed `cdc_block.max_timestamp` lookup. Returns the smallest
	/// `min_version` of any block whose `max_timestamp >= cutoff`, or `None` if
	/// no block straddles the cutoff (caller falls back to scanning live rows).
	#[inline]
	fn try_block_index_cutoff(&self, cutoff_nanos: i64) -> CdcStorageResult<Option<CommitVersion>> {
		let block_hit: Option<Vec<u8>> = {
			let conn = self.inner.conn.lock();
			conn.query_row(
				r#"SELECT min_version FROM "cdc_block"
				   WHERE max_timestamp >= ?1 ORDER BY max_timestamp ASC LIMIT 1"#,
				params![cutoff_nanos],
				|row| row.get::<_, Vec<u8>>(0),
			)
			.ok()
		};
		block_hit.map(|b| bytes_to_version(&b)).transpose()
	}

	/// Pick the start of the live-row scan: the smallest live version, or
	/// `None` if the live table is empty (caller returns `block_max + 1`).
	#[inline]
	fn pick_scan_start(&self) -> CdcStorageResult<Option<CommitVersion>> {
		self.min_version_live()
	}

	#[inline]
	fn scan_live_for_cutoff(
		&self,
		start: CommitVersion,
		cutoff_nanos: i64,
	) -> CdcStorageResult<Option<CommitVersion>> {
		let mut next_start = Bound::Included(start);
		loop {
			let batch = self.read_range_live(next_start, Bound::Unbounded, 256)?;
			if batch.items.is_empty() {
				let last = self.max_version()?.unwrap_or(CommitVersion(0));
				return Ok(Some(CommitVersion(last.0.saturating_add(1))));
			}
			for cdc in &batch.items {
				if datetime_to_nanos(&cdc.timestamp) >= cutoff_nanos {
					return Ok(Some(cdc.version));
				}
			}
			if !batch.has_more {
				let last = batch.items.last().unwrap().version;
				return Ok(Some(CommitVersion(last.0.saturating_add(1))));
			}
			next_start = Bound::Excluded(batch.items.last().unwrap().version);
		}
	}

	#[inline]
	fn read_live(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		let conn = self.inner.conn.lock();
		let result = conn.query_row(
			r#"SELECT payload FROM "cdc" WHERE version = ?1"#,
			params![version_to_bytes(version).as_slice()],
			|row| row.get::<_, Vec<u8>>(0),
		);
		match result {
			Ok(bytes) => {
				let cdc: Cdc = from_bytes(&bytes)
					.map_err(|e| CdcError::Codec(format!("postcard decode: {e}")))?;
				Ok(Some(cdc))
			}
			Err(QueryReturnedNoRows) => Ok(None),
			Err(e) => Err(CdcError::Internal(format!("read cdc: {e}"))),
		}
	}
}
