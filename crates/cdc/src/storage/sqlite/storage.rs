// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! SQLite-backed implementation of `CdcStorage`.
//!
//! Single table, one row per CommitVersion, payload is a postcard-encoded `Cdc`.
//! Concurrency: single `Mutex<Connection>` (rusqlite::Connection is Send but !Sync).

use std::{collections::Bound, iter::repeat_n, sync::Arc};

use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcBatch},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sqlite::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	pragma,
};
use reifydb_type::value::datetime::DateTime;
use rusqlite::{Connection, Error::QueryReturnedNoRows, params, params_from_iter, types::Value as SqlValue};

use crate::{
	compact::{block, block::CompactBlockSummary, cache::BlockCache},
	error::CdcError,
	storage::{CdcStorage, CdcStorageResult, DropBeforeResult, DroppedCdcEntry},
};

#[derive(Clone)]
pub struct SqliteCdcStorage {
	inner: Arc<Inner>,
}

struct Inner {
	conn: Mutex<Connection>,
	block_cache: BlockCache,
}

impl SqliteCdcStorage {
	pub fn new(config: SqliteConfig) -> Self {
		Self::new_with_cache_capacity(config, BlockCache::DEFAULT_CAPACITY)
	}

	pub fn new_with_cache_capacity(config: SqliteConfig, cache_capacity: usize) -> Self {
		let db_path = resolve_db_path(config.path.clone(), "cdc.db");
		let flags = convert_flags(&config.flags);

		let conn = connect(&db_path, flags).expect("Failed to connect to CDC SQLite database");
		pragma::apply(&conn, &config).expect("Failed to configure CDC SQLite pragmas");

		Self::ensure_schema(&conn);

		Self {
			inner: Arc::new(Inner {
				conn: Mutex::new(conn),
				block_cache: BlockCache::new(cache_capacity),
			}),
		}
	}

	pub fn in_memory() -> Self {
		Self::new(SqliteConfig::in_memory())
	}

	fn ensure_schema(conn: &Connection) {
		conn.execute(
			r#"CREATE TABLE IF NOT EXISTS "cdc" (
				version BLOB PRIMARY KEY,
				payload BLOB NOT NULL
			) WITHOUT ROWID"#,
			[],
		)
		.expect("Failed to create cdc table");

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

		conn.execute(
			r#"CREATE INDEX IF NOT EXISTS "cdc_block_max_ts_idx"
			   ON "cdc_block"(max_timestamp)"#,
			[],
		)
		.expect("Failed to create cdc_block_max_ts index");
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
		let row: Option<(Vec<u8>, Vec<u8>)> = {
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
			})?
		};
		let Some((max_bytes, payload)) = row else {
			return Ok(None);
		};
		let block_max = bytes_to_version(&max_bytes)?;
		let entries = self.load_block_cached(block_max, &payload)?;
		Ok(entries.iter().find(|c| c.version == version).cloned())
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
		let (lower_sql, lower_bytes) = match start {
			Bound::Included(v) => (" AND version >= ?", Some(version_to_bytes(v))),
			Bound::Excluded(v) => (" AND version > ?", Some(version_to_bytes(v))),
			Bound::Unbounded => ("", None),
		};
		let (upper_sql, upper_bytes) = match end {
			Bound::Included(v) => (" AND version <= ?", Some(version_to_bytes(v))),
			Bound::Excluded(v) => (" AND version < ?", Some(version_to_bytes(v))),
			Bound::Unbounded => ("", None),
		};
		let sql = format!(
			r#"SELECT payload FROM "cdc" WHERE 1=1{lower_sql}{upper_sql} ORDER BY version ASC LIMIT ?"#
		);
		let conn = self.inner.conn.lock();
		let mut stmt = conn.prepare(&sql).map_err(|e| CdcError::Internal(format!("range prepare: {e}")))?;

		let mut values: Vec<SqlValue> = Vec::new();
		if let Some(b) = lower_bytes {
			values.push(SqlValue::Blob(b.to_vec()));
		}
		if let Some(b) = upper_bytes {
			values.push(SqlValue::Blob(b.to_vec()));
		}
		let limit = (batch_size as i64).saturating_add(1);
		values.push(SqlValue::Integer(limit));

		let rows = stmt
			.query_map(params_from_iter(values.iter()), |row| row.get::<_, Vec<u8>>(0))
			.map_err(|e| CdcError::Internal(format!("range rows: {e}")))?;

		let mut items: Vec<Cdc> = Vec::new();
		for row in rows {
			let bytes = row.map_err(|e| CdcError::Internal(format!("range row: {e}")))?;
			let cdc: Cdc = from_bytes(&bytes)
				.map_err(|e| CdcError::Codec(format!("postcard decode range: {e}")))?;
			items.push(cdc);
		}
		let has_more = items.len() > batch_size as usize;
		if has_more {
			items.truncate(batch_size as usize);
		}
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
	) -> CdcStorageResult<Option<CompactBlockSummary>> {
		self.compact_oldest_inner(target_size, safety_lag, false, zstd_level)
	}

	pub fn compact_all(&self, target_size: usize, zstd_level: u8) -> CdcStorageResult<Vec<CompactBlockSummary>> {
		let mut out = Vec::new();
		while let Some(s) = self.compact_oldest_inner(target_size, 0, false, zstd_level)? {
			out.push(s);
		}
		if let Some(tail) = self.compact_oldest_inner(target_size, 0, true, zstd_level)? {
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
	) -> CdcStorageResult<Option<CompactBlockSummary>> {
		if target_size == 0 {
			return Ok(None);
		}

		let conn = self.inner.conn.lock();

		let max_live: Option<Vec<u8>> = conn
			.query_row(r#"SELECT MAX(version) FROM "cdc""#, [], |row| row.get::<_, Option<Vec<u8>>>(0))
			.ok()
			.flatten();
		let Some(max_bytes) = max_live else {
			return Ok(None);
		};
		let max_v = bytes_to_version(&max_bytes)?.0;
		if max_v < safety_lag {
			return Ok(None);
		}
		let eligible_max = CommitVersion(max_v - safety_lag);
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
			let cdc: Cdc = from_bytes(&pb)
				.map_err(|e| CdcError::Codec(format!("postcard decode in compact: {e}")))?;
			version_blobs.push(vb);
			entries.push(cdc);
		}
		drop(stmt);

		if entries.is_empty() {
			return Ok(None);
		}
		if !allow_partial && entries.len() < target_size {
			return Ok(None);
		}

		let min_version = entries.first().unwrap().version;
		let max_version = entries.last().unwrap().version;
		let min_v_bytes = version_to_bytes(min_version);
		let max_v_bytes = version_to_bytes(max_version);

		let (min_ts_nanos, max_ts_nanos) = entries.iter().fold((i64::MAX, i64::MIN), |(lo, hi), c| {
			let n = datetime_to_nanos(&c.timestamp);
			(lo.min(n), hi.max(n))
		});

		let payload = block::encode(&entries, zstd_level)?;

		let tx = conn
			.unchecked_transaction()
			.map_err(|e| CdcError::Internal(format!("compact tx begin: {e}")))?;

		tx.execute(
			r#"INSERT INTO "cdc_block"
			   (max_version, min_version, min_timestamp, max_timestamp, num_entries, payload)
			   VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
			params![
				max_v_bytes.as_slice(),
				min_v_bytes.as_slice(),
				min_ts_nanos,
				max_ts_nanos,
				entries.len() as i64,
				payload.as_slice(),
			],
		)
		.map_err(|e| CdcError::Internal(format!("compact insert block: {e}")))?;

		let placeholders = repeat_n("?", version_blobs.len()).collect::<Vec<_>>().join(",");
		let del_sql = format!(r#"DELETE FROM "cdc" WHERE version IN ({})"#, placeholders);
		let mut del_stmt =
			tx.prepare(&del_sql).map_err(|e| CdcError::Internal(format!("compact delete prepare: {e}")))?;
		let del_params: Vec<SqlValue> = version_blobs.iter().map(|b| SqlValue::Blob(b.clone())).collect();
		del_stmt.execute(params_from_iter(del_params.iter()))
			.map_err(|e| CdcError::Internal(format!("compact delete execute: {e}")))?;
		drop(del_stmt);

		tx.commit().map_err(|e| CdcError::Internal(format!("compact commit: {e}")))?;
		let compressed_bytes = payload.len();

		Ok(Some(CompactBlockSummary {
			min_version,
			max_version,
			num_entries: entries.len(),
			compressed_bytes,
		}))
	}
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
		{
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
					return Ok(Some(cdc));
				}
				Err(QueryReturnedNoRows) => {}
				Err(e) => return Err(CdcError::Internal(format!("read cdc: {e}"))),
			}
		}
		self.read_from_blocks(version)
	}

	fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		let lo_inc: CommitVersion = match start {
			Bound::Included(v) => v,
			Bound::Excluded(v) => CommitVersion(v.0.saturating_add(1)),
			Bound::Unbounded => CommitVersion(0),
		};
		let hi_inc: CommitVersion = match end {
			Bound::Included(v) => v,
			Bound::Excluded(v) => CommitVersion(v.0.saturating_sub(1)),
			Bound::Unbounded => CommitVersion(u64::MAX),
		};
		if lo_inc > hi_inc {
			return Ok(CdcBatch {
				items: Vec::new(),
				has_more: false,
			});
		}

		let want = batch_size as usize;
		let mut items: Vec<Cdc> = Vec::with_capacity(want.min(64));

		let block_rows: Vec<(Vec<u8>, Vec<u8>)> = {
			let conn = self.inner.conn.lock();
			let mut stmt = conn
				.prepare(
					r#"SELECT max_version, payload FROM "cdc_block"
					   WHERE max_version >= ?1 AND min_version <= ?2
					   ORDER BY max_version ASC"#,
				)
				.map_err(|e| CdcError::Internal(format!("range blocks prepare: {e}")))?;
			let lo_b = version_to_bytes(lo_inc);
			let hi_b = version_to_bytes(hi_inc);
			let rows = stmt
				.query_map(params![lo_b.as_slice(), hi_b.as_slice()], |row| {
					Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
				})
				.map_err(|e| CdcError::Internal(format!("range blocks rows: {e}")))?;
			let mut out = Vec::new();
			for r in rows {
				out.push(r.map_err(|e| CdcError::Internal(format!("range blocks row: {e}")))?);
			}
			out
		};

		let mut has_more = false;
		'outer: for (max_bytes, payload) in block_rows {
			let block_max = bytes_to_version(&max_bytes)?;
			let entries = self.load_block_cached(block_max, &payload)?;
			for cdc in entries.iter() {
				if cdc.version < lo_inc || cdc.version > hi_inc {
					continue;
				}
				if items.len() == want {
					has_more = true;
					break 'outer;
				}
				items.push(cdc.clone());
			}
		}

		if items.len() < want {
			let lo_live: Bound<CommitVersion> = match items.last() {
				Some(last) => Bound::Excluded(last.version),
				None => start,
			};
			let need = (want - items.len()) as u64;
			let live = self.read_range_live(lo_live, end, need)?;
			if live.has_more {
				has_more = true;
			}
			items.extend(live.items);
		}

		Ok(CdcBatch {
			items,
			has_more,
		})
	}

	fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		Ok(self.read(version)?.map(|c| c.system_changes.len()).unwrap_or(0))
	}

	fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		let conn = self.inner.conn.lock();
		let block_min: Option<Vec<u8>> = conn
			.query_row(r#"SELECT MIN(min_version) FROM "cdc_block""#, [], |row| {
				row.get::<_, Option<Vec<u8>>>(0)
			})
			.ok()
			.flatten();
		let live_min: Option<Vec<u8>> = conn
			.query_row(r#"SELECT MIN(version) FROM "cdc""#, [], |row| row.get::<_, Option<Vec<u8>>>(0))
			.ok()
			.flatten();
		let mut best: Option<CommitVersion> = None;
		for c in [block_min, live_min].into_iter().flatten() {
			let v = bytes_to_version(&c)?;
			best = Some(best.map_or(v, |b| b.min(v)));
		}
		Ok(best)
	}

	fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		let conn = self.inner.conn.lock();
		let live: Option<Vec<u8>> = conn
			.query_row(r#"SELECT MAX(version) FROM "cdc""#, [], |row| row.get::<_, Option<Vec<u8>>>(0))
			.ok()
			.flatten();
		if let Some(b) = live {
			return Ok(Some(bytes_to_version(&b)?));
		}
		let block_max: Option<Vec<u8>> = conn
			.query_row(r#"SELECT MAX(max_version) FROM "cdc_block""#, [], |row| {
				row.get::<_, Option<Vec<u8>>>(0)
			})
			.ok()
			.flatten();
		block_max.map(|b| bytes_to_version(&b)).transpose()
	}

	fn drop_before(&self, version: CommitVersion) -> CdcStorageResult<DropBeforeResult> {
		let conn = self.inner.conn.lock();
		let version_bytes = version_to_bytes(version);

		let mut entries = Vec::new();
		let mut count = 0usize;
		let mut block_pks_to_delete: Vec<Vec<u8>> = Vec::new();

		{
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
			for row in rows {
				let (max_bytes, payload) =
					row.map_err(|e| CdcError::Internal(format!("drop blocks row: {e}")))?;
				let block_max = bytes_to_version(&max_bytes)?;
				for cdc in &block::decode(&payload)? {
					count += 1;
					for sys_change in &cdc.system_changes {
						entries.push(DroppedCdcEntry {
							key: sys_change.key().clone(),
							value_bytes: sys_change.value_bytes() as u64,
						});
					}
				}
				self.inner.block_cache.remove(block_max);
				block_pks_to_delete.push(max_bytes);
			}
		}

		{
			let mut stmt = conn
				.prepare(r#"SELECT payload FROM "cdc" WHERE version < ?1 ORDER BY version ASC"#)
				.map_err(|e| CdcError::Internal(format!("drop_before prepare: {e}")))?;
			let rows = stmt
				.query_map(params![version_bytes.as_slice()], |row| row.get::<_, Vec<u8>>(0))
				.map_err(|e| CdcError::Internal(format!("drop_before rows: {e}")))?;
			for row in rows {
				let bytes = row.map_err(|e| CdcError::Internal(format!("drop_before row: {e}")))?;
				let cdc: Cdc = from_bytes(&bytes)
					.map_err(|e| CdcError::Codec(format!("postcard decode drop_before: {e}")))?;
				count += 1;
				for sys_change in &cdc.system_changes {
					entries.push(DroppedCdcEntry {
						key: sys_change.key().clone(),
						value_bytes: sys_change.value_bytes() as u64,
					});
				}
			}
		}

		for pk in &block_pks_to_delete {
			conn.execute(r#"DELETE FROM "cdc_block" WHERE max_version = ?1"#, params![pk.as_slice()])
				.map_err(|e| CdcError::Internal(format!("drop block delete: {e}")))?;
		}
		conn.execute(r#"DELETE FROM "cdc" WHERE version < ?1"#, params![version_bytes.as_slice()])
			.map_err(|e| CdcError::Internal(format!("drop_before delete: {e}")))?;
		let _ = conn.execute("PRAGMA incremental_vacuum", []);

		Ok(DropBeforeResult {
			count,
			entries,
		})
	}

	fn find_ttl_cutoff(&self, cutoff: DateTime) -> CdcStorageResult<Option<CommitVersion>> {
		let cutoff_nanos = datetime_to_nanos(&cutoff);

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
		if let Some(min_v) = block_hit {
			return Ok(Some(bytes_to_version(&min_v)?));
		}

		let live_min = match self.min_version_live()? {
			Some(v) => v,
			None => {
				return self
					.max_version_blocks()
					.map(|opt| opt.map(|v| CommitVersion(v.0.saturating_add(1))));
			}
		};

		let mut next_start = Bound::Included(live_min);
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
}
