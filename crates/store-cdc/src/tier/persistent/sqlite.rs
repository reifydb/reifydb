// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, AtomicUsize, Ordering},
};

use postcard::{from_bytes, to_stdvec};
use reifydb_codec::cdc;
use reifydb_core::{
	common::CommitVersion, error::diagnostic::internal::internal, event::metric::CdcEviction, interface::cdc::Cdc,
};
use reifydb_runtime::sync::mutex::{Mutex, MutexGuard};
use reifydb_sqlite::{
	SqliteConfig, SqliteTempPathGuard,
	connection::{connect, convert_flags, resolve_db_path},
	pragma,
};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	count::Count,
	error, reifydb_assertions,
	value::{datetime::DateTime, duration::Duration},
};
use rusqlite::{Connection, OptionalExtension, Result as SqliteResult, Row, Transaction, TransactionBehavior, params};
use tracing::instrument;

use crate::{
	storage::{Cutoff, aggregate_evictions, merge_evictions, total_evicted_count},
	tier::persistent::CdcPersistentMetrics,
	types::{Block, BlockId, BlockSummary, DropOutcome},
};

const BLOCK_ZSTD_LEVEL: i32 = 1;

const READ_CONN_CACHE_SIZE: ByteSize = ByteSize::from_mib(2);

const READ_CONN_BUSY_TIMEOUT: Duration = Duration::from_milliseconds_const(5_000);

const WRITE_CONN_BUSY_TIMEOUT: Duration = Duration::from_milliseconds_const(200);

const SUMMARY_COLUMNS: &str = r#"max_version, min_version, min_timestamp, max_timestamp, "count", stored_bytes"#;

type SummaryRow = (Vec<u8>, Vec<u8>, i64, i64, i64, i64);

type BlockRow = (Vec<u8>, Vec<u8>, i64, i64, i64, i64, Vec<u8>);

type DoomedRow = (Vec<u8>, i64, Vec<u8>);

#[derive(Clone)]
pub struct SqliteCdcPersistent {
	inner: Arc<SqliteCdcPersistentInner>,
}

struct SqliteCdcPersistentInner {
	conn: Mutex<Option<Connection>>,
	readers: ReadPool,
	truncated_before: AtomicU64,
	blocks: AtomicU64,
	stored_bytes: AtomicU64,
	appends: AtomicU64,
	loads: AtomicU64,
	drops: AtomicU64,
}

struct ReadPool {
	conns: Vec<Mutex<Option<Connection>>>,
	next: AtomicUsize,
}

impl ReadPool {
	#[instrument(name = "store::cdc::persistent::conn_acquire", level = "debug", skip_all)]
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
}

impl SqliteCdcPersistent {
	#[instrument(name = "store::cdc::persistent::new", level = "debug", skip(config), fields(
		db_path = ?config.path,
		read_pool_size = config.read_pool_size
	))]
	pub fn new(config: SqliteConfig) -> Self {
		let conn = open_connection(&config);
		let truncated_before = read_truncated_before(&conn);
		let (blocks, stored_bytes) = read_block_stats(&conn);
		let pool_size = config.read_pool_size.max(1) as usize;
		let mut conns = Vec::with_capacity(pool_size);
		for _ in 0..pool_size {
			conns.push(Mutex::new(Some(open_read_connection(&config))));
		}
		Self {
			inner: Arc::new(SqliteCdcPersistentInner {
				conn: Mutex::new(Some(conn)),
				readers: ReadPool {
					conns,
					next: AtomicUsize::new(0),
				},
				truncated_before: AtomicU64::new(truncated_before),
				blocks: AtomicU64::new(blocks),
				stored_bytes: AtomicU64::new(stored_bytes),
				appends: AtomicU64::new(0),
				loads: AtomicU64::new(0),
				drops: AtomicU64::new(0),
			}),
		}
	}

	pub fn in_memory() -> (Self, SqliteTempPathGuard) {
		let (config, guard) = SqliteConfig::in_memory();
		(Self::new(config), guard)
	}

	#[instrument(name = "store::cdc::persistent::append_block", level = "debug", skip_all)]
	pub fn append_block(&self, block: &Block) -> Result<()> {
		let Some(first) = block.entries.first() else {
			return Err(error!(internal(
				"an empty cdc block has no version range and would break prefix truncation"
			)));
		};
		let last = block.entries.last().unwrap();
		reifydb_assertions! {
			assert!(
				block.entries.windows(2).all(|w| w[0].version < w[1].version),
				"block entries must be strictly ascending by version"
			);
			assert_eq!(
				block.summary.id.0, last.version,
				"a block is identified by its highest version"
			);
			assert_eq!(
				block.summary.min_version, first.version,
				"summary min_version must be the lowest entry version"
			);
			assert_eq!(
				block.summary.max_version, last.version,
				"summary max_version must be the highest entry version"
			);
			assert_eq!(
				block.summary.count.as_u64(), block.entries.len() as u64,
				"summary count must match the entries the payload carries"
			);
		}

		let (min_timestamp, max_timestamp) = summarize_timestamps(&block.entries);
		let payload = encode_entries(&block.entries)?;
		let rollup = aggregate_evictions(block.entries.iter().flat_map(|entry| entry.changes.iter()));
		let rollup_bytes =
			to_stdvec(&rollup).map_err(|e| error!(internal(format!("cdc rollup encode: {e}"))))?;
		let stored_bytes = payload.len() as u64;

		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Err(error!(internal("cdc persistent tier is closed")));
		};
		conn.prepare_cached(
			r#"INSERT INTO "cdc_block"
			   (max_version, min_version, min_timestamp, max_timestamp, "count", stored_bytes,
			    stats_rollup, payload)
			   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"#,
		)
		.map_err(|e| error!(internal(format!("cdc block insert prepare: {e}"))))?
		.execute(params![
			version_to_bytes(last.version).as_slice(),
			version_to_bytes(first.version).as_slice(),
			min_timestamp,
			max_timestamp,
			block.entries.len() as i64,
			stored_bytes as i64,
			rollup_bytes.as_slice(),
			payload.as_slice(),
		])
		.map_err(|e| error!(internal(format!("cdc block insert: {e}"))))?;

		self.inner.blocks.fetch_add(1, Ordering::Relaxed);
		self.inner.stored_bytes.fetch_add(stored_bytes, Ordering::Relaxed);
		self.inner.appends.fetch_add(1, Ordering::Relaxed);
		Ok(())
	}

	#[instrument(name = "store::cdc::persistent::load_block_containing", level = "trace", skip(self), fields(version = version.0))]
	pub fn load_block_containing(&self, version: CommitVersion) -> Result<Option<Arc<Block>>> {
		self.load_one(
			r#"SELECT max_version, min_version, min_timestamp, max_timestamp, "count", stored_bytes,
			   payload FROM "cdc_block"
			   WHERE max_version >= ?1 AND min_version <= ?1
			   ORDER BY max_version ASC LIMIT 1"#,
			version,
		)
	}

	#[instrument(name = "store::cdc::persistent::load_block", level = "trace", skip(self), fields(block = id.0.0))]
	pub fn load_block(&self, id: BlockId) -> Result<Option<Arc<Block>>> {
		self.load_one(
			r#"SELECT max_version, min_version, min_timestamp, max_timestamp, "count", stored_bytes,
			   payload FROM "cdc_block" WHERE max_version = ?1"#,
			id.0,
		)
	}

	#[instrument(name = "store::cdc::persistent::summaries_from", level = "trace", skip(self), fields(from = from.0, limit = limit))]
	pub fn summaries_from(&self, from: CommitVersion, limit: usize) -> Result<Vec<BlockSummary>> {
		let sql = format!(r#"SELECT {SUMMARY_COLUMNS} FROM "cdc_block"
			   WHERE max_version >= ?1 ORDER BY max_version ASC LIMIT ?2"#);
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Err(error!(internal("cdc persistent tier is closed")));
		};
		let from_bytes = version_to_bytes(from);
		let mut stmt = conn
			.prepare_cached(&sql)
			.map_err(|e| error!(internal(format!("cdc summaries prepare: {e}"))))?;
		let rows = stmt
			.query_map(params![from_bytes.as_slice(), clamp_limit(limit)], read_summary_row)
			.map_err(|e| error!(internal(format!("cdc summaries query: {e}"))))?;
		let mut summaries = Vec::new();
		for row in rows {
			let row = row.map_err(|e| error!(internal(format!("cdc summary row: {e}"))))?;
			summaries.push(summary_from_row(row)?);
		}
		Ok(summaries)
	}

	#[instrument(name = "store::cdc::persistent::drop_blocks_below", level = "debug", skip_all)]
	pub fn drop_blocks_below(&self, cutoff: Cutoff, limit: usize) -> Result<DropOutcome> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Err(error!(internal("cdc persistent tier is closed")));
		};

		let bound = match cutoff {
			Cutoff::Version(version) => Some(version_to_bytes(version)),
			Cutoff::Unbounded => None,
		};
		let scanned = scan_droppable(conn, bound.as_ref(), limit)?;
		let more_remaining = scanned.len() > limit;
		let doomed = &scanned[..scanned.len().min(limit)];
		if doomed.is_empty() {
			return Ok(DropOutcome {
				count: Count::ZERO,
				entries: Vec::new(),
				more_remaining,
			});
		}

		let mut rollups: Vec<CdcEviction> = Vec::new();
		let mut freed = 0u64;
		let mut highest_dropped = 0u64;
		for (max_bytes, stored_bytes, rollup) in doomed {
			rollups.extend(decode_rollup(rollup)?);
			freed = freed.saturating_add(*stored_bytes as u64);
			highest_dropped = highest_dropped.max(bytes_to_version(max_bytes)?.0);
		}
		let floor = highest_dropped.saturating_add(1);
		self.inner.truncated_before.fetch_max(floor, Ordering::Release);
		delete_blocks(conn, doomed, &floor.to_be_bytes())?;

		let blocks = self.inner.blocks.load(Ordering::Relaxed);
		self.inner.blocks.store(blocks.saturating_sub(doomed.len() as u64), Ordering::Relaxed);
		let stored_bytes = self.inner.stored_bytes.load(Ordering::Relaxed);
		self.inner.stored_bytes.store(stored_bytes.saturating_sub(freed), Ordering::Relaxed);
		self.inner.drops.fetch_add(doomed.len() as u64, Ordering::Relaxed);

		let entries = merge_evictions(rollups);
		let count = total_evicted_count(&entries);
		Ok(DropOutcome {
			count,
			entries,
			more_remaining,
		})
	}

	#[instrument(name = "store::cdc::persistent::min_version", level = "trace", skip(self))]
	pub fn min_version(&self) -> Result<Option<CommitVersion>> {
		self.aggregate_version(r#"SELECT MIN(min_version) FROM "cdc_block""#)
	}

	#[instrument(name = "store::cdc::persistent::max_version", level = "trace", skip(self))]
	pub fn max_version(&self) -> Result<Option<CommitVersion>> {
		self.aggregate_version(r#"SELECT MAX(max_version) FROM "cdc_block""#)
	}

	#[instrument(name = "store::cdc::persistent::find_ttl_cutoff", level = "debug", skip(self, cutoff))]
	pub fn find_ttl_cutoff(&self, cutoff: DateTime) -> Result<Option<Cutoff>> {
		let hit: Option<Vec<u8>> = {
			let guard = self.inner.readers.acquire();
			let Some(conn) = guard.as_ref() else {
				return Err(error!(internal("cdc persistent tier is closed")));
			};
			conn.prepare_cached(
				r#"SELECT min_version FROM "cdc_block"
				   WHERE max_timestamp >= ?1 ORDER BY max_version ASC LIMIT 1"#,
			)
			.map_err(|e| error!(internal(format!("cdc ttl cutoff prepare: {e}"))))?
			.query_row(params![datetime_to_nanos(&cutoff)], |row| row.get::<_, Vec<u8>>(0))
			.optional()
			.map_err(|e| error!(internal(format!("cdc ttl cutoff query: {e}"))))?
		};
		if let Some(bytes) = hit {
			return Ok(Some(Cutoff::Version(bytes_to_version(&bytes)?)));
		}
		Ok(self.max_version()?.map(|_| Cutoff::Unbounded))
	}

	#[instrument(name = "store::cdc::persistent::truncated_before", level = "trace", skip(self))]
	pub fn truncated_before(&self) -> CommitVersion {
		CommitVersion(self.inner.truncated_before.load(Ordering::Acquire))
	}

	#[instrument(name = "store::cdc::persistent::metrics", level = "trace", skip(self))]
	pub fn metrics(&self) -> CdcPersistentMetrics {
		CdcPersistentMetrics {
			blocks: self.inner.blocks.load(Ordering::Relaxed),
			stored_bytes: ByteSize::from_bytes(self.inner.stored_bytes.load(Ordering::Relaxed)),
			appends: self.inner.appends.load(Ordering::Relaxed),
			loads: self.inner.loads.load(Ordering::Relaxed),
			drops: self.inner.drops.load(Ordering::Relaxed),
		}
	}

	#[instrument(name = "store::cdc::persistent::shutdown", level = "debug", skip(self))]
	pub fn shutdown(&self) {
		let guard = self.inner.conn.lock();
		if let Some(conn) = guard.as_ref() {
			let _ = pragma::shutdown(conn);
		}
	}

	fn load_one(&self, sql: &str, version: CommitVersion) -> Result<Option<Arc<Block>>> {
		let row: Option<BlockRow> = {
			let guard = self.inner.readers.acquire();
			let Some(conn) = guard.as_ref() else {
				return Err(error!(internal("cdc persistent tier is closed")));
			};
			conn.prepare_cached(sql)
				.map_err(|e| error!(internal(format!("cdc block load prepare: {e}"))))?
				.query_row(params![version_to_bytes(version).as_slice()], |row| {
					Ok((
						row.get::<_, Vec<u8>>(0)?,
						row.get::<_, Vec<u8>>(1)?,
						row.get::<_, i64>(2)?,
						row.get::<_, i64>(3)?,
						row.get::<_, i64>(4)?,
						row.get::<_, i64>(5)?,
						row.get::<_, Vec<u8>>(6)?,
					))
				})
				.optional()
				.map_err(|e| error!(internal(format!("cdc block load: {e}"))))?
		};
		let Some((max_bytes, min_bytes, min_ts, max_ts, count, stored_bytes, payload)) = row else {
			return Ok(None);
		};
		let summary = summary_from_row((max_bytes, min_bytes, min_ts, max_ts, count, stored_bytes))?;
		let entries = cdc::decode::<Vec<Cdc>>(&payload)
			.map_err(|e| error!(internal(format!("cdc block decode: {e}"))))?
			.into_iter()
			.map(Arc::new)
			.collect();
		self.inner.loads.fetch_add(1, Ordering::Relaxed);
		Ok(Some(Arc::new(Block {
			summary,
			entries,
		})))
	}

	fn aggregate_version(&self, sql: &str) -> Result<Option<CommitVersion>> {
		let bytes: Option<Vec<u8>> = {
			let guard = self.inner.readers.acquire();
			let Some(conn) = guard.as_ref() else {
				return Err(error!(internal("cdc persistent tier is closed")));
			};
			conn.prepare_cached(sql)
				.map_err(|e| error!(internal(format!("cdc version bound prepare: {e}"))))?
				.query_row([], |row| row.get::<_, Option<Vec<u8>>>(0))
				.map_err(|e| error!(internal(format!("cdc version bound query: {e}"))))?
		};
		bytes.map(|b| bytes_to_version(&b)).transpose()
	}
}

fn open_connection(config: &SqliteConfig) -> Connection {
	let db_path = resolve_db_path(config.path.clone(), "cdc.db");
	let flags = convert_flags(&config.flags);
	let conn = connect(&db_path, flags).expect("Failed to connect to CDC SQLite database");
	pragma::apply(&conn, config).expect("Failed to configure CDC SQLite pragmas");
	conn.busy_timeout(WRITE_CONN_BUSY_TIMEOUT.to_std()).expect("Failed to set CDC write busy timeout");
	ensure_schema(&conn);
	conn
}

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

fn ensure_schema(conn: &Connection) {
	conn.execute(
		r#"CREATE TABLE IF NOT EXISTS "cdc_block" (
			max_version BLOB PRIMARY KEY,
			min_version BLOB NOT NULL,
			min_timestamp INTEGER NOT NULL,
			max_timestamp INTEGER NOT NULL,
			count INTEGER NOT NULL,
			stored_bytes INTEGER NOT NULL,
			stats_rollup BLOB NOT NULL,
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

fn read_block_stats(conn: &Connection) -> (u64, u64) {
	conn.query_row(r#"SELECT COUNT(*), COALESCE(SUM(stored_bytes), 0) FROM "cdc_block""#, [], |row| {
		Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64))
	})
	.expect("Failed to read cdc_block statistics")
}

fn persist_truncated_before(tx: &Transaction<'_>, version_bytes: &[u8; 8]) -> Result<()> {
	tx.prepare_cached(
		r#"INSERT INTO "cdc_meta" (key, value) VALUES ('truncated_before', ?1)
		   ON CONFLICT(key) DO UPDATE SET value = excluded.value WHERE excluded.value > value"#,
	)
	.map_err(|e| error!(internal(format!("cdc truncated_before prepare: {e}"))))?
	.execute(params![version_bytes.as_slice()])
	.map_err(|e| error!(internal(format!("cdc truncated_before write: {e}"))))?;
	Ok(())
}

#[instrument(name = "store::cdc::persistent::scan_droppable", level = "debug", skip_all, fields(limit = limit))]
fn scan_droppable(conn: &Connection, cutoff_bytes: Option<&[u8; 8]>, limit: usize) -> Result<Vec<DoomedRow>> {
	let want = clamp_limit(limit.saturating_add(1));
	let read = |row: &Row<'_>| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?, row.get::<_, Vec<u8>>(2)?));
	let mut scanned = Vec::new();
	match cutoff_bytes {
		Some(bytes) => {
			let mut stmt = conn
				.prepare_cached(
					r#"SELECT max_version, stored_bytes, stats_rollup FROM "cdc_block"
					   WHERE max_version < ?1 ORDER BY max_version ASC LIMIT ?2"#,
				)
				.map_err(|e| error!(internal(format!("cdc drop scan prepare: {e}"))))?;
			let rows = stmt
				.query_map(params![bytes.as_slice(), want], read)
				.map_err(|e| error!(internal(format!("cdc drop scan query: {e}"))))?;
			for row in rows {
				scanned.push(row.map_err(|e| error!(internal(format!("cdc drop scan row: {e}"))))?);
			}
		}
		None => {
			let mut stmt = conn
				.prepare_cached(
					r#"SELECT max_version, stored_bytes, stats_rollup FROM "cdc_block"
					   ORDER BY max_version ASC LIMIT ?1"#,
				)
				.map_err(|e| error!(internal(format!("cdc drop scan prepare: {e}"))))?;
			let rows = stmt
				.query_map(params![want], read)
				.map_err(|e| error!(internal(format!("cdc drop scan query: {e}"))))?;
			for row in rows {
				scanned.push(row.map_err(|e| error!(internal(format!("cdc drop scan row: {e}"))))?);
			}
		}
	}
	Ok(scanned)
}

#[instrument(name = "store::cdc::persistent::delete_blocks", level = "debug", skip_all, fields(block_count = doomed.len()))]
fn delete_blocks(conn: &Connection, doomed: &[DoomedRow], floor_bytes: &[u8; 8]) -> Result<()> {
	let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
		.map_err(|e| error!(internal(format!("cdc drop tx begin: {e}"))))?;
	{
		let mut stmt = tx
			.prepare_cached(r#"DELETE FROM "cdc_block" WHERE max_version = ?1"#)
			.map_err(|e| error!(internal(format!("cdc drop delete prepare: {e}"))))?;
		for (max_bytes, _, _) in doomed {
			stmt.execute(params![max_bytes.as_slice()])
				.map_err(|e| error!(internal(format!("cdc drop delete: {e}"))))?;
		}
	}
	persist_truncated_before(&tx, floor_bytes)?;
	tx.commit().map_err(|e| error!(internal(format!("cdc drop commit: {e}"))))?;
	Ok(())
}

fn read_summary_row(row: &Row<'_>) -> SqliteResult<SummaryRow> {
	Ok((
		row.get::<_, Vec<u8>>(0)?,
		row.get::<_, Vec<u8>>(1)?,
		row.get::<_, i64>(2)?,
		row.get::<_, i64>(3)?,
		row.get::<_, i64>(4)?,
		row.get::<_, i64>(5)?,
	))
}

fn summary_from_row(row: SummaryRow) -> Result<BlockSummary> {
	let (max_bytes, min_bytes, min_timestamp, max_timestamp, count, stored_bytes) = row;
	let max_version = bytes_to_version(&max_bytes)?;
	Ok(BlockSummary {
		id: BlockId(max_version),
		min_version: bytes_to_version(&min_bytes)?,
		max_version,
		min_timestamp: DateTime::from_nanos(min_timestamp as u64),
		max_timestamp: DateTime::from_nanos(max_timestamp as u64),
		count: Count::new(count as u64),
		stored_bytes: ByteSize::from_bytes(stored_bytes as u64),
	})
}

#[instrument(name = "store::cdc::persistent::encode_entries", level = "debug", skip_all, fields(entry_count = entries.len()))]
fn encode_entries(entries: &[Arc<Cdc>]) -> Result<Vec<u8>> {
	let borrowed: Vec<&Cdc> = entries.iter().map(|entry| entry.as_ref()).collect();
	cdc::encode(&borrowed, BLOCK_ZSTD_LEVEL).map_err(|e| error!(internal(format!("cdc block encode: {e}"))))
}

fn decode_rollup(bytes: &[u8]) -> Result<Vec<CdcEviction>> {
	from_bytes(bytes).map_err(|e| error!(internal(format!("cdc rollup decode: {e}"))))
}

fn summarize_timestamps(entries: &[Arc<Cdc>]) -> (i64, i64) {
	entries.iter().fold((i64::MAX, i64::MIN), |(lo, hi), entry| {
		let nanos = datetime_to_nanos(&entry.timestamp);
		(lo.min(nanos), hi.max(nanos))
	})
}

fn datetime_to_nanos(value: &DateTime) -> i64 {
	value.to_nanos() as i64
}

fn version_to_bytes(version: CommitVersion) -> [u8; 8] {
	version.0.to_be_bytes()
}

fn bytes_to_version(bytes: &[u8]) -> Result<CommitVersion> {
	let raw: [u8; 8] = bytes.try_into().map_err(|_| error!(internal("cdc block version is not 8 bytes")))?;
	Ok(CommitVersion(u64::from_be_bytes(raw)))
}

fn clamp_limit(limit: usize) -> i64 {
	limit.min(i64::MAX as usize) as i64
}
