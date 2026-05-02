// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Bound, sync::Arc};

use reifydb_core::{common::CommitVersion, error::diagnostic::internal::internal, interface::store::EntryKind};
use reifydb_runtime::sync::mutex::Mutex;
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
		build_create_warm_current_sql, build_get_warm_current_sql, build_range_warm_current_sql,
		build_upsert_warm_current_sql, version_from_bytes, version_to_bytes,
	},
};
use crate::tier::{HistoricalCursor, RangeBatch, RangeCursor, RawEntry, TierBackend, TierBatch, TierStorage};

/// SQLite-backed warm tier storage.
///
/// Phase 1 of the tiered architecture: stores only the latest committed value
/// per key in a per-`EntryKind` `__warm_current` table. There is no historical
/// version chain in warm; aged history dies on the floor of hot via the
/// existing watermark GC and never reaches warm.
///
/// Read semantics: a request at version `W` is served if and only if the
/// stored row's version `V <= W`. A row whose `V > W` is "from the future"
/// relative to the snapshot and the read returns `None` (caller continues to
/// lower tiers).
#[derive(Clone)]
pub struct SqliteWarmStorage {
	inner: Arc<SqliteWarmStorageInner>,
}

struct SqliteWarmStorageInner {
	/// Single connection protected by Mutex for thread-safe access.
	/// rusqlite::Connection is Send but not Sync, so a mutex is required.
	conn: Mutex<Connection>,
}

impl SqliteWarmStorage {
	#[instrument(name = "store::multi::warm::sqlite::new", level = "debug", skip(config), fields(
		db_path = ?config.path,
		page_size = config.page_size,
		journal_mode = %config.journal_mode.as_str()
	))]
	pub fn new(config: SqliteConfig) -> Self {
		let db_path = resolve_db_path(config.path.clone(), "warm.db");
		let flags = convert_flags(&config.flags);

		let conn = connect(&db_path, flags).expect("Failed to connect to warm database");
		pragma::apply(&conn, &config).expect("Failed to configure warm SQLite pragmas");

		Self {
			inner: Arc::new(SqliteWarmStorageInner {
				conn: Mutex::new(conn),
			}),
		}
	}

	pub fn in_memory() -> Self {
		Self::new(SqliteConfig::in_memory())
	}

	pub fn count_current(&self, table: EntryKind) -> Result<u64> {
		let table_name = warm_current_table_name(table);
		let conn = self.inner.conn.lock();
		let sql = format!("SELECT COUNT(*) FROM \"{}\"", table_name);
		match conn.query_row(&sql, [], |row| row.get::<_, i64>(0)) {
			Ok(c) => Ok(c as u64),
			Err(e) if e.to_string().contains("no such table") => Ok(0),
			Err(e) => Err(error!(internal(format!("Failed to count warm current: {}", e)))),
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

		let table_name = warm_current_table_name(req.table);
		let conn = self.inner.conn.lock();

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
			Err(e) => return Err(error!(internal(format!("Failed to prepare warm range: {}", e)))),
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
				key: CowVec::new(key),
				version: version_from_bytes(&version_blob),
				value: value.map(CowVec::new),
			})
		}) {
			Ok(rows) => rows
				.collect::<SqliteResult<Vec<_>>>()
				.map_err(|e| error!(internal(format!("Failed to read warm row: {}", e))))?,
			Err(e) if e.to_string().contains("no such table") => {
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
			Err(e) => return Err(error!(internal(format!("Failed to scan warm range: {}", e)))),
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

impl TierStorage for SqliteWarmStorage {
	#[instrument(name = "store::multi::warm::sqlite::get", level = "trace", skip(self), fields(table = ?table, key_len = key.len(), version = version.0))]
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<Option<CowVec<u8>>> {
		let table_name = warm_current_table_name(table);
		let conn = self.inner.conn.lock();
		let sql = build_get_warm_current_sql(&table_name);

		let result = match conn.prepare_cached(&sql) {
			Ok(mut stmt) => stmt.query_row(params![key], |row| {
				let version_bytes: Vec<u8> = row.get(0)?;
				let value: Option<Vec<u8>> = row.get(1)?;
				Ok((version_from_bytes(&version_bytes), value))
			}),
			Err(e) if e.to_string().contains("no such table") => Err(QueryReturnedNoRows),
			Err(e) => return Err(error!(internal(format!("Failed to prepare warm get: {}", e)))),
		};

		match result {
			// Snapshot read: only return the row if it was committed at or before
			// the requested version. A row whose version > requested is "from the
			// future" relative to this snapshot and warm cannot answer.
			Ok((stored_version, value)) if stored_version <= version => Ok(value.map(CowVec::new)),
			Ok(_) => Ok(None),
			Err(QueryReturnedNoRows) => Ok(None),
			Err(e) if e.to_string().contains("no such table") => Ok(None),
			Err(e) => Err(error!(internal(format!("Failed to read warm: {}", e)))),
		}
	}

	#[instrument(name = "store::multi::warm::sqlite::set", level = "debug", skip(self, batches), fields(table_count = batches.len(), version = version.0))]
	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<()> {
		if batches.is_empty() {
			return Ok(());
		}

		let conn = self.inner.conn.lock();
		let tx = conn
			.unchecked_transaction()
			.map_err(|e| error!(internal(format!("Failed to start warm transaction: {}", e))))?;

		let new_version_bytes = version_to_bytes(version);

		for (table, entries) in batches {
			let table_name = warm_current_table_name(table);
			Self::create_table_if_needed(&tx, &table_name)
				.map_err(|e| error!(internal(format!("Failed to ensure warm table: {}", e))))?;

			let upsert_sql = build_upsert_warm_current_sql(&table_name);
			let mut stmt = tx
				.prepare_cached(&upsert_sql)
				.map_err(|e| error!(internal(format!("Failed to prepare warm upsert: {}", e))))?;

			for (key, value) in entries {
				let key_slice = key.as_slice();
				let value_slice = value.as_ref().map(|v| v.as_slice());
				stmt.execute(params![key_slice, new_version_bytes.as_slice(), value_slice])
					.map_err(|e| error!(internal(format!("Failed to upsert warm row: {}", e))))?;
			}
		}

		tx.commit().map_err(|e| error!(internal(format!("Failed to commit warm transaction: {}", e))))
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
			.map_err(|e| error!(internal(format!("Failed to ensure warm table: {}", e))))
	}

	fn clear_table(&self, table: EntryKind) -> Result<()> {
		let table_name = warm_current_table_name(table);
		let conn = self.inner.conn.lock();
		let result = conn.execute(&format!("DELETE FROM \"{}\"", table_name), []);
		if let Err(e) = result
			&& !e.to_string().contains("no such table")
		{
			return Err(error!(internal(format!("Failed to clear warm {}: {}", table_name, e))));
		}
		Ok(())
	}

	fn drop(&self, _batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>>) -> Result<()> {
		// TODO: change the TierStorage interface so warm doesn't have to expose
		// drop at all. Warm has no historical chain to physically erase per
		// (key, version) and receives no drop-actor traffic.
		panic!("SqliteWarmStorage::drop: warm tier has no historical chain to drop versions from");
	}

	fn get_all_versions(&self, table: EntryKind, key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		// Warm holds at most one version per key (the latest committed value).
		// Returning [] for "no row" or [(v, value_opt)] for "the one row" is
		// what the cascade's tombstone disambiguation expects in
		// `store/version.rs::get_at_version`. This is NOT historical data;
		// it is the single current row.
		// TODO: change the TierStorage interface to remove the
		// historical-chain methods entirely; this current-only access should
		// live on a smaller warm-specific trait.
		let table_name = warm_current_table_name(table);
		let conn = self.inner.conn.lock();
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
					"Failed to prepare warm get_all_versions: {}",
					e
				))));
			}
		};

		match result {
			Ok(row) => Ok(vec![row]),
			Err(QueryReturnedNoRows) => Ok(Vec::new()),
			Err(e) if e.to_string().contains("no such table") => Ok(Vec::new()),
			Err(e) => Err(error!(internal(format!("Failed to read warm versions: {}", e)))),
		}
	}

	fn scan_historical_below(
		&self,
		_table: EntryKind,
		_cutoff: CommitVersion,
		_cursor: &mut HistoricalCursor,
		_batch_size: usize,
	) -> Result<Vec<(CowVec<u8>, CommitVersion)>> {
		// TODO: change the TierStorage interface so warm doesn't have to expose
		// historical-chain methods at all. Warm is current-only by design;
		// callers must not invoke this on warm.
		panic!("SqliteWarmStorage::scan_historical_below: warm tier has no historical chain");
	}
}

impl TierBackend for SqliteWarmStorage {}
