// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	ops::Bound,
	sync::Arc,
};

use reifydb_core::{common::CommitVersion, error::diagnostic::internal::internal, interface::store::EntryKind};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sqlite::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	pragma,
};
use reifydb_type::{Result, error, util::cowvec::CowVec};
use rusqlite::{
	CachedStatement, Connection, Error::QueryReturnedNoRows, Result as SqliteResult, Row, ToSql,
	Transaction as SqliteTransaction, params,
};
use tracing::{field, instrument, trace_span};

use super::{
	entry::{current_table_name, historical_table_name},
	query::{
		build_create_current_sql, build_create_historical_sql, build_get_all_versions_sql,
		build_get_current_sql, build_get_historical_sql, build_range_current_query, version_from_bytes,
		version_to_bytes,
	},
};
use crate::tier::{RangeBatch, RangeCursor, RawEntry, TierBackend, TierBatch, TierStorage};

/// SQLite-based primitive storage with split current/historical layout.
///
/// Per logical table we maintain two physical tables:
/// - `<name>__current` holds at most one row per logical key: the latest visible version. This is the hot read path -
///   point gets and range scans of "the current state" hit this table only.
/// - `<name>__historical` holds older versions superseded by current, plus any out-of-order writes (a write whose
///   version is below the existing current). Cold path; only consulted for snapshot reads at versions older than
///   current's version.
#[derive(Clone)]
pub struct SqlitePrimitiveStorage {
	inner: Arc<SqlitePrimitiveStorageInner>,
}

struct SqlitePrimitiveStorageInner {
	/// Single connection protected by Mutex for thread-safe access.
	/// rusqlite::Connection is Send but not Sync, so a mutex is required.
	/// (M2 will replace this with per-worker readers + a writer mutex.)
	conn: Mutex<Connection>,
}

impl SqlitePrimitiveStorage {
	#[instrument(name = "store::multi::sqlite::new", level = "debug", skip(config), fields(
		db_path = ?config.path,
		page_size = config.page_size,
		journal_mode = %config.journal_mode.as_str()
	))]
	pub fn new(config: SqliteConfig) -> Self {
		let db_path = resolve_db_path(config.path.clone(), "primitive.db");
		let flags = convert_flags(&config.flags);

		let conn = connect(&db_path, flags).expect("Failed to connect to database");
		pragma::apply(&conn, &config).expect("Failed to configure SQLite pragmas");

		Self {
			inner: Arc::new(SqlitePrimitiveStorageInner {
				conn: Mutex::new(conn),
			}),
		}
	}

	pub fn in_memory() -> Self {
		Self::new(SqliteConfig::in_memory())
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

	pub fn count_current(&self, table: EntryKind) -> Result<u64> {
		let current_name = current_table_name(table);
		let conn = self.inner.conn.lock();
		let sql = format!("SELECT COUNT(*) FROM \"{}\"", current_name);
		match conn.query_row(&sql, [], |row| row.get::<_, i64>(0)) {
			Ok(c) => Ok(c as u64),
			Err(e) if e.to_string().contains("no such table") => Ok(0),
			Err(e) => Err(error!(internal(format!("Failed to count current: {}", e)))),
		}
	}

	pub fn count_historical(&self, table: EntryKind) -> Result<u64> {
		let historical_name = historical_table_name(table);
		let conn = self.inner.conn.lock();
		let sql = format!("SELECT COUNT(*) FROM \"{}\"", historical_name);
		match conn.query_row(&sql, [], |row| row.get::<_, i64>(0)) {
			Ok(c) => Ok(c as u64),
			Err(e) if e.to_string().contains("no such table") => Ok(0),
			Err(e) => Err(error!(internal(format!("Failed to count historical: {}", e)))),
		}
	}

	/// Create both physical tables (current + historical) for a logical table.
	fn create_tables_if_needed(conn: &Connection, current_name: &str, historical_name: &str) -> SqliteResult<()> {
		conn.execute(&build_create_current_sql(current_name), [])?;
		conn.execute(&build_create_historical_sql(historical_name), [])?;
		Ok(())
	}
}

/// Decode a `(version, value)` row that may come from either physical table.
fn decode_versioned_value_row(row: &Row<'_>) -> SqliteResult<(CommitVersion, Option<Vec<u8>>)> {
	let version_bytes: Vec<u8> = row.get(0)?;
	let value: Option<Vec<u8>> = row.get(1)?;
	Ok((version_from_bytes(&version_bytes), value))
}

impl TierStorage for SqlitePrimitiveStorage {
	#[instrument(name = "store::multi::sqlite::get", level = "trace", skip(self), fields(table = ?table, key_len = key.len(), version = version.0))]
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<Option<CowVec<u8>>> {
		let current_name = current_table_name(table);
		let historical_name = historical_table_name(table);
		let conn = self.inner.conn.lock();

		// Probe __current first. If the row exists and its version is <=
		// snapshot, that's the answer (value or tombstone). Otherwise
		// (no row, or current.version > snapshot) consult historical.
		let current_sql = build_get_current_sql(&current_name);
		let current_result = match conn.prepare_cached(&current_sql) {
			Ok(mut stmt) => stmt.query_row(params![key], decode_versioned_value_row),
			Err(e) if e.to_string().contains("no such table") => Err(QueryReturnedNoRows),
			Err(e) => return Err(error!(internal(format!("Failed to prepare get_current: {}", e)))),
		};

		match current_result {
			Ok((cur_version, value)) if cur_version <= version => {
				return Ok(value.map(CowVec::new));
			}
			Ok(_) => {}                    // current.version > snapshot, fall through
			Err(QueryReturnedNoRows) => {} // not in current, fall through
			Err(e) => return Err(error!(internal(format!("Failed to read current: {}", e)))),
		}

		// Historical fallback.
		let historical_sql = build_get_historical_sql(&historical_name);
		let historical_result = match conn.prepare_cached(&historical_sql) {
			Ok(mut stmt) => stmt.query_row(
				params![key, version_to_bytes(version).as_slice()],
				decode_versioned_value_row,
			),
			Err(e) if e.to_string().contains("no such table") => return Ok(None),
			Err(e) => return Err(error!(internal(format!("Failed to prepare get_historical: {}", e)))),
		};

		match historical_result {
			Ok((_, value)) => Ok(value.map(CowVec::new)),
			Err(QueryReturnedNoRows) => Ok(None),
			Err(e) if e.to_string().contains("no such table") => Ok(None),
			Err(e) => Err(error!(internal(format!("Failed to read historical: {}", e)))),
		}
	}

	#[instrument(name = "store::multi::sqlite::contains", level = "trace", skip(self), fields(table = ?table, key_len = key.len(), version = version.0), ret)]
	fn contains(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<bool> {
		Ok(TierStorage::get(self, table, key, version)?.is_some())
	}

	#[instrument(name = "store::multi::sqlite::set", level = "debug", skip(self, batches), fields(table_count = batches.len(), version = version.0))]
	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<()> {
		if batches.is_empty() {
			return Ok(());
		}

		let conn = self.inner.conn.lock();
		let tx = conn
			.unchecked_transaction()
			.map_err(|e| error!(internal(format!("Failed to start transaction: {}", e))))?;

		for (table, entries) in batches {
			let current_name = current_table_name(table);
			let historical_name = historical_table_name(table);

			// Make sure both tables exist before writing. Cheap when they
			// already do (idempotent CREATE TABLE IF NOT EXISTS).
			Self::create_tables_if_needed(&tx, &current_name, &historical_name)
				.map_err(|e| error!(internal(format!("Failed to ensure tables: {}", e))))?;

			apply_set_to_split_tables(&tx, &current_name, &historical_name, version, &entries)
				.map_err(|e| error!(internal(format!("Failed to apply set: {}", e))))?;
		}

		tx.commit().map_err(|e| error!(internal(format!("Failed to commit transaction: {}", e))))
	}

	#[instrument(name = "store::multi::sqlite::range_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size, version = version.0))]
	fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch> {
		self.range_next_directional(table, cursor, start, end, version, batch_size, false)
	}

	#[instrument(name = "store::multi::sqlite::range_rev_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size, version = version.0))]
	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch> {
		self.range_next_directional(table, cursor, start, end, version, batch_size, true)
	}

	fn ensure_table(&self, table: EntryKind) -> Result<()> {
		let current_name = current_table_name(table);
		let historical_name = historical_table_name(table);
		let conn = self.inner.conn.lock();

		Self::create_tables_if_needed(&conn, &current_name, &historical_name)
			.map_err(|e| error!(internal(format!("Failed to ensure tables: {}", e))))
	}

	fn clear_table(&self, table: EntryKind) -> Result<()> {
		let current_name = current_table_name(table);
		let historical_name = historical_table_name(table);
		let conn = self.inner.conn.lock();

		for name in [&current_name, &historical_name] {
			let result = conn.execute(&format!("DELETE FROM \"{}\"", name), []);
			if let Err(e) = result
				&& !e.to_string().contains("no such table")
			{
				return Err(error!(internal(format!("Failed to clear {}: {}", name, e))));
			}
		}
		Ok(())
	}

	#[instrument(name = "store::multi::sqlite::drop", level = "debug", skip(self, batches), fields(table_count = batches.len()))]
	fn drop(&self, batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>>) -> Result<()> {
		if batches.is_empty() {
			return Ok(());
		}

		let conn = self.inner.conn.lock();
		let tx = conn
			.unchecked_transaction()
			.map_err(|e| error!(internal(format!("Failed to start transaction: {}", e))))?;

		for (table, entries) in batches {
			let current_name = current_table_name(table);
			let historical_name = historical_table_name(table);

			drop_versions_from_split(&tx, &current_name, &historical_name, &entries)
				.map_err(|e| error!(internal(format!("Failed to drop entries: {}", e))))?;
		}

		tx.commit().map_err(|e| error!(internal(format!("Failed to commit drop transaction: {}", e))))
	}

	#[instrument(name = "store::multi::sqlite::get_all_versions", level = "trace", skip(self), fields(table = ?table, key_len = key.len()))]
	fn get_all_versions(&self, table: EntryKind, key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		let current_name = current_table_name(table);
		let historical_name = historical_table_name(table);
		let conn = self.inner.conn.lock();

		let sql = build_get_all_versions_sql(&current_name, &historical_name);
		let mut stmt = match conn.prepare_cached(&sql) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
			Err(e) => return Err(error!(internal(format!("Failed to prepare query: {}", e)))),
		};

		let versions: Vec<(CommitVersion, Option<CowVec<u8>>)> = match stmt.query_map(params![key], |row| {
			let (version, value) = decode_versioned_value_row(row)?;
			Ok((version, value.map(CowVec::new)))
		}) {
			Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
			Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
			Err(e) => return Err(error!(internal(format!("Failed to query versions: {}", e)))),
		};

		Ok(versions)
	}
}

impl SqlitePrimitiveStorage {
	#[allow(clippy::too_many_arguments)]
	fn range_next_directional(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
		reverse: bool,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let current_name = current_table_name(table);
		let historical_name = historical_table_name(table);

		// Collapse the cursor's last_key into the appropriate bound.
		// Forward scan: cursor moves the start bound forward (Excluded
		// last_key). Reverse scan: cursor moves the end bound backward.
		let (effective_start, effective_end) = match (reverse, &cursor.last_key) {
			(false, Some(last)) => (Bound::Excluded(last.as_slice().to_vec()), bound_to_owned(end)),
			(false, None) => (bound_to_owned(start), bound_to_owned(end)),
			(true, Some(last)) => (bound_to_owned(start), Bound::Excluded(last.as_slice().to_vec())),
			(true, None) => (bound_to_owned(start), bound_to_owned(end)),
		};

		let conn = {
			let _lock_span = trace_span!("sqlite::range_next::lock").entered();
			self.inner.conn.lock()
		};

		let start_ref = bound_as_ref(&effective_start);
		let end_ref = bound_as_ref(&effective_end);
		let (query, params) =
			build_range_current_query(&current_name, start_ref, end_ref, reverse, batch_size + 1);

		let mut stmt = {
			let _prepare_span = trace_span!("sqlite::range_next::prepare").entered();
			match conn.prepare_cached(&query) {
				Ok(stmt) => stmt,
				Err(e) if e.to_string().contains("no such table") => {
					cursor.exhausted = true;
					return Ok(RangeBatch::empty());
				}
				Err(e) => {
					return Err(error!(internal(format!("Failed to prepare range query: {}", e))));
				}
			}
		};

		let params_refs: Vec<&dyn ToSql> = params.iter().map(|p| p as &dyn ToSql).collect();

		let raw_rows: Vec<(CowVec<u8>, CommitVersion, Option<CowVec<u8>>)> = {
			let query_span = trace_span!("sqlite::range_next::query_map", returned = field::Empty);
			let _entered = query_span.enter();
			let collected: Vec<_> = stmt
				.query_map(params_refs.as_slice(), |row| {
					let key: Vec<u8> = row.get(0)?;
					let version_bytes: Vec<u8> = row.get(1)?;
					let value: Option<Vec<u8>> = row.get(2)?;
					Ok((
						CowVec::new(key),
						version_from_bytes(&version_bytes),
						value.map(CowVec::new),
					))
				})
				.map_err(|e| error!(internal(format!("Failed to query range: {}", e))))?
				.filter_map(|r| r.ok())
				.collect();
			query_span.record("returned", collected.len());
			collected
		};

		// For each row whose current.version > snapshot, fall back to
		// historical. Most rows in the steady state pass the snapshot
		// check, so this loop is usually pure pass-through.
		let mut entries: Vec<RawEntry> = Vec::with_capacity(raw_rows.len());
		let mut historical_stmt: Option<CachedStatement<'_>> = None;
		let historical_sql = build_get_historical_sql(&historical_name);

		for (key, cur_version, cur_value) in raw_rows {
			if cur_version <= version {
				entries.push(RawEntry {
					key,
					version: cur_version,
					value: cur_value,
				});
				continue;
			}

			// Snapshot is older than this key's current. Look up
			// historical for the largest version <= snapshot.
			if historical_stmt.is_none() {
				historical_stmt = match conn.prepare_cached(&historical_sql) {
					Ok(s) => Some(s),
					Err(e) if e.to_string().contains("no such table") => continue,
					Err(e) => {
						return Err(error!(internal(format!(
							"Failed to prepare historical fallback: {}",
							e
						))));
					}
				};
			}

			let stmt = historical_stmt.as_mut().unwrap();
			let result = stmt.query_row(
				params![key.as_slice(), version_to_bytes(version).as_slice()],
				decode_versioned_value_row,
			);
			match result {
				Ok((hv, hvalue)) => entries.push(RawEntry {
					key,
					version: hv,
					value: hvalue.map(CowVec::new),
				}),
				Err(QueryReturnedNoRows) => {
					// No visible historical version - skip the key.
					// (Caller treats absence as "not present at this snapshot".)
				}
				Err(e) if e.to_string().contains("no such table") => {}
				Err(e) => {
					return Err(error!(internal(format!(
						"Failed historical fallback for range row: {}",
						e
					))));
				}
			}
		}
		drop(historical_stmt);

		let has_more = entries.len() > batch_size;
		if has_more {
			entries.truncate(batch_size);
		}

		let batch = RangeBatch {
			entries,
			has_more,
		};

		if let Some(last_entry) = batch.entries.last() {
			cursor.last_key = Some(last_entry.key.clone());
		}
		if !batch.has_more {
			cursor.exhausted = true;
		}

		Ok(batch)
	}
}

impl TierBackend for SqlitePrimitiveStorage {}

/// Apply a set batch to the split-table layout, mirroring the memory tier's
/// `process_table` logic. Caller is responsible for table creation.
fn apply_set_to_split_tables(
	tx: &SqliteTransaction,
	current_name: &str,
	historical_name: &str,
	version: CommitVersion,
	entries: &[(CowVec<u8>, Option<CowVec<u8>>)],
) -> SqliteResult<()> {
	let new_version_bytes = version_to_bytes(version);

	let select_current_sql = format!("SELECT version, value FROM \"{}\" WHERE key = ?1", current_name);
	let upsert_current_sql = format!(
		"INSERT INTO \"{}\" (key, version, value) VALUES (?1, ?2, ?3) \
		 ON CONFLICT(key) DO UPDATE SET version = excluded.version, value = excluded.value",
		current_name
	);
	let insert_historical_sql =
		format!("INSERT OR REPLACE INTO \"{}\" (key, version, value) VALUES (?1, ?2, ?3)", historical_name);

	let mut select_current = tx.prepare_cached(&select_current_sql)?;
	let mut upsert_current = tx.prepare_cached(&upsert_current_sql)?;
	let mut insert_historical = tx.prepare_cached(&insert_historical_sql)?;

	for (key, value) in entries {
		let key_slice = key.as_slice();
		let value_slice = value.as_ref().map(|v| v.as_slice());

		let prior: Option<(CommitVersion, Option<Vec<u8>>)> =
			match select_current.query_row(params![key_slice], decode_versioned_value_row) {
				Ok(row) => Some(row),
				Err(QueryReturnedNoRows) => None,
				Err(e) => return Err(e),
			};

		match prior {
			None => {
				// First write for this key: install in current.
				upsert_current.execute(params![
					key_slice,
					new_version_bytes.as_slice(),
					value_slice
				])?;
			}
			Some((prior_version, _)) if prior_version < version => {
				// New write supersedes the prior current. Demote
				// the prior to historical, then install the new
				// row as current. Reading the prior value back
				// from `prior` keeps us off a second SQL round trip.
				let (_, prior_value) = prior.as_ref().unwrap();
				let prior_version_bytes = version_to_bytes(prior_version);
				insert_historical.execute(params![
					key_slice,
					prior_version_bytes.as_slice(),
					prior_value.as_deref()
				])?;
				upsert_current.execute(params![
					key_slice,
					new_version_bytes.as_slice(),
					value_slice
				])?;
			}
			Some((prior_version, _)) if prior_version > version => {
				// Out-of-order write: the prior current is from a
				// later commit. Park the new row in historical and
				// leave current alone.
				insert_historical.execute(params![
					key_slice,
					new_version_bytes.as_slice(),
					value_slice
				])?;
			}
			Some(_) => {
				// prior_version == version. Idempotent overwrite of
				// current's value. (Matches the legacy
				// INSERT OR REPLACE semantics.)
				upsert_current.execute(params![
					key_slice,
					new_version_bytes.as_slice(),
					value_slice
				])?;
			}
		}
	}

	Ok(())
}

/// Apply a drop batch (physical removal of specific (key, version) pairs)
/// to the split layout. When every stored version of a key is in the drop
/// batch (the row TTL case) we take a fast path that deletes from both
/// physical tables in a single round trip per key, skipping the cascading
/// historical->current promotions the per-version path would otherwise do.
/// Otherwise we fall back to the per-version logic: dropping the current
/// promotes the next-newest historical row; dropping a historical version
/// just deletes it.
fn drop_versions_from_split(
	tx: &SqliteTransaction,
	current_name: &str,
	historical_name: &str,
	entries: &[(CowVec<u8>, CommitVersion)],
) -> SqliteResult<()> {
	let select_current_sql = format!("SELECT version FROM \"{}\" WHERE key = ?1", current_name);
	let select_all_historical_versions_sql = format!("SELECT version FROM \"{}\" WHERE key = ?1", historical_name);
	let delete_current_sql = format!("DELETE FROM \"{}\" WHERE key = ?1", current_name);
	let upsert_current_sql = format!(
		"INSERT INTO \"{}\" (key, version, value) VALUES (?1, ?2, ?3) \
		 ON CONFLICT(key) DO UPDATE SET version = excluded.version, value = excluded.value",
		current_name
	);
	let pop_historical_sql = format!(
		"SELECT version, value FROM \"{}\" WHERE key = ?1 ORDER BY version DESC LIMIT 1",
		historical_name
	);
	let delete_historical_one_sql = format!("DELETE FROM \"{}\" WHERE key = ?1 AND version = ?2", historical_name);
	let delete_historical_all_sql = format!("DELETE FROM \"{}\" WHERE key = ?1", historical_name);

	let mut select_current = match tx.prepare_cached(&select_current_sql) {
		Ok(s) => s,
		Err(e) if e.to_string().contains("no such table") => return Ok(()),
		Err(e) => return Err(e),
	};
	let mut select_all_historical_versions = tx.prepare_cached(&select_all_historical_versions_sql)?;
	let mut delete_current = tx.prepare_cached(&delete_current_sql)?;
	let mut upsert_current = tx.prepare_cached(&upsert_current_sql)?;
	let mut pop_historical = tx.prepare_cached(&pop_historical_sql)?;
	let mut delete_historical_one = tx.prepare_cached(&delete_historical_one_sql)?;
	let mut delete_historical_all = tx.prepare_cached(&delete_historical_all_sql)?;

	let mut by_key: HashMap<&[u8], Vec<CommitVersion>> = HashMap::new();
	for (key, version) in entries {
		by_key.entry(key.as_slice()).or_default().push(*version);
	}

	for (key_slice, dropped_versions) in by_key {
		let dropped_set: HashSet<CommitVersion> = dropped_versions.iter().copied().collect();

		let cur_version: Option<CommitVersion> = match select_current.query_row(params![key_slice], |row| {
			let bytes: Vec<u8> = row.get(0)?;
			Ok(version_from_bytes(&bytes))
		}) {
			Ok(v) => Some(v),
			Err(QueryReturnedNoRows) => None,
			Err(e) if e.to_string().contains("no such table") => None,
			Err(e) => return Err(e),
		};

		let stored_hist_versions: Vec<CommitVersion> =
			match select_all_historical_versions.query_map(params![key_slice], |row| {
				let bytes: Vec<u8> = row.get(0)?;
				Ok(version_from_bytes(&bytes))
			}) {
				Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
				Err(e) if e.to_string().contains("no such table") => Vec::new(),
				Err(e) => return Err(e),
			};

		let cur_covered = cur_version.is_none_or(|v| dropped_set.contains(&v));
		let hist_covered = stored_hist_versions.iter().all(|v| dropped_set.contains(v));

		if cur_covered && hist_covered {
			delete_current.execute(params![key_slice])?;
			let _ = delete_historical_all.execute(params![key_slice]);
			continue;
		}

		for version in dropped_versions {
			let cur_now: Option<CommitVersion> = match select_current.query_row(params![key_slice], |row| {
				let bytes: Vec<u8> = row.get(0)?;
				Ok(version_from_bytes(&bytes))
			}) {
				Ok(v) => Some(v),
				Err(QueryReturnedNoRows) => None,
				Err(e) if e.to_string().contains("no such table") => None,
				Err(e) => return Err(e),
			};

			if cur_now == Some(version) {
				let promoted: Option<(CommitVersion, Option<Vec<u8>>)> = match pop_historical
					.query_row(params![key_slice], decode_versioned_value_row)
				{
					Ok(row) => Some(row),
					Err(QueryReturnedNoRows) => None,
					Err(e) if e.to_string().contains("no such table") => None,
					Err(e) => return Err(e),
				};

				match promoted {
					Some((promoted_version, promoted_value)) => {
						let promoted_version_bytes = version_to_bytes(promoted_version);
						upsert_current.execute(params![
							key_slice,
							promoted_version_bytes.as_slice(),
							promoted_value.as_deref()
						])?;
						delete_historical_one.execute(params![
							key_slice,
							promoted_version_bytes.as_slice()
						])?;
					}
					None => {
						delete_current.execute(params![key_slice])?;
						let _ = delete_historical_all.execute(params![key_slice]);
					}
				}
			} else {
				let version_bytes = version_to_bytes(version);
				let result =
					delete_historical_one.execute(params![key_slice, version_bytes.as_slice()]);
				if let Err(e) = result
					&& !e.to_string().contains("no such table")
				{
					return Err(e);
				}
			}
		}
	}

	Ok(())
}

fn bound_as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
	match bound {
		Bound::Included(v) => Bound::Included(v.as_slice()),
		Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

fn bound_to_owned(bound: Bound<&[u8]>) -> Bound<Vec<u8>> {
	match bound {
		Bound::Included(v) => Bound::Included(v.to_vec()),
		Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{id::TableId, shape::ShapeId};

	use super::*;

	#[test]
	fn test_basic_operations() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let key = CowVec::new(b"key1".to_vec());
		let version = CommitVersion(1);

		// Put and get
		storage.set(
			version,
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value1".to_vec())))])]),
		)
		.unwrap();
		let value = storage.get(EntryKind::Multi, &key, version).unwrap();
		assert_eq!(value.as_deref(), Some(b"value1".as_slice()));

		// Contains
		assert!(storage.contains(EntryKind::Multi, &key, version).unwrap());
		assert!(!storage.contains(EntryKind::Multi, b"nonexistent", version).unwrap());

		// Delete (tombstone)
		let version2 = CommitVersion(2);
		storage.set(version2, HashMap::from([(EntryKind::Multi, vec![(key.clone(), None)])])).unwrap();
		assert!(!storage.contains(EntryKind::Multi, &key, version2).unwrap());
	}

	#[test]
	fn test_source_tables() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let source1 = ShapeId::Table(TableId(1));
		let source2 = ShapeId::Table(TableId(2));
		let key = CowVec::new(b"key".to_vec());
		let version = CommitVersion(1);

		storage.set(
			version,
			HashMap::from([(
				EntryKind::Source(source1),
				vec![(key.clone(), Some(CowVec::new(b"table1".to_vec())))],
			)]),
		)
		.unwrap();
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Source(source2),
				vec![(key.clone(), Some(CowVec::new(b"table2".to_vec())))],
			)]),
		)
		.unwrap();

		assert_eq!(
			storage.get(EntryKind::Source(source1), &key, version).unwrap().as_deref(),
			Some(b"table1".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Source(source2), &key, version).unwrap().as_deref(),
			Some(b"table2".as_slice())
		);
	}

	#[test]
	fn test_version_queries() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let key = CowVec::new(b"key1".to_vec());

		// Insert multiple versions
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(2),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v2".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(3),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v3".to_vec())))])]),
		)
		.unwrap();

		// Get at specific versions
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().as_deref(),
			Some(b"v3".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().as_deref(),
			Some(b"v2".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().as_deref(),
			Some(b"v1".as_slice())
		);

		// Get at intermediate version returns closest <= version
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(10)).unwrap().as_deref(),
			Some(b"v3".as_slice())
		);
	}

	#[test]
	fn test_range_next() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
					(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
					(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 100)
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
		assert_eq!(&*batch.entries[0].key, b"a");
		assert_eq!(&*batch.entries[1].key, b"b");
		assert_eq!(&*batch.entries[2].key, b"c");
	}

	#[test]
	fn test_range_rev_next() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
					(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
					(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_rev_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 100)
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
		assert_eq!(&*batch.entries[0].key, b"c");
		assert_eq!(&*batch.entries[1].key, b"b");
		assert_eq!(&*batch.entries[2].key, b"a");
	}

	#[test]
	fn test_range_streaming_pagination() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let version = CommitVersion(1);

		let entries: Vec<_> =
			(0..10u8).map(|i| (CowVec::new(vec![i]), Some(CowVec::new(vec![i * 10])))).collect();
		storage.set(version, HashMap::from([(EntryKind::Multi, entries)])).unwrap();

		let mut cursor = RangeCursor::new();

		let batch1 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch1.entries[0].key, &[0]);
		assert_eq!(&*batch1.entries[2].key, &[2]);

		let batch2 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch2.entries[0].key, &[3]);
		assert_eq!(&*batch2.entries[2].key, &[5]);
	}

	#[test]
	fn test_range_reving_pagination() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let version = CommitVersion(1);

		let entries: Vec<_> =
			(0..10u8).map(|i| (CowVec::new(vec![i]), Some(CowVec::new(vec![i * 10])))).collect();
		storage.set(version, HashMap::from([(EntryKind::Multi, entries)])).unwrap();

		let mut cursor = RangeCursor::new();

		let batch1 = storage
			.range_rev_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch1.entries[0].key, &[9]);
		assert_eq!(&*batch1.entries[2].key, &[7]);

		let batch2 = storage
			.range_rev_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.exhausted);
		assert_eq!(&*batch2.entries[0].key, &[6]);
		assert_eq!(&*batch2.entries[2].key, &[4]);
	}

	#[test]
	fn test_get_nonexistent_table() {
		let storage = SqlitePrimitiveStorage::in_memory();
		let value = storage.get(EntryKind::Multi, b"key", CommitVersion(1)).unwrap();
		assert_eq!(value, None);
	}

	#[test]
	fn test_range_nonexistent_table() {
		let storage = SqlitePrimitiveStorage::in_memory();
		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				CommitVersion(1),
				100,
			)
			.unwrap();
		assert!(batch.entries.is_empty());
		assert!(cursor.exhausted);
	}

	#[test]
	fn test_drop_specific_version() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let key = CowVec::new(b"key1".to_vec());

		// Insert versions 1, 2, 3
		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{}", v).into_bytes())))],
				)]),
			)
			.unwrap();
		}

		// Drop version 1 (lives in historical at this point)
		storage.drop(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(1))])])).unwrap();

		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().is_none());
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().as_deref(),
			Some(b"v2".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().as_deref(),
			Some(b"v3".as_slice())
		);
	}

	#[test]
	fn test_current_promotion_on_newer_write() {
		// Successive writes promote prior current to historical.
		let storage = SqlitePrimitiveStorage::in_memory();

		let key = CowVec::new(b"k".to_vec());
		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{}", v).into_bytes())))],
				)]),
			)
			.unwrap();
		}

		// All three versions are reachable.
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().as_deref(),
			Some(b"v1".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().as_deref(),
			Some(b"v2".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().as_deref(),
			Some(b"v3".as_slice())
		);

		// And the per-physical-table layout is what we expect: one row in
		// __current at version 3, two in __historical at versions 1 and 2.
		let conn = storage.inner.conn.lock();
		let current_count: i64 =
			conn.query_row("SELECT COUNT(*) FROM \"multi__current\"", [], |row| row.get(0)).unwrap();
		let historical_count: i64 =
			conn.query_row("SELECT COUNT(*) FROM \"multi__historical\"", [], |row| row.get(0)).unwrap();
		assert_eq!(current_count, 1);
		assert_eq!(historical_count, 2);
	}

	#[test]
	fn test_out_of_order_write_lands_in_historical() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let key = CowVec::new(b"k".to_vec());
		// First write: v3 (becomes current).
		storage.set(
			CommitVersion(3),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v3".to_vec())))])]),
		)
		.unwrap();
		// Second write: v1 (older than current; should land in historical).
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();

		// Get at v3 returns the current.
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().as_deref(),
			Some(b"v3".as_slice())
		);
		// Get at v1 reaches into historical and finds v1.
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().as_deref(),
			Some(b"v1".as_slice())
		);
		// Get at v2 returns the largest version <= 2, which is v1.
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().as_deref(),
			Some(b"v1".as_slice())
		);

		// __current still has v3; __historical has v1.
		let conn = storage.inner.conn.lock();
		let current_version: Vec<u8> = conn
			.query_row(
				"SELECT version FROM \"multi__current\" WHERE key = ?1",
				params![key.as_slice()],
				|row| row.get(0),
			)
			.unwrap();
		assert_eq!(version_from_bytes(&current_version), CommitVersion(3));
	}

	#[test]
	fn test_drop_current_promotes_historical() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let key = CowVec::new(b"k".to_vec());
		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{}", v).into_bytes())))],
				)]),
			)
			.unwrap();
		}

		// At this point: __current = v3; __historical = {v1, v2}.
		// Drop the current (v3) - v2 should be promoted into __current.
		storage.drop(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(3))])])).unwrap();

		// Reads at v3 now return v2 (the new current).
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().as_deref(),
			Some(b"v2".as_slice())
		);
		// v3 is gone.
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().as_deref(),
			Some(b"v2".as_slice())
		);
		// v1 still reachable via historical.
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().as_deref(),
			Some(b"v1".as_slice())
		);
	}

	#[test]
	fn test_get_at_old_snapshot_uses_historical() {
		// A snapshot read at a version older than current must consult historical.
		let storage = SqlitePrimitiveStorage::in_memory();

		let key = CowVec::new(b"k".to_vec());
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(5),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v5".to_vec())))])]),
		)
		.unwrap();

		// Snapshot at 3 sees v1 (historical fallback).
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().as_deref(),
			Some(b"v1".as_slice())
		);
		// Snapshot at 5 sees current.
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(5)).unwrap().as_deref(),
			Some(b"v5".as_slice())
		);
	}

	#[test]
	fn test_range_at_old_snapshot_falls_back_to_historical() {
		// A range read at a snapshot older than current.version must
		// surface the historical row for keys whose current is "from the future".
		let storage = SqlitePrimitiveStorage::in_memory();

		// Two keys, each with v1 and v5; v5 ends up as current.
		for v in [1u64, 5] {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![
						(
							CowVec::new(b"a".to_vec()),
							Some(CowVec::new(format!("a{}", v).into_bytes())),
						),
						(
							CowVec::new(b"b".to_vec()),
							Some(CowVec::new(format!("b{}", v).into_bytes())),
						),
					],
				)]),
			)
			.unwrap();
		}

		// Snapshot at version 3: both keys' current is v5, fallback to historical v1.
		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				CommitVersion(3),
				100,
			)
			.unwrap();

		assert_eq!(batch.entries.len(), 2);
		assert_eq!(&*batch.entries[0].key, b"a");
		assert_eq!(batch.entries[0].version, CommitVersion(1));
		assert_eq!(batch.entries[0].value.as_deref().map(|v| v.to_vec()), Some(b"a1".to_vec()));
		assert_eq!(&*batch.entries[1].key, b"b");
		assert_eq!(batch.entries[1].version, CommitVersion(1));
	}

	#[test]
	fn test_get_all_versions_unions_current_and_historical() {
		let storage = SqlitePrimitiveStorage::in_memory();

		let key = CowVec::new(b"k".to_vec());
		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{}", v).into_bytes())))],
				)]),
			)
			.unwrap();
		}

		let versions = storage.get_all_versions(EntryKind::Multi, &key).unwrap();
		assert_eq!(versions.len(), 3);
		assert_eq!(versions[0].0, CommitVersion(3));
		assert_eq!(versions[1].0, CommitVersion(2));
		assert_eq!(versions[2].0, CommitVersion(1));
	}
}
