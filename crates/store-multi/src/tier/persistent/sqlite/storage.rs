// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	iter::repeat_n,
	ops::Bound,
	sync::{
		Arc, OnceLock,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::bytes::{SHAPE_HEADER_SIZE, read_updated_at},
};
#[cfg(test)]
use reifydb_core::metrics::scan::ScanCounters;
use reifydb_core::{
	common::CommitVersion,
	error::diagnostic::internal::internal,
	interface::{catalog::storage::StorageId, store::EntryKind},
	key::row::StorageRowKey,
	metrics::scan::record_page,
};
use reifydb_runtime::{
	shutdown::Shutdown,
	sync::{
		map::Map,
		mutex::{Mutex, MutexGuard},
	},
};
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard, pragma};
use reifydb_store::{
	coverage::cursor::Cursor,
	filter::KeyFilter,
	metrics::PageCacheMetrics,
	sqlite::{OpenMessages, open, page_cache_metrics, pool::ReadPool},
};
use reifydb_store_commit::{
	MultiVersionScope, RangeBatch, RangeCursor, RangeStop, RawEntry, TierBatch, VersionedGetResult,
};
use reifydb_value::{
	Result, error,
	util::cowvec::CowVec,
	value::{datetime::DateTime, row_number::RowNumber},
};
use rusqlite::{
	Connection, Error::QueryReturnedNoRows, Result as SqliteResult, Row, ToSql, Transaction, TransactionBehavior,
	params_from_iter,
};
use tracing::{instrument, warn};

use super::schema::row_from_sql;
use crate::{
	filter::{ARMED_CAPACITY_KEYS, MultiKeys},
	tier::{
		TierStorage,
		persistent::sqlite::{
			entry::{SqliteSchema, current_table_name, current_table_name_to_entry, sqlite_schema},
			query::{
				build_chunked_upsert_sql, build_chunked_upsert_sql_partitioned,
				build_chunked_upsert_sql_row, build_create_current_sql,
				build_create_current_sql_partitioned, build_create_current_sql_row,
				build_current_exists_sql, build_current_keys_sql, build_current_keys_sql_partitioned,
				build_current_keys_sql_row, build_delete_current_sql,
				build_delete_current_sql_partitioned, build_delete_current_sql_row,
				build_delete_keys_sql, build_delete_keys_sql_partitioned, build_delete_keys_sql_row,
				build_expired_keys_sql, build_expired_keys_sql_partitioned, build_expired_keys_sql_row,
				build_get_current_sql, build_get_current_sql_partitioned, build_get_current_sql_row,
				build_get_many_current_sql, build_get_many_current_sql_partitioned,
				build_get_many_current_sql_row, build_max_version_sql, build_range_current_sql,
				build_range_current_sql_partitioned, build_range_current_sql_partitioned_exact,
				build_range_current_sql_row, build_upsert_current_sql,
				build_upsert_current_sql_partitioned, build_upsert_current_sql_row, version_from_bytes,
				version_to_bytes,
			},
			schema::{
				PartitionedRangeBounds, partition_half_to_sql, partitioned_ident_of,
				partitioned_key_for, partitioned_range_bounds, row_ident_of, row_key_for,
				row_range_bounds, row_to_sql,
			},
		},
	},
};

const GET_MANY_CHUNK: usize = 900;

const UPSERT_CHUNK: usize = 100;

const GET_MANY_BUCKETS: [usize; 5] = [1, 8, 64, 512, GET_MANY_CHUNK];

fn bucket_key_count(len: usize) -> usize {
	for &bucket in GET_MANY_BUCKETS.iter() {
		if len <= bucket {
			return bucket;
		}
	}
	GET_MANY_CHUNK
}

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
	filter: KeyFilter<MultiKeys>,

	written_high_water: AtomicU64,
	opened_high_water: OnceLock<u64>,
}

struct TableSql {
	table_name: String,
	schema: SqliteSchema,
	get_sql: String,
	upsert_sql: String,
	chunked_upsert_sql: String,
	delete_sql: String,
	chunked_delete_sql: String,
	create_sql: String,
}

impl TableSql {
	fn build(table: EntryKind) -> Self {
		let table_name = current_table_name(table);
		let schema = sqlite_schema(table);
		let (get_sql, upsert_sql, chunked_upsert_sql, delete_sql, chunked_delete_sql, create_sql) = match schema
		{
			SqliteSchema::Blob => (
				build_get_current_sql(&table_name),
				build_upsert_current_sql(&table_name),
				build_chunked_upsert_sql(&table_name, UPSERT_CHUNK),
				build_delete_current_sql(&table_name, 1, false),
				build_delete_current_sql(&table_name, UPSERT_CHUNK, true),
				build_create_current_sql(&table_name),
			),
			SqliteSchema::Row => (
				build_get_current_sql_row(&table_name),
				build_upsert_current_sql_row(&table_name),
				build_chunked_upsert_sql_row(&table_name, UPSERT_CHUNK),
				build_delete_current_sql_row(&table_name, 1, false),
				build_delete_current_sql_row(&table_name, UPSERT_CHUNK, true),
				build_create_current_sql_row(&table_name),
			),
			SqliteSchema::Partitioned => (
				build_get_current_sql_partitioned(&table_name),
				build_upsert_current_sql_partitioned(&table_name),
				build_chunked_upsert_sql_partitioned(&table_name, UPSERT_CHUNK),
				build_delete_current_sql_partitioned(&table_name, 1, false),
				build_delete_current_sql_partitioned(&table_name, UPSERT_CHUNK, true),
				build_create_current_sql_partitioned(&table_name),
			),
		};
		Self {
			table_name,
			schema,
			get_sql,
			upsert_sql,
			chunked_upsert_sql,
			delete_sql,
			chunked_delete_sql,
			create_sql,
		}
	}
}

const OPEN_MESSAGES: OpenMessages = OpenMessages {
	connect: "Failed to connect to persistent database",
	pragmas: "Failed to configure persistent SQLite pragmas",
	busy_timeout: "Failed to set persistent busy timeout",
	read_connect: "Failed to open persistent read connection",
	read_pragmas: "Failed to configure persistent read connection",
	read_busy_timeout: "Failed to set persistent read busy timeout",
};

impl SqlitePersistentStorage {
	#[instrument(name = "store::multi::persistent::sqlite::new", level = "debug", skip(config), fields(
		db_path = ?config.path,
		page_size = config.page_size.as_ref().map(|size| size.as_bytes()),
		read_pool_size = config.read_pool_size,
		journal_mode = config.journal_mode.as_ref().map(|mode| mode.as_str())
	))]
	pub fn new(config: SqliteConfig) -> Self {
		let (conn, readers) = open(&config, "persistent.db", &OPEN_MESSAGES);

		let filter = if any_current_row(&conn) {
			KeyFilter::<MultiKeys>::new()
		} else {
			KeyFilter::<MultiKeys>::armed(ARMED_CAPACITY_KEYS)
		};

		Self {
			inner: Arc::new(SqlitePersistentStorageInner {
				conn: Mutex::new(Some(conn)),
				readers,
				table_sql: Map::new(),
				cache_hits: AtomicU64::new(0),
				cache_misses: AtomicU64::new(0),
				filter,
				written_high_water: AtomicU64::new(0),
				opened_high_water: OnceLock::new(),
			}),
		}
	}

	pub fn install_floor(&self) -> Result<CommitVersion> {
		let opened = match self.inner.opened_high_water.get() {
			Some(opened) => *opened,
			None => {
				let probed = self.probe_high_water()?;
				*self.inner.opened_high_water.get_or_init(|| probed)
			}
		};
		Ok(CommitVersion(opened.max(self.inner.written_high_water.load(Ordering::SeqCst))))
	}

	fn probe_high_water(&self) -> Result<u64> {
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Err(error!(internal(
				"Persistent storage is shut down; refusing to probe the install floor, whose \
				 fallback would gate coverage materializes on a version no read ever observed"
					.to_string()
			)));
		};
		Ok(highest_current_version(conn))
	}

	fn record_written_version(&self, version: CommitVersion) {
		self.inner.written_high_water.fetch_max(version.0, Ordering::SeqCst);
	}

	pub fn filter(&self) -> &KeyFilter<MultiKeys> {
		&self.inner.filter
	}

	pub(crate) fn current_key_slice(
		&self,
		table: EntryKind,
		cursor: Option<&EncodedKey>,
		budget: usize,
	) -> Result<Vec<EncodedKey>> {
		if budget == 0 {
			return Ok(Vec::new());
		}
		let table_sql = self.table_sql(table);
		let storage = source_storage(table);
		let limit = budget.min(i64::MAX as usize) as i64;

		let sql = match table_sql.schema {
			SqliteSchema::Blob => build_current_keys_sql(&table_sql.table_name, cursor.is_some()),
			SqliteSchema::Row => build_current_keys_sql_row(&table_sql.table_name, cursor.is_some()),
			SqliteSchema::Partitioned => {
				build_current_keys_sql_partitioned(&table_sql.table_name, cursor.is_some())
			}
		};

		let mut params: Vec<Box<dyn ToSql>> = Vec::new();
		if let Some(key) = cursor {
			match table_sql.schema {
				SqliteSchema::Blob => params.push(Box::new(key.to_vec())),
				_ => {
					let ints = key_ints(table_sql.schema, key.as_slice()).ok_or_else(|| {
						error!(internal(
							"a current-key cursor does not decode under its own table's \
							 narrow schema"
								.to_string()
						))
					})?;
					for i in ints {
						params.push(Box::new(i));
					}
				}
			}
		}
		params.push(Box::new(limit));

		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(Vec::new());
		};
		let mut stmt = match conn.prepare_cached(&sql) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
			Err(e) => {
				return Err(error!(internal(format!("Failed to prepare current key scan: {}", e))));
			}
		};
		let flat: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
		let mut rows = stmt
			.query(params_from_iter(flat))
			.map_err(|e| error!(internal(format!("Failed to scan current keys: {}", e))))?;

		let mut out = Vec::with_capacity(budget);
		while let Some(row) =
			rows.next().map_err(|e| error!(internal(format!("Failed to read current key: {}", e))))?
		{
			let returned = read_returned_key(table_sql.schema, row)
				.map_err(|e| error!(internal(format!("Failed to decode current key: {}", e))))?;
			out.push(returned.into_encoded_key(storage));
		}
		Ok(out)
	}

	pub fn page_cache_metrics(&self) -> PageCacheMetrics {
		page_cache_metrics(
			&self.inner.conn,
			&self.inner.readers,
			&self.inner.cache_hits,
			&self.inner.cache_misses,
		)
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
			let sql = match table_sql.schema {
				SqliteSchema::Blob => build_delete_keys_sql(&table_sql.table_name, chunk.len()),
				SqliteSchema::Row => build_delete_keys_sql_row(&table_sql.table_name, chunk.len()),
				SqliteSchema::Partitioned => {
					build_delete_keys_sql_partitioned(&table_sql.table_name, chunk.len())
				}
			};
			let mut boxed: Vec<Box<dyn ToSql>> =
				Vec::with_capacity(chunk.len() * table_sql.schema.key_column_count());
			for key in chunk {
				push_key_params(table_sql.schema, key.as_slice(), &mut boxed)?;
			}
			let flat: Vec<&dyn ToSql> = boxed.iter().map(|p| p.as_ref()).collect();
			match conn.execute(&sql, params_from_iter(flat)) {
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
		let storage = source_storage(table);
		let limit = limit.min(i64::MAX as usize);
		let sql = match table_sql.schema {
			SqliteSchema::Blob => build_expired_keys_sql(&table_sql.table_name, cursor.is_some(), limit),
			SqliteSchema::Row => build_expired_keys_sql_row(&table_sql.table_name, cursor.is_some(), limit),
			SqliteSchema::Partitioned => {
				build_expired_keys_sql_partitioned(&table_sql.table_name, cursor.is_some(), limit)
			}
		};
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
			match table_sql.schema {
				SqliteSchema::Blob => params.push(Box::new(key.to_vec())),
				_ => {
					let ints = key_ints(table_sql.schema, key).ok_or_else(|| {
						error!(internal(
							"an expired-keys cursor does not decode under its own \
							 table's narrow schema"
								.to_string()
						))
					})?;
					for i in ints {
						params.push(Box::new(i));
					}
				}
			}
		}
		let key_columns = table_sql.schema.key_column_count();
		let flat: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
		let rows = match stmt.query_map(params_from_iter(flat), |row| {
			let returned = read_returned_key(table_sql.schema, row)?;
			let nanos: i64 = row.get(key_columns)?;
			Ok((returned, DateTime::from_nanos(nanos as u64)))
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
			let (returned, at) = row.map_err(|e| {
				error!(internal(format!(
					"Failed to read expired key from {}: {}",
					table_sql.table_name, e
				)))
			})?;
			out.push((returned.into_encoded_key(storage), at));
		}
		Ok(out)
	}

	fn upsert_entries_collecting_accepted(
		&self,
		tx: &Transaction,
		table: EntryKind,
		table_sql: &TableSql,
		version: CommitVersion,
		entries: &[(EncodedKey, Option<CowVec<u8>>)],
		accepted: &mut Vec<EncodedKey>,
	) -> Result<()> {
		self.record_written_version(version);
		let new_version_bytes = version_to_bytes(version);
		let mut chunk_stmt = tx
			.prepare_cached(&table_sql.chunked_upsert_sql)
			.map_err(|e| error!(internal(format!("Failed to prepare chunked persistent upsert: {}", e))))?;
		let mut single_stmt = tx
			.prepare_cached(&table_sql.upsert_sql)
			.map_err(|e| error!(internal(format!("Failed to prepare persistent upsert: {}", e))))?;

		let mut sets: Vec<&(EncodedKey, Option<CowVec<u8>>)> = Vec::with_capacity(entries.len());
		let mut removals: Vec<&EncodedKey> = Vec::new();
		for entry in entries {
			if entry.1.is_some() {
				sets.push(entry);
			} else {
				removals.push(&entry.0);
			}
		}

		let storage = source_storage(table);
		let key_columns = table_sql.schema.key_column_count();

		let mut chunks = sets.chunks_exact(UPSERT_CHUNK);
		for chunk in chunks.by_ref() {
			let mut boxed: Vec<Box<dyn ToSql>> = Vec::with_capacity(chunk.len() * (key_columns + 3));
			for (key, value) in chunk.iter().copied() {
				self.inner.filter.add((table, key));
				push_key_params(table_sql.schema, key.as_slice(), &mut boxed)?;
				boxed.push(Box::new(new_version_bytes.to_vec()));
				boxed.push(Box::new(value.as_ref().map(|v| v.as_slice().to_vec())));
				boxed.push(Box::new(
					expiry_stamp(table, value.as_ref()).map(|at| at.to_nanos() as i64),
				));
			}
			let flat: Vec<&dyn ToSql> = boxed.iter().map(|p| p.as_ref()).collect();
			let returned = chunk_stmt
				.query_map(params_from_iter(flat), |row| read_returned_key(table_sql.schema, row))
				.map_err(|e| error!(internal(format!("Failed to upsert persistent rows: {}", e))))?;
			for key in returned {
				let key = key
					.map_err(|e| {
						error!(internal(format!(
							"Failed to read accepted persistent key: {}",
							e
						)))
					})?
					.into_encoded_key(storage);
				accepted.push(key);
			}
		}

		for (key, value) in chunks.remainder().iter().copied() {
			self.inner.filter.add((table, key));
			let mut boxed: Vec<Box<dyn ToSql>> = Vec::with_capacity(key_columns + 3);
			push_key_params(table_sql.schema, key.as_slice(), &mut boxed)?;
			boxed.push(Box::new(new_version_bytes.to_vec()));
			boxed.push(Box::new(value.as_ref().map(|v| v.as_slice().to_vec())));
			boxed.push(Box::new(expiry_stamp(table, value.as_ref()).map(|at| at.to_nanos() as i64)));
			let flat: Vec<&dyn ToSql> = boxed.iter().map(|p| p.as_ref()).collect();
			let affected = single_stmt
				.execute(params_from_iter(flat))
				.map_err(|e| error!(internal(format!("Failed to upsert persistent row: {}", e))))?;
			if affected > 0 {
				accepted.push(key.clone());
			}
		}

		self.delete_entries_collecting_accepted(tx, table, table_sql, &new_version_bytes, &removals, accepted)?;

		Ok(())
	}

	fn delete_entries_collecting_accepted(
		&self,
		tx: &Transaction,
		table: EntryKind,
		table_sql: &TableSql,
		version_bytes: &[u8],
		removals: &[&EncodedKey],
		accepted: &mut Vec<EncodedKey>,
	) -> Result<()> {
		if removals.is_empty() {
			return Ok(());
		}
		let mut chunk_stmt = tx
			.prepare_cached(&table_sql.chunked_delete_sql)
			.map_err(|e| error!(internal(format!("Failed to prepare chunked persistent delete: {}", e))))?;
		let mut single_stmt = tx
			.prepare_cached(&table_sql.delete_sql)
			.map_err(|e| error!(internal(format!("Failed to prepare persistent delete: {}", e))))?;

		let storage = source_storage(table);
		let key_columns = table_sql.schema.key_column_count();

		let mut chunks = removals.chunks_exact(UPSERT_CHUNK);
		for chunk in chunks.by_ref() {
			let mut boxed: Vec<Box<dyn ToSql>> = Vec::with_capacity(chunk.len() * key_columns + 1);
			for key in chunk.iter() {
				push_key_params(table_sql.schema, key.as_slice(), &mut boxed)?;
			}
			boxed.push(Box::new(version_bytes.to_vec()));
			let flat: Vec<&dyn ToSql> = boxed.iter().map(|p| p.as_ref()).collect();
			let returned = chunk_stmt
				.query_map(params_from_iter(flat), |row| read_returned_key(table_sql.schema, row))
				.map_err(|e| error!(internal(format!("Failed to delete persistent rows: {}", e))))?;
			for key in returned {
				let key = key
					.map_err(|e| {
						error!(internal(format!(
							"Failed to read deleted persistent key: {}",
							e
						)))
					})?
					.into_encoded_key(storage);
				accepted.push(key);
			}
		}

		for key in chunks.remainder().iter().copied() {
			let mut boxed: Vec<Box<dyn ToSql>> = Vec::with_capacity(key_columns + 1);
			push_key_params(table_sql.schema, key.as_slice(), &mut boxed)?;
			boxed.push(Box::new(version_bytes.to_vec()));
			let flat: Vec<&dyn ToSql> = boxed.iter().map(|p| p.as_ref()).collect();
			let affected = single_stmt
				.execute(params_from_iter(flat))
				.map_err(|e| error!(internal(format!("Failed to delete persistent row: {}", e))))?;
			if affected > 0 {
				accepted.push((*key).clone());
			}
		}

		Ok(())
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

		for (table, entries) in batches {
			let table_sql = self.table_sql(table);
			Self::create_table_if_needed(&tx, &table_sql.create_sql)
				.map_err(|e| error!(internal(format!("Failed to ensure persistent table: {}", e))))?;

			self.upsert_entries_collecting_accepted(
				&tx,
				table,
				&table_sql,
				version,
				&entries,
				&mut accepted,
			)?;
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

		let mut ensured: HashSet<EntryKind> = HashSet::new();
		for (version, batch) in batches {
			for (table, entries) in batch {
				let table_sql = self.table_sql(table);
				if ensured.insert(table) {
					Self::create_table_if_needed(&tx, &table_sql.create_sql).map_err(|e| {
						error!(internal(format!("Failed to ensure persistent table: {}", e)))
					})?;
				}

				self.upsert_entries_collecting_accepted(
					&tx,
					table,
					&table_sql,
					version,
					&entries,
					&mut accepted,
				)?;
			}
		}

		tx.commit().map_err(|e| error!(internal(format!("Failed to commit persistent transaction: {}", e))))?;
		Ok(accepted)
	}

	fn create_table_if_needed(conn: &Connection, create_sql: &str) -> SqliteResult<()> {
		conn.execute_batch(create_sql)?;
		Ok(())
	}

	pub(crate) fn range_chunk_row(
		&self,
		cursor: &mut Cursor<RangeStop, StorageRowKey>,
		req: RowRangeChunkRequest<'_>,
	) -> Result<RangeBatch<StorageRowKey>> {
		if cursor.is_exhausted() {
			return Ok(RangeBatch::empty());
		}

		let table_sql = self.table_sql(req.table);
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Err(error!(internal(
				"Persistent storage is shut down; refusing to report a range chunk exhausted \
				 having read nothing, which hands the caller a short scan reported as a \
				 complete one"
					.to_string()
			)));
		};

		let version_bytes = version_to_bytes(req.scope.read()).to_vec();
		let limit_i64 = req.batch_size as i64;

		let to_sql_bound = |bound: Bound<&StorageRowKey>| match bound {
			Bound::Included(key) => Bound::Included(row_to_sql(key.row().0)),
			Bound::Excluded(key) => Bound::Excluded(row_to_sql(key.row().0)),
			Bound::Unbounded => Bound::Unbounded,
		};
		let lower = to_sql_bound(req.start);
		let upper = to_sql_bound(req.end);
		let last_row = cursor.last_key().map(|key| row_to_sql(key.row().0));

		let sql = build_range_current_sql_row(
			&table_sql.table_name,
			bound_shape_i64(&lower),
			bound_shape_i64(&upper),
			last_row.is_some(),
			req.descending,
		);
		let mut stmt = match conn.prepare_cached(&sql) {
			Ok(s) => s,
			Err(e) if e.to_string().contains("no such table") => {
				cursor.finish_with(RangeStop::AbsentTable);
				return Ok(RangeBatch::empty());
			}
			Err(e) => {
				return Err(error!(internal(format!("Failed to prepare persistent range: {}", e))));
			}
		};

		let mut params: Vec<Box<dyn ToSql>> = Vec::new();
		if let Some(v) = bound_value(lower) {
			params.push(Box::new(v));
		}
		if let Some(v) = bound_value(upper) {
			params.push(Box::new(v));
		}
		if let Some(v) = last_row {
			params.push(Box::new(v));
		}
		params.push(Box::new(version_bytes));
		params.push(Box::new(limit_i64));
		let flat: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();

		let raw: Vec<RawEntry<StorageRowKey>> = match stmt.query_map(params_from_iter(flat), |row| {
			let r: i64 = row.get(0)?;
			let version_blob: Vec<u8> = row.get(1)?;
			let value: Option<Vec<u8>> = row.get(2)?;
			Ok(RawEntry {
				key: StorageRowKey::new(RowNumber(row_from_sql(r))),
				version: version_from_bytes(&version_blob),
				value: value.map(CowVec::new),
			})
		}) {
			Ok(rows) => rows
				.collect::<SqliteResult<Vec<_>>>()
				.map_err(|e| error!(internal(format!("Failed to read persistent row: {}", e))))?,
			Err(e) if e.to_string().contains("no such table") => {
				cursor.finish_with(RangeStop::AbsentTable);
				return Ok(RangeBatch::empty());
			}
			Err(e) => {
				return Err(error!(internal(format!("Failed to scan persistent range: {}", e))));
			}
		};

		record_page(raw.len() as u64, raw.iter().filter(|e| e.value.is_none()).count() as u64);
		let has_more = raw.len() == req.batch_size;
		if let Some(last) = raw.last() {
			cursor.advance(last.key);
		}
		if !has_more {
			cursor.finish_with(RangeStop::Scanned);
		}
		Ok(RangeBatch {
			entries: raw,
			has_more,
		})
	}

	fn range_chunk(&self, cursor: &mut RangeCursor, req: RangeChunkRequest<'_>) -> Result<RangeBatch> {
		if cursor.is_exhausted() {
			return Ok(RangeBatch::empty());
		}

		let table_sql = self.table_sql(req.table);
		let storage = source_storage(req.table);
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Err(error!(internal(
				"Persistent storage is shut down; refusing to report a range chunk exhausted \
				 having read nothing, which hands the caller a short scan reported as a \
				 complete one"
					.to_string()
			)));
		};

		let version_bytes = version_to_bytes(req.scope.read()).to_vec();
		let limit_i64 = req.batch_size as i64;

		let raw: Vec<RawEntry> = match table_sql.schema {
			SqliteSchema::Blob => {
				let sql = build_range_current_sql(
					&table_sql.table_name,
					bound_shape(req.start),
					bound_shape(req.end),
					cursor.last_key().is_some(),
					req.descending,
				);
				let mut stmt = match conn.prepare_cached(&sql) {
					Ok(s) => s,
					Err(e) if e.to_string().contains("no such table") => {
						cursor.finish_with(RangeStop::AbsentTable);
						return Ok(RangeBatch::empty());
					}
					Err(e) => {
						return Err(error!(internal(format!(
							"Failed to prepare persistent range: {}",
							e
						))));
					}
				};
				let mut params: Vec<Box<dyn ToSql>> = Vec::new();
				match req.start {
					Bound::Included(s) | Bound::Excluded(s) => params.push(Box::new(s.to_vec())),
					Bound::Unbounded => {}
				}
				match req.end {
					Bound::Included(e) | Bound::Excluded(e) => params.push(Box::new(e.to_vec())),
					Bound::Unbounded => {}
				}
				if let Some(k) = cursor.last_key().map(|k| k.as_slice()) {
					params.push(Box::new(k.to_vec()));
				}
				params.push(Box::new(version_bytes.clone()));
				params.push(Box::new(limit_i64));
				match stmt.query_map(params_from_iter(params), |row| {
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
						error!(internal(format!("Failed to read persistent row: {}", e)))
					})?,
					Err(e) if e.to_string().contains("no such table") => {
						cursor.finish_with(RangeStop::AbsentTable);
						return Ok(RangeBatch::empty());
					}
					Err(e) => {
						return Err(error!(internal(format!(
							"Failed to scan persistent range: {}",
							e
						))));
					}
				}
			}
			SqliteSchema::Row => {
				let bounds = row_range_bounds(req.start, req.end);
				let last_row =
					cursor.last_key()
						.map(|k| {
							row_ident_of(k.as_slice())
								.map(|ident| row_to_sql(ident.row().0))
								.ok_or_else(|| {
									error!(internal("a range cursor does not decode as a RowKey".to_string()))
								})
						})
						.transpose()?;
				let sql = build_range_current_sql_row(
					&table_sql.table_name,
					bound_shape_i64(&bounds.lower),
					bound_shape_i64(&bounds.upper),
					last_row.is_some(),
					req.descending,
				);
				let mut stmt = match conn.prepare_cached(&sql) {
					Ok(s) => s,
					Err(e) if e.to_string().contains("no such table") => {
						cursor.finish_with(RangeStop::AbsentTable);
						return Ok(RangeBatch::empty());
					}
					Err(e) => {
						return Err(error!(internal(format!(
							"Failed to prepare persistent range: {}",
							e
						))));
					}
				};
				let mut params: Vec<Box<dyn ToSql>> = Vec::new();
				if let Some(v) = bound_value(bounds.lower) {
					params.push(Box::new(v));
				}
				if let Some(v) = bound_value(bounds.upper) {
					params.push(Box::new(v));
				}
				if let Some(v) = last_row {
					params.push(Box::new(v));
				}
				params.push(Box::new(version_bytes.clone()));
				params.push(Box::new(limit_i64));
				let storage_id = storage.expect("row schema entry kinds always carry a storage id");
				let flat: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
				match stmt.query_map(params_from_iter(flat), |row| {
					let r: i64 = row.get(0)?;
					let version_blob: Vec<u8> = row.get(1)?;
					let value: Option<Vec<u8>> = row.get(2)?;
					Ok(RawEntry {
						key: row_key_for(storage_id, r),
						version: version_from_bytes(&version_blob),
						value: value.map(CowVec::new),
					})
				}) {
					Ok(rows) => rows.collect::<SqliteResult<Vec<_>>>().map_err(|e| {
						error!(internal(format!("Failed to read persistent row: {}", e)))
					})?,
					Err(e) if e.to_string().contains("no such table") => {
						cursor.finish_with(RangeStop::AbsentTable);
						return Ok(RangeBatch::empty());
					}
					Err(e) => {
						return Err(error!(internal(format!(
							"Failed to scan persistent range: {}",
							e
						))));
					}
				}
			}
			SqliteSchema::Partitioned => {
				let bounds = partitioned_range_bounds(req.start, req.end);
				let storage_id =
					storage.expect("partitioned schema entry kinds always carry a storage id");
				match bounds {
					PartitionedRangeBounds::ExactPartition {
						partition_hi,
						partition_lo,
						lower_row,
						upper_row,
					} => {
						let last_row = cursor
							.last_key()
							.map(|k| {
								partitioned_ident_of(k.as_slice())
									.map(|ident| row_to_sql(ident.row().0))
									.ok_or_else(|| {
										error!(internal(
											"a range cursor does not decode as a \
											 PartitionedRowKey"
												.to_string()
										))
									})
							})
							.transpose()?;
						let sql = build_range_current_sql_partitioned_exact(
							&table_sql.table_name,
							bound_shape_i64(&lower_row),
							bound_shape_i64(&upper_row),
							last_row.is_some(),
							req.descending,
						);
						let mut stmt = match conn.prepare_cached(&sql) {
							Ok(s) => s,
							Err(e) if e.to_string().contains("no such table") => {
								cursor.finish_with(RangeStop::AbsentTable);
								return Ok(RangeBatch::empty());
							}
							Err(e) => {
								return Err(error!(internal(format!(
									"Failed to prepare persistent range: {}",
									e
								))));
							}
						};
						let mut params: Vec<Box<dyn ToSql>> =
							vec![Box::new(partition_hi), Box::new(partition_lo)];
						if let Some(v) = bound_value(lower_row) {
							params.push(Box::new(v));
						}
						if let Some(v) = bound_value(upper_row) {
							params.push(Box::new(v));
						}
						if let Some(v) = last_row {
							params.push(Box::new(v));
						}
						params.push(Box::new(version_bytes.clone()));
						params.push(Box::new(limit_i64));
						let flat: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
						match stmt.query_map(params_from_iter(flat), |row| {
							let hi: i64 = row.get(0)?;
							let lo: i64 = row.get(1)?;
							let r: i64 = row.get(2)?;
							let version_blob: Vec<u8> = row.get(3)?;
							let value: Option<Vec<u8>> = row.get(4)?;
							Ok(RawEntry {
								key: partitioned_key_for(storage_id, hi, lo, r),
								version: version_from_bytes(&version_blob),
								value: value.map(CowVec::new),
							})
						}) {
							Ok(rows) => {
								rows.collect::<SqliteResult<Vec<_>>>().map_err(|e| {
									error!(internal(format!(
										"Failed to read persistent row: {}",
										e
									)))
								})?
							}
							Err(e) if e.to_string().contains("no such table") => {
								cursor.finish_with(RangeStop::AbsentTable);
								return Ok(RangeBatch::empty());
							}
							Err(e) => {
								return Err(error!(internal(format!(
									"Failed to scan persistent range: {}",
									e
								))));
							}
						}
					}
					PartitionedRangeBounds::Open {
						lower,
						upper,
					} => {
						let last_triple =
							cursor.last_key()
								.map(|k| {
									partitioned_ident_of(k.as_slice())
										.map(|ident| {
											(
											partition_half_to_sql(ident.partition_hi()),
											partition_half_to_sql(ident.partition_lo()),
											row_to_sql(ident.row().0),
										)
										})
										.ok_or_else(|| {
											error!(internal(
											"a range cursor does not decode as a \
											 PartitionedRowKey"
												.to_string()
										))
										})
								})
								.transpose()?;
						let sql = build_range_current_sql_partitioned(
							&table_sql.table_name,
							bound_shape_triple(&lower),
							bound_shape_triple(&upper),
							last_triple.is_some(),
							req.descending,
						);
						let mut stmt = match conn.prepare_cached(&sql) {
							Ok(s) => s,
							Err(e) if e.to_string().contains("no such table") => {
								cursor.finish_with(RangeStop::AbsentTable);
								return Ok(RangeBatch::empty());
							}
							Err(e) => {
								return Err(error!(internal(format!(
									"Failed to prepare persistent range: {}",
									e
								))));
							}
						};
						let mut params: Vec<Box<dyn ToSql>> = Vec::new();
						if let Some((hi, lo, r)) = bound_value(lower) {
							params.push(Box::new(hi));
							params.push(Box::new(lo));
							params.push(Box::new(r));
						}
						if let Some((hi, lo, r)) = bound_value(upper) {
							params.push(Box::new(hi));
							params.push(Box::new(lo));
							params.push(Box::new(r));
						}
						if let Some((hi, lo, r)) = last_triple {
							params.push(Box::new(hi));
							params.push(Box::new(lo));
							params.push(Box::new(r));
						}
						params.push(Box::new(version_bytes.clone()));
						params.push(Box::new(limit_i64));
						let flat: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
						match stmt.query_map(params_from_iter(flat), |row| {
							let hi: i64 = row.get(0)?;
							let lo: i64 = row.get(1)?;
							let r: i64 = row.get(2)?;
							let version_blob: Vec<u8> = row.get(3)?;
							let value: Option<Vec<u8>> = row.get(4)?;
							Ok(RawEntry {
								key: partitioned_key_for(storage_id, hi, lo, r),
								version: version_from_bytes(&version_blob),
								value: value.map(CowVec::new),
							})
						}) {
							Ok(rows) => {
								rows.collect::<SqliteResult<Vec<_>>>().map_err(|e| {
									error!(internal(format!(
										"Failed to read persistent row: {}",
										e
									)))
								})?
							}
							Err(e) if e.to_string().contains("no such table") => {
								cursor.finish_with(RangeStop::AbsentTable);
								return Ok(RangeBatch::empty());
							}
							Err(e) => {
								return Err(error!(internal(format!(
									"Failed to scan persistent range: {}",
									e
								))));
							}
						}
					}
				}
			}
		};
		let page_was_full = raw.len() >= req.batch_size;
		let last_scanned = raw.last().map(|e| e.key.clone());
		record_page(raw.len() as u64, raw.iter().filter(|e| e.value.is_none()).count() as u64);
		let entries: Vec<RawEntry> = raw.into_iter().filter(|e| req.scope.contains(e.version)).collect();

		if let Some(last) = last_scanned {
			cursor.advance(last);
		}
		if !page_was_full {
			cursor.finish_with(RangeStop::Scanned);
		}

		let has_more = !cursor.is_exhausted();
		Ok(RangeBatch {
			entries,
			has_more,
		})
	}
}

fn any_current_row(conn: &Connection) -> bool {
	let mut stmt = conn
		.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
		.expect("persistent table listing could not be prepared");
	let names: Vec<String> = stmt
		.query_map([], |row| row.get::<_, String>(0))
		.expect("persistent table listing failed")
		.collect::<SqliteResult<Vec<String>>>()
		.expect("persistent table name could not be read");
	drop(stmt);

	for name in names {
		if current_table_name_to_entry(&name).is_none() {
			continue;
		}
		let exists: i64 = conn
			.query_row(&build_current_exists_sql(&name), [], |row| row.get(0))
			.expect("persistent existence probe failed");
		if exists != 0 {
			return true;
		}
	}
	false
}

fn highest_current_version(conn: &Connection) -> u64 {
	let mut stmt = conn
		.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
		.expect("persistent table listing could not be prepared");
	let names: Vec<String> = stmt
		.query_map([], |row| row.get::<_, String>(0))
		.expect("persistent table listing failed")
		.collect::<SqliteResult<Vec<String>>>()
		.expect("persistent table name could not be read");
	drop(stmt);

	let mut highest = 0u64;
	for name in names {
		if current_table_name_to_entry(&name).is_none() {
			continue;
		}
		let blob: Option<Vec<u8>> = conn
			.query_row(&build_max_version_sql(&name), [], |row| row.get(0))
			.expect("persistent high water probe failed");
		if let Some(blob) = blob {
			highest = highest.max(version_from_bytes(&blob).0);
		}
	}
	highest
}

fn source_storage(table: EntryKind) -> Option<StorageId> {
	match table {
		EntryKind::Source(storage, _) | EntryKind::PartitionedSource(storage, _) => Some(storage),
		EntryKind::Multi => None,
	}
}

fn push_key_params(schema: SqliteSchema, key: &[u8], boxed: &mut Vec<Box<dyn ToSql>>) -> Result<()> {
	match schema {
		SqliteSchema::Blob => {
			boxed.push(Box::new(key.to_vec()));
		}
		SqliteSchema::Row => {
			let ident = row_ident_of(key).ok_or_else(|| {
				error!(internal(
					"a row-schema table received a key that does not decode as a RowKey"
						.to_string()
				))
			})?;
			boxed.push(Box::new(row_to_sql(ident.row().0)));
		}
		SqliteSchema::Partitioned => {
			let ident = partitioned_ident_of(key).ok_or_else(|| {
				error!(internal(
					"a partitioned-schema table received a key that does not decode as a \
					 PartitionedRowKey"
						.to_string()
				))
			})?;
			boxed.push(Box::new(partition_half_to_sql(ident.partition_hi())));
			boxed.push(Box::new(partition_half_to_sql(ident.partition_lo())));
			boxed.push(Box::new(row_to_sql(ident.row().0)));
		}
	}
	Ok(())
}

enum ReturnedKey {
	Blob(Vec<u8>),
	Row(i64),
	Partitioned(i64, i64, i64),
}

impl ReturnedKey {
	fn into_encoded_key(self, storage: Option<StorageId>) -> EncodedKey {
		match self {
			ReturnedKey::Blob(bytes) => EncodedKey::new(bytes),
			ReturnedKey::Row(row) => row_key_for(
				storage.expect("a row-schema table's entry kind always carries a storage id"),
				row,
			),
			ReturnedKey::Partitioned(hi, lo, row) => partitioned_key_for(
				storage.expect("a partitioned-schema table's entry kind always carries a storage id"),
				hi,
				lo,
				row,
			),
		}
	}
}

fn key_ints(schema: SqliteSchema, key: &[u8]) -> Option<Vec<i64>> {
	match schema {
		SqliteSchema::Blob => None,
		SqliteSchema::Row => row_ident_of(key).map(|ident| vec![row_to_sql(ident.row().0)]),
		SqliteSchema::Partitioned => partitioned_ident_of(key).map(|ident| {
			vec![
				partition_half_to_sql(ident.partition_hi()),
				partition_half_to_sql(ident.partition_lo()),
				row_to_sql(ident.row().0),
			]
		}),
	}
}

fn read_returned_key(schema: SqliteSchema, row: &Row) -> SqliteResult<ReturnedKey> {
	match schema {
		SqliteSchema::Blob => Ok(ReturnedKey::Blob(row.get::<_, Vec<u8>>(0)?)),
		SqliteSchema::Row => Ok(ReturnedKey::Row(row.get::<_, i64>(0)?)),
		SqliteSchema::Partitioned => Ok(ReturnedKey::Partitioned(row.get(0)?, row.get(1)?, row.get(2)?)),
	}
}

fn expiry_stamp(table: EntryKind, value: Option<&CowVec<u8>>) -> Option<DateTime> {
	match (table, value) {
		(EntryKind::Source(_, _) | EntryKind::PartitionedSource(_, _), Some(row))
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

fn bound_shape_i64(b: &Bound<i64>) -> Bound<()> {
	match b {
		Bound::Included(_) => Bound::Included(()),
		Bound::Excluded(_) => Bound::Excluded(()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

fn bound_shape_triple(b: &Bound<(i64, i64, i64)>) -> Bound<()> {
	match b {
		Bound::Included(_) => Bound::Included(()),
		Bound::Excluded(_) => Bound::Excluded(()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

fn bound_value<T: Copy>(b: Bound<T>) -> Option<T> {
	match b {
		Bound::Included(v) | Bound::Excluded(v) => Some(v),
		Bound::Unbounded => None,
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

pub(crate) struct RowRangeChunkRequest<'a> {
	pub(crate) table: EntryKind,
	pub(crate) start: Bound<&'a StorageRowKey>,
	pub(crate) end: Bound<&'a StorageRowKey>,
	pub(crate) scope: MultiVersionScope,
	pub(crate) batch_size: usize,
	pub(crate) descending: bool,
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

		let mut boxed: Vec<Box<dyn ToSql>> = Vec::with_capacity(table_sql.schema.key_column_count());
		if push_key_params(table_sql.schema, key, &mut boxed).is_err() {
			return Ok(VersionedGetResult::NotFound);
		}
		let flat: Vec<&dyn ToSql> = boxed.iter().map(|p| p.as_ref()).collect();

		let result = match conn.prepare_cached(&table_sql.get_sql) {
			Ok(mut stmt) => stmt.query_row(params_from_iter(flat), |row| {
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

		let table_sql = self.table_sql(table);
		if table_sql.schema == SqliteSchema::Blob {
			return self.get_many_blob(&table_sql, keys, version, &mut out).map(|()| out);
		}

		let key_columns = table_sql.schema.key_column_count();
		let mut index: HashMap<Vec<i64>, usize> = HashMap::with_capacity(keys.len());
		let mut per_key_params: Vec<Vec<i64>> = Vec::with_capacity(keys.len());
		for (i, &k) in keys.iter().enumerate() {
			let params = key_ints(table_sql.schema, k).ok_or_else(|| {
				error!(internal(
					"a get_many key does not decode under its own table's narrow schema"
						.to_string()
				))
			})?;
			debug_assert_eq!(params.len(), key_columns);
			index.insert(params.clone(), i);
			per_key_params.push(params);
		}

		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(out);
		};

		for chunk in per_key_params.chunks(GET_MANY_CHUNK) {
			let bucket = bucket_key_count(chunk.len());
			let sql = match table_sql.schema {
				SqliteSchema::Row => build_get_many_current_sql_row(&table_sql.table_name, bucket),
				SqliteSchema::Partitioned => {
					build_get_many_current_sql_partitioned(&table_sql.table_name, bucket)
				}
				SqliteSchema::Blob => unreachable!("blob schema handled by get_many_blob"),
			};
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

			let pad = chunk[0].clone();
			let mut padded: Vec<i64> = Vec::with_capacity(bucket * key_columns);
			for p in chunk {
				padded.extend_from_slice(p);
			}
			for _ in chunk.len()..bucket {
				padded.extend_from_slice(&pad);
			}
			let mut rows = stmt
				.query(params_from_iter(padded))
				.map_err(|e| error!(internal(format!("Failed to query persistent get_many: {}", e))))?;

			while let Some(row) = rows.next().map_err(|e| {
				error!(internal(format!("Failed to read persistent get_many row: {}", e)))
			})? {
				let key_params: Vec<i64> = (0..key_columns)
					.map(|c| {
						row.get::<_, i64>(c).map_err(|e| {
							error!(internal(format!(
								"Failed to read persistent get_many key: {}",
								e
							)))
						})
					})
					.collect::<Result<Vec<_>>>()?;
				let Some(&i) = index.get(&key_params) else {
					continue;
				};
				let version_bytes: Vec<u8> = row.get(key_columns).map_err(|e| {
					error!(internal(format!("Failed to read persistent get_many version: {}", e)))
				})?;
				let stored_version = version_from_bytes(&version_bytes);
				if stored_version > version {
					continue;
				}
				let value: Option<Vec<u8>> = row.get(key_columns + 1).map_err(|e| {
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

	fn get_many_blob(
		&self,
		table_sql: &TableSql,
		keys: &[&[u8]],
		version: CommitVersion,
		out: &mut [VersionedGetResult],
	) -> Result<()> {
		let index: HashMap<&[u8], usize> = keys.iter().enumerate().map(|(i, &k)| (k, i)).collect();
		let guard = self.inner.readers.acquire();
		let Some(conn) = guard.as_ref() else {
			return Ok(());
		};

		for chunk in keys.chunks(GET_MANY_CHUNK) {
			let bucket = bucket_key_count(chunk.len());
			let sql = build_get_many_current_sql(&table_sql.table_name, bucket);
			let mut stmt = match conn.prepare_cached(&sql) {
				Ok(stmt) => stmt,
				Err(e) if e.to_string().contains("no such table") => return Ok(()),
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

		Ok(())
	}
}

impl TierStorage for SqlitePersistentStorage {
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult> {
		match table {
			EntryKind::Source(_, _) => self.get_source(table, key, version),
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
			EntryKind::Source(_, _) => self.get_many_source(table, keys, version),
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
}

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

	use reifydb_core::{
		interface::{
			catalog::{id::TableId, storage::StorageId},
			store::EntryLayout,
		},
		key::{
			row::{PartitionedRowKey, RowKey, RowKeyRange},
			typed::key::Key,
		},
	};
	use reifydb_value::value::{partition::Partition, row_number::RowNumber};

	use super::*;
	use crate::tier::persistent::sqlite::schema::row_from_sql;

	// `table()` backs the narrow row schema, so every key built against it must decode as a RowKey.
	fn table() -> EntryKind {
		EntryKind::Source(StorageId::Table(TableId(1)), EntryLayout::Row)
	}

	fn key(n: u64) -> EncodedKey {
		RowKey::encoded(StorageId::Table(TableId(1)), RowNumber(n))
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
			.map(|(key, _)| RowKey::decode(&key).unwrap().row.0)
			.collect()
	}

	fn partitioned_expired_at(s: &SqlitePersistentStorage, kind: EntryKind, cutoff: u64) -> Vec<u64> {
		s.expired_keys(kind, at(cutoff), None, 100)
			.unwrap()
			.into_iter()
			.map(|(key, _)| PartitionedRowKey::decode(&key).unwrap().row.0)
			.collect()
	}

	fn reap(s: &SqlitePersistentStorage, keys: &[u64]) -> u64 {
		let keys: Vec<EncodedKey> = keys.iter().map(|n| key(*n)).collect();
		s.delete_keys(table(), &keys).unwrap()
	}

	fn stored_keys(s: &SqlitePersistentStorage) -> Vec<u64> {
		// The narrow row schema stores the row number descending in `key`, never an encoded RowKey,
		// so reading the column raw would report the inverted integer rather than the row.
		let table_name = s.table_sql(table()).table_name.clone();
		let guard = s.inner.conn.lock();
		let conn = guard.as_ref().expect("write connection is present");
		let mut stmt = conn.prepare(&format!("SELECT key FROM \"{}\" ORDER BY key", table_name)).unwrap();
		let keys: Vec<u64> = stmt
			.query_map([], |row| row.get::<_, i64>(0))
			.unwrap()
			.map(|key| row_from_sql(key.unwrap()))
			.collect();
		keys
	}

	fn visible(s: &SqlitePersistentStorage, k: &EncodedKey) -> bool {
		s.get(table(), k.as_slice(), CommitVersion(u64::MAX)).unwrap().value().is_some()
	}

	#[test]
	fn a_range_does_not_fetch_deleted_rows_because_the_delete_removed_them() {
		// The timer probe asks for one row and used to receive every dead row in the prefix,
		// because LIMIT applies after the WHERE: measured at 1,837 rows fetched per probe, 100%
		// of them dead. A removal now deletes the row outright rather than rewriting it to a
		// none value, so the dead rows are not there to be scanned past in the first place.
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
		assert_eq!(
			s.count_current(table()).unwrap(),
			1,
			"the 50 removals must delete their rows outright; rewriting them to a none value \
			 leaves 50 dead rows for every later scan to page through and discard"
		);

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
			"a deleted key must not surface, and the one live row must"
		);
		assert_eq!(scanned.fetched, 1, "the 50 deleted rows must never cross into Rust");
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
			vec![key(4), key(3)],
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
	fn create_table_leaves_the_version_column_unindexed() {
		// No query plan selects a bare version index, so recreating it costs a b-tree write per row on every
		// source table for nothing.
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
			!indices.contains(&format!("{table_name}__version")),
			"the bare version index must not be created; it is written on every row and read by no query plan, got {indices:?}"
		);
		assert!(
			!indices.contains(&format!("{table_name}__tombstone")),
			"the tombstone index must not be created; a removal deletes its row outright, so nothing \
			 writes a valueless row for it to index, got {indices:?}"
		);
		assert!(
			!indices.iter().any(|n| n.ends_with("__created_nanos") || n.ends_with("__updated_nanos")),
			"the dropped timestamp indices must not be recreated, got {indices:?}"
		);
	}
	#[test]
	fn a_key_written_again_after_a_removal_comes_back() {
		// A removal must leave no row behind, or the parked one wins the CAS and strands the key absent.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(2), HashMap::from([(table(), vec![(key(1), None)])])).unwrap();

		s.set(
			CommitVersion(3),
			HashMap::from([(table(), vec![(key(1), Some(row(b"back"))), (key(2), Some(row(b"new")))])]),
		)
		.unwrap();

		assert!(visible(&s, &key(1)), "a fresh write after a removal must land");
		assert!(visible(&s, &key(2)), "an unrelated key in the same batch must land too");
	}

	#[test]
	fn a_chunked_upsert_batch_reports_exactly_the_keys_that_won_their_cas() {
		// a key marked accepted despite losing its CAS is evicted from memory while never persisted
		let (s, _guard) = SqlitePersistentStorage::in_memory();

		let evens: Vec<_> = (0..170u64).step_by(2).map(|i| (key(i), Some(row(b"seed-even")))).collect();
		let odds: Vec<_> = (1..170u64).step_by(2).map(|i| (key(i), Some(row(b"seed-odd")))).collect();
		s.set_collecting_accepted(CommitVersion(200), HashMap::from([(table(), evens)])).unwrap();
		s.set_collecting_accepted(CommitVersion(50), HashMap::from([(table(), odds)])).unwrap();

		let attempt: Vec<_> = (0..170u64).map(|i| (key(i), Some(row(b"attempt")))).collect();
		let accepted =
			s.set_collecting_accepted(CommitVersion(150), HashMap::from([(table(), attempt)])).unwrap();

		let mut accepted_ids: Vec<u64> = accepted.iter().map(|k| RowKey::decode(k).unwrap().row.0).collect();
		accepted_ids.sort();
		let expected_odds: Vec<u64> = (1..170u64).step_by(2).collect();
		assert_eq!(
			accepted_ids, expected_odds,
			"only the keys whose stored version (50) lost to this batch's version (150) may be reported \
			 accepted"
		);

		for i in (0..170u64).step_by(2) {
			let value = s.get(table(), key(i).as_slice(), CommitVersion(u64::MAX)).unwrap().value();
			assert_eq!(
				value.as_ref().map(|v| v.as_slice()),
				Some(&b"seed-even"[..]),
				"key {i} lost its CAS (stored version 200 >= batch version 150) and must be untouched"
			);
		}
		for i in (1..170u64).step_by(2) {
			let value = s.get(table(), key(i).as_slice(), CommitVersion(u64::MAX)).unwrap().value();
			assert_eq!(
				value.as_ref().map(|v| v.as_slice()),
				Some(&b"attempt"[..]),
				"key {i} won its CAS (stored version 50 < batch version 150) and must carry this batch's \
				 value"
			);
		}
	}

	#[test]
	fn a_write_below_a_removals_version_still_lands_because_versions_arrive_out_of_order() {
		// Batches reach this tier unordered, so a lower version after a removal is an ordinary write.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(CommitVersion(5), HashMap::from([(table(), vec![(key(1), None)])])).unwrap();

		s.set(CommitVersion(4), HashMap::from([(table(), vec![(key(1), Some(row(b"earlier")))])])).unwrap();

		assert!(visible(&s, &key(1)), "the row must land: nothing on disk outranks it after the removal");
	}

	#[test]
	fn persist_sweep_errors_when_storage_is_shut_down() {
		// The sweep hands over the only copy of a row: the commit buffer drops it on the strength of this
		// call returning Ok. A shut-down storage that reported success would lose the row silently.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.shutdown();

		let batches = vec![(CommitVersion(1), HashMap::from([(table(), vec![(key(1), Some(row(b"v")))])]))];

		assert!(
			s.persist_sweep(batches).is_err(),
			"a shut-down persistent tier must refuse the sweep loudly so the buffer is not dropped"
		);
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
			seen.push(RowKey::decode(&k).unwrap().row.0);
			cursor = Some((a, k));
		}

		assert_eq!(
			seen,
			vec![2, 1, 3],
			"threading the cursor must walk every candidate exactly once, in order: rows 1 and 2 share a \
			 stamp, so the key breaks the tie descending, and only then does the later stamp follow"
		);
	}

	#[test]
	fn delete_keys_removes_the_row_outright_leaving_no_tombstone() {
		// A reaped row must vanish, not become a tombstone the reaper would then have to clear again.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(
			CommitVersion(1),
			HashMap::from([(
				table(),
				vec![
					(key(1), Some(stamped(100))),
					(key(2), Some(stamped(200))),
					(key(3), Some(stamped(500))),
				],
			)]),
		)
		.unwrap();

		assert_eq!(reap(&s, &[1, 2]), 2, "every named key must be removed");
		assert_eq!(stored_keys(&s), vec![3], "a reaped key must leave no row behind, not even a tombstone");
		assert_eq!(
			expired_at(&s, table(), 1_000),
			vec![3],
			"a reaped key must not resurface as an expiry candidate"
		);
	}

	#[test]
	fn row_schema_full_scan_preserves_the_blob_schemas_descending_ascending_order() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		let t = table();
		let mut writes = Vec::new();
		for n in 1u64..=5 {
			writes.push((key(n), Some(row(b"x"))));
		}
		s.set(CommitVersion(1), HashMap::from([(t, writes)])).unwrap();

		let range = RowKey::full_scan(StorageId::Table(TableId(1)));
		let (start, end) = match (&range.start, &range.end) {
			(Bound::Included(s), Bound::Included(e)) => (s.as_slice(), e.as_slice()),
			_ => panic!("expected included bounds"),
		};

		let mut cursor = RangeCursor::default();
		let batch = s
			.range_next(
				t,
				&mut cursor,
				Bound::Included(start),
				Bound::Included(end),
				MultiVersionScope::AsOf {
					read: CommitVersion(10),
				},
				1024,
			)
			.unwrap();
		let forward: Vec<u64> = batch.entries.iter().map(|e| RowKey::decode(&e.key).unwrap().row.0).collect();
		assert_eq!(
			forward,
			vec![5, 4, 3, 2, 1],
			"range_next must reproduce the old byte-ascending-is-row-descending order of the BLOB schema"
		);

		let mut cursor2 = RangeCursor::default();
		let batch2 = s
			.range_rev_next(
				t,
				&mut cursor2,
				Bound::Included(start),
				Bound::Included(end),
				MultiVersionScope::AsOf {
					read: CommitVersion(10),
				},
				1024,
			)
			.unwrap();
		let reverse: Vec<u64> = batch2.entries.iter().map(|e| RowKey::decode(&e.key).unwrap().row.0).collect();
		assert_eq!(
			reverse,
			vec![1, 2, 3, 4, 5],
			"range_rev_next must reproduce the old byte-descending-is-row-ascending order of the BLOB schema"
		);
	}

	#[test]
	fn row_schema_scan_range_mixes_a_full_cursor_bound_with_a_prefix_only_end_bound() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		let t = table();
		let mut writes = Vec::new();
		for n in 1u64..=5 {
			writes.push((key(n), Some(row(b"x"))));
		}
		s.set(CommitVersion(1), HashMap::from([(t, writes)])).unwrap();

		let range = RowKeyRange::scan_range(StorageId::Table(TableId(1)), Some(&key(4)));
		let (start, end) = match (&range.start, &range.end) {
			(Bound::Excluded(s), Bound::Included(e)) => (s.as_slice(), e.as_slice()),
			other => panic!(
				"expected an excluded cursor start and an included prefix-only end, got {other:?}"
			),
		};

		let mut cursor = RangeCursor::default();
		let batch = s
			.range_next(
				t,
				&mut cursor,
				Bound::Excluded(start),
				Bound::Included(end),
				MultiVersionScope::AsOf {
					read: CommitVersion(10),
				},
				1024,
			)
			.unwrap();
		let got: Vec<u64> = batch.entries.iter().map(|e| RowKey::decode(&e.key).unwrap().row.0).collect();
		assert_eq!(
			got,
			vec![3, 2, 1],
			"resuming after row 4 must yield every remaining row, oldest scan order preserved"
		);
	}

	#[test]
	fn row_schema_expired_keys_scan_finds_rows() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(
			CommitVersion(1),
			HashMap::from([(table(), vec![(key(1), Some(stamped(100))), (key(2), Some(stamped(200)))])]),
		)
		.unwrap();

		assert_eq!(
			expired_at(&s, table(), 150),
			vec![1],
			"the narrow row schema must still surface expiry candidates through its own key column"
		);
	}

	fn partitioned_table() -> EntryKind {
		EntryKind::PartitionedSource(StorageId::Table(TableId(2)), EntryLayout::Row)
	}

	fn partitioned_key(partition: u128, n: u64) -> EncodedKey {
		PartitionedRowKey::encoded(StorageId::Table(TableId(2)), Partition(partition), RowNumber(n))
	}

	#[test]
	fn partitioned_schema_get_after_insert_is_exact() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		let k = partitioned_key(7, 3);
		s.set(CommitVersion(1), HashMap::from([(partitioned_table(), vec![(k.clone(), Some(row(b"v")))])]))
			.unwrap();

		let got = s.get(partitioned_table(), k.as_slice(), CommitVersion(u64::MAX)).unwrap();
		assert_eq!(got.value().as_ref().map(|v| v.as_slice()), Some(&b"v"[..]));
	}

	#[test]
	fn partitioned_schema_full_scan_with_both_bounds_prefix_only() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		let t = partitioned_table();
		s.set(
			CommitVersion(1),
			HashMap::from([(
				t,
				vec![
					(partitioned_key(1, 1), Some(row(b"a"))),
					(partitioned_key(1, 2), Some(row(b"b"))),
					(partitioned_key(2, 1), Some(row(b"c"))),
				],
			)]),
		)
		.unwrap();

		let range = PartitionedRowKey::full_scan(StorageId::Table(TableId(2)));
		let (start, end) = match (&range.start, &range.end) {
			(Bound::Included(s), Bound::Included(e)) => (s.as_slice(), e.as_slice()),
			other => panic!("expected two prefix-only included bounds, got {other:?}"),
		};

		let mut cursor = RangeCursor::default();
		let batch = s
			.range_next(
				t,
				&mut cursor,
				Bound::Included(start),
				Bound::Included(end),
				MultiVersionScope::AsOf {
					read: CommitVersion(10),
				},
				1024,
			)
			.unwrap();
		assert_eq!(batch.entries.len(), 3, "a full-table scan across partitions must reach every row");
	}

	#[test]
	fn partitioned_schema_scan_range_mixes_a_full_cursor_bound_with_a_prefix_only_end_bound() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		let t = partitioned_table();
		s.set(
			CommitVersion(1),
			HashMap::from([(
				t,
				vec![
					(partitioned_key(1, 1), Some(row(b"a"))),
					(partitioned_key(1, 2), Some(row(b"b"))),
					(partitioned_key(2, 1), Some(row(b"c"))),
				],
			)]),
		)
		.unwrap();

		// Forward order visits the largest partition/row tuple first, so a real cursor is that tuple.
		let range = PartitionedRowKey::scan_range(StorageId::Table(TableId(2)), Some(&partitioned_key(2, 1)));
		let (start, end) = match (&range.start, &range.end) {
			(Bound::Excluded(s), Bound::Included(e)) => (s.as_slice(), e.as_slice()),
			other => panic!(
				"expected an excluded cursor start and an included prefix-only end, got {other:?}"
			),
		};

		let mut cursor = RangeCursor::default();
		let batch = s
			.range_next(
				t,
				&mut cursor,
				Bound::Excluded(start),
				Bound::Included(end),
				MultiVersionScope::AsOf {
					read: CommitVersion(10),
				},
				1024,
			)
			.unwrap();
		assert_eq!(batch.entries.len(), 2, "resuming past the first row must still reach the other two");
	}
	#[test]
	fn partitioned_schema_expired_keys_scan_finds_rows() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		s.set(
			CommitVersion(1),
			HashMap::from([(
				partitioned_table(),
				vec![
					(partitioned_key(1, 1), Some(stamped(100))),
					(partitioned_key(1, 2), Some(stamped(200))),
				],
			)]),
		)
		.unwrap();

		assert_eq!(
			partitioned_expired_at(&s, partitioned_table(), 150),
			vec![1],
			"the narrow partitioned schema must still surface expiry candidates through its own columns"
		);
	}

	#[test]
	fn partitioned_expired_keys_resume_breaks_a_shared_stamp_tie_in_encoded_key_order() {
		// The row schema pins this tie-break; the partitioned schema did not, so reversing its cursor
		// predicate and its ORDER BY together passed every test while walking candidates backwards.
		// The evictor threads this cursor across ticks, so the index must hand back candidates in the
		// same order the key space has, or a candidate it cannot remove strands the rows behind it.
		// The two partitions straddle the sign bit, which is where the halves last disagreed.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		let low = 1u128;
		let high = (1u128 << 127) | 3;
		s.set(
			CommitVersion(1),
			HashMap::from([(
				partitioned_table(),
				vec![
					(partitioned_key(low, 1), Some(stamped(100))),
					(partitioned_key(low, 2), Some(stamped(100))),
					(partitioned_key(high, 1), Some(stamped(100))),
					(partitioned_key(low, 3), Some(stamped(200))),
				],
			)]),
		)
		.unwrap();

		let mut tied: Vec<(u128, u64)> = vec![(low, 1), (low, 2), (high, 1)];
		tied.sort_by(|a, b| partitioned_key(a.0, a.1).as_slice().cmp(partitioned_key(b.0, b.1).as_slice()));
		let mut expected = tied.clone();
		expected.push((low, 3));

		let mut seen = Vec::new();
		let mut cursor: Option<(DateTime, EncodedKey)> = None;
		loop {
			let batch = s
				.expired_keys(
					partitioned_table(),
					at(1_000),
					cursor.as_ref().map(|(a, k)| (*a, k.as_slice())),
					1,
				)
				.unwrap();
			let Some((k, a)) = batch.last().cloned() else {
				break;
			};
			let decoded = PartitionedRowKey::decode(&k).unwrap();
			seen.push((decoded.partition.0, decoded.row.0));
			cursor = Some((a, k));
		}

		assert_eq!(
			seen, expected,
			"threading the cursor must walk every candidate once, oldest stamp first and ties broken \
			 exactly as the encoded keys sort"
		);
		assert_eq!(
			seen[0],
			(high, 1),
			"the larger partition encodes lower, so it must lead the tie, not trail it"
		);
	}

	#[test]
	fn partitioned_schema_paginated_full_scan_reaches_every_row_across_many_partitions() {
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		let t = partitioned_table();
		let mut writes = Vec::new();
		let mut seed = 0x9E3779B97F4A7C15u64;
		for _ in 1u128..=64 {
			// xorshift64*, mimicking the scattered sign bits real xxh3_128 partition hashes produce.
			seed ^= seed << 13;
			seed ^= seed >> 7;
			seed ^= seed << 17;
			let hi = seed as u128;
			seed ^= seed << 13;
			seed ^= seed >> 7;
			seed ^= seed << 17;
			let lo = seed as u128;
			let p = (hi << 64) | lo;
			writes.push((partitioned_key(p, 1), Some(row(b"a"))));
			writes.push((partitioned_key(p, 2), Some(row(b"b"))));
		}
		s.set(CommitVersion(1), HashMap::from([(t, writes)])).unwrap();

		let range = PartitionedRowKey::full_scan(StorageId::Table(TableId(2)));
		let (start, end) = match (&range.start, &range.end) {
			(Bound::Included(s), Bound::Included(e)) => (s.as_slice(), e.as_slice()),
			other => panic!("expected two prefix-only included bounds, got {other:?}"),
		};

		let mut cursor = RangeCursor::default();
		let mut total = 0usize;
		loop {
			let batch = s
				.range_next(
					t,
					&mut cursor,
					Bound::Included(start),
					Bound::Included(end),
					MultiVersionScope::AsOf {
						read: CommitVersion(10),
					},
					4,
				)
				.unwrap();
			total += batch.entries.len();
			if cursor.is_exhausted() {
				break;
			}
		}
		assert_eq!(total, 128, "a paginated full-table scan with a small batch size must reach every row");
	}

	#[test]
	fn partitioned_schema_paginated_scan_yields_exactly_the_encoded_key_order_in_both_directions() {
		// The count-only pagination tests pass under a reversal that flips the ORDER BY and the cursor
		// predicate together, because every row is still reached, just backwards. Callers merge this
		// stream with the in-memory tiers on encoded-key order, so the narrow partitioned columns must
		// reproduce that order exactly, not merely reach every row. The partitions below straddle the
		// sign bit in both halves, so a half that inverts alone or not at all reorders the sequence.
		let (s, _guard) = SqlitePersistentStorage::in_memory();
		let t = partitioned_table();
		let partitions: [u128; 4] =
			[1, (1u128 << 64) | 7, (1u128 << 127) | 3, (1u128 << 127) | (1u128 << 63) | 9];
		let mut written = Vec::new();
		let mut writes = Vec::new();
		for p in partitions {
			for r in 1u64..=3 {
				let k = partitioned_key(p, r);
				written.push((k.clone(), p, r));
				writes.push((k, Some(row(b"a"))));
			}
		}
		s.set(CommitVersion(1), HashMap::from([(t, writes)])).unwrap();

		let mut forward_expected = written.clone();
		forward_expected.sort_by(|a, b| a.0.as_slice().cmp(b.0.as_slice()));
		let forward_expected: Vec<(u128, u64)> = forward_expected.iter().map(|(_, p, r)| (*p, *r)).collect();
		let mut reverse_expected = forward_expected.clone();
		reverse_expected.reverse();

		let range = PartitionedRowKey::full_scan(StorageId::Table(TableId(2)));
		let (start, end) = match (&range.start, &range.end) {
			(Bound::Included(s), Bound::Included(e)) => (s.as_slice(), e.as_slice()),
			other => panic!("expected two prefix-only included bounds, got {other:?}"),
		};

		let forward = paginate_partitioned(&s, t, start, end, false);
		assert_eq!(
			forward, forward_expected,
			"a paginated forward scan must walk the encoded keys ascending, partition then row"
		);

		let reverse = paginate_partitioned(&s, t, start, end, true);
		assert_eq!(
			reverse, reverse_expected,
			"a paginated reverse scan must walk the same encoded keys descending, the exact mirror"
		);
	}

	fn paginate_partitioned(
		s: &SqlitePersistentStorage,
		t: EntryKind,
		start: &[u8],
		end: &[u8],
		reverse: bool,
	) -> Vec<(u128, u64)> {
		// A batch of 2 against 12 rows forces the cursor to carry the direction across six pages, so a
		// cursor predicate pointing the wrong way drops or repeats rows instead of paging cleanly.
		let mut cursor = RangeCursor::default();
		let mut out = Vec::new();
		loop {
			let scope = MultiVersionScope::AsOf {
				read: CommitVersion(10),
			};
			let batch = if reverse {
				s.range_rev_next(t, &mut cursor, Bound::Included(start), Bound::Included(end), scope, 2)
			} else {
				s.range_next(t, &mut cursor, Bound::Included(start), Bound::Included(end), scope, 2)
			}
			.unwrap();
			for entry in &batch.entries {
				let decoded = PartitionedRowKey::decode(&entry.key).unwrap();
				out.push((decoded.partition.0, decoded.row.0));
			}
			if cursor.is_exhausted() {
				return out;
			}
		}
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
}
