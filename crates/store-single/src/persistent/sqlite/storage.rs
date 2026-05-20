// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, sync::Arc};

use reifydb_core::{encoded::key::EncodedKey, internal_error};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sqlite::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	pragma,
};
use reifydb_type::{Result, util::cowvec::CowVec};
use rusqlite::{
	Connection, Error::QueryReturnedNoRows, Result as SqliteResult, ToSql, Transaction as SqliteTransaction, params,
};
use tracing::instrument;

use super::query::build_range_query;
use crate::tier::{RangeBatch, RangeCursor, RawEntry, TierBackend, TierStorage};

const TABLE_NAME: &str = "entries";

#[derive(Clone)]
pub struct SqlitePersistentStorage {
	inner: Arc<SqlitePersistentStorageInner>,
}

struct SqlitePersistentStorageInner {
	conn: Mutex<Connection>,
}

impl SqlitePersistentStorage {
	#[instrument(name = "store::single::persistent::new", level = "debug", skip(config), fields(
		db_path = ?config.path,
		page_size = config.page_size,
		journal_mode = %config.journal_mode.as_str()
	))]
	pub fn new(config: SqliteConfig) -> Self {
		let db_path = resolve_db_path(config.path.clone(), "persistent.db");
		let flags = convert_flags(&config.flags);

		let conn = connect(&db_path, flags).expect("Failed to connect to database");
		pragma::apply(&conn, &config).expect("Failed to configure SQLite pragmas");

		Self {
			inner: Arc::new(SqlitePersistentStorageInner {
				conn: Mutex::new(conn),
			}),
		}
	}

	pub fn in_memory() -> Self {
		Self::new(SqliteConfig::in_memory())
	}
}

impl TierStorage for SqlitePersistentStorage {
	#[instrument(name = "store::single::persistent::get", level = "trace", skip(self, key), fields(key_len = key.len()))]
	fn get(&self, key: &[u8]) -> Result<Option<CowVec<u8>>> {
		let conn = self.inner.conn.lock();

		let result = conn.query_row(
			&format!("SELECT value FROM \"{}\" WHERE key = ?1", TABLE_NAME),
			params![key],
			|row| row.get::<_, Option<Vec<u8>>>(0),
		);

		match result {
			Ok(Some(value)) => Ok(Some(CowVec::new(value))),
			Ok(None) => Ok(None),
			Err(QueryReturnedNoRows) => Ok(None),
			Err(e) if e.to_string().contains("no such table") => Ok(None),
			Err(e) => Err(internal_error!("Failed to get: {}", e)),
		}
	}

	#[instrument(name = "store::single::persistent::get_with_tombstone", level = "trace", skip(self, key), fields(key_len = key.len()))]
	fn get_with_tombstone(&self, key: &[u8]) -> Result<Option<Option<CowVec<u8>>>> {
		let conn = self.inner.conn.lock();

		let result = conn.query_row(
			&format!("SELECT value FROM \"{}\" WHERE key = ?1", TABLE_NAME),
			params![key],
			|row| row.get::<_, Option<Vec<u8>>>(0),
		);

		match result {
			Ok(value) => Ok(Some(value.map(CowVec::new))),
			Err(QueryReturnedNoRows) => Ok(None),
			Err(e) if e.to_string().contains("no such table") => Ok(None),
			Err(e) => Err(internal_error!("Failed to get_with_tombstone: {}", e)),
		}
	}

	#[instrument(name = "store::single::persistent::contains", level = "trace", skip(self, key), fields(key_len = key.len()), ret)]
	fn contains(&self, key: &[u8]) -> Result<bool> {
		let conn = self.inner.conn.lock();

		let result = conn.query_row(
			&format!("SELECT value IS NOT NULL FROM \"{}\" WHERE key = ?1", TABLE_NAME),
			params![key],
			|row| row.get::<_, bool>(0),
		);

		match result {
			Ok(has_value) => Ok(has_value),
			Err(QueryReturnedNoRows) => Ok(false),
			Err(e) if e.to_string().contains("no such table") => Ok(false),
			Err(e) => Err(internal_error!("Failed to check contains: {}", e)),
		}
	}

	#[instrument(name = "store::single::persistent::set", level = "debug", skip(self, entries), fields(entry_count = entries.len()))]
	fn set(&self, entries: Vec<(EncodedKey, Option<CowVec<u8>>)>) -> Result<()> {
		if entries.is_empty() {
			return Ok(());
		}

		let conn = self.inner.conn.lock();
		let tx = conn
			.unchecked_transaction()
			.map_err(|e| internal_error!("Failed to start transaction: {}", e))?;

		let result = insert_entries_in_tx(&tx, TABLE_NAME, &entries);
		if let Err(e) = result {
			if e.to_string().contains("no such table") {
				tx.execute(
					&format!(
						"CREATE TABLE IF NOT EXISTS \"{}\" (
							key BLOB NOT NULL PRIMARY KEY,
							value BLOB
						) WITHOUT ROWID",
						TABLE_NAME
					),
					[],
				)
				.map_err(|e| internal_error!("Failed to create table: {}", e))?;
				insert_entries_in_tx(&tx, TABLE_NAME, &entries)
					.map_err(|e| internal_error!("Failed to insert entries: {}", e))?;
			} else {
				return Err(internal_error!("Failed to insert entries: {}", e));
			}
		}

		tx.commit().map_err(|e| internal_error!("Failed to commit transaction: {}", e))
	}

	#[instrument(name = "store::single::persistent::range_next", level = "trace", skip(self, cursor))]
	fn range_next(
		&self,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let effective_start: Bound<Vec<u8>> = match &cursor.last_key {
			Some(last) => Bound::Excluded(last.as_slice().to_vec()),
			None => bound_to_owned(start),
		};
		let end_owned = bound_to_owned(end);

		let conn = self.inner.conn.lock();

		let start_ref = bound_as_ref(&effective_start);
		let end_ref = bound_as_ref(&end_owned);
		let (query, params) = build_range_query(TABLE_NAME, start_ref, end_ref, false, batch_size + 1);

		let mut stmt = match conn.prepare(&query) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => {
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
			Err(e) => return Err(internal_error!("Failed to prepare query: {}", e)),
		};

		let params_refs: Vec<&dyn ToSql> = params.iter().map(|p| p as &dyn ToSql).collect();

		let entries: Vec<RawEntry> = stmt
			.query_map(params_refs.as_slice(), |row| {
				let key: Vec<u8> = row.get(0)?;
				let value: Option<Vec<u8>> = row.get(1)?;
				Ok(RawEntry {
					key: EncodedKey::new(key),
					value: value.map(CowVec::new),
				})
			})
			.map_err(|e| internal_error!("Failed to query range: {}", e))?
			.filter_map(|r| r.ok())
			.collect();

		let has_more = entries.len() > batch_size;
		let entries = if has_more {
			entries.into_iter().take(batch_size).collect()
		} else {
			entries
		};

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

	#[instrument(name = "store::single::persistent::range_rev_next", level = "trace", skip(self, cursor))]
	fn range_rev_next(
		&self,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let start_owned = bound_to_owned(start);
		let effective_end: Bound<Vec<u8>> = match &cursor.last_key {
			Some(last) => Bound::Excluded(last.as_slice().to_vec()),
			None => bound_to_owned(end),
		};

		let conn = self.inner.conn.lock();

		let start_ref = bound_as_ref(&start_owned);
		let end_ref = bound_as_ref(&effective_end);
		let (query, params) = build_range_query(TABLE_NAME, start_ref, end_ref, true, batch_size + 1);

		let mut stmt = match conn.prepare(&query) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => {
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
			Err(e) => return Err(internal_error!("Failed to prepare query: {}", e)),
		};

		let params_refs: Vec<&dyn ToSql> = params.iter().map(|p| p as &dyn ToSql).collect();

		let entries: Vec<RawEntry> = stmt
			.query_map(params_refs.as_slice(), |row| {
				let key: Vec<u8> = row.get(0)?;
				let value: Option<Vec<u8>> = row.get(1)?;
				Ok(RawEntry {
					key: EncodedKey::new(key),
					value: value.map(CowVec::new),
				})
			})
			.map_err(|e| internal_error!("Failed to query range: {}", e))?
			.filter_map(|r| r.ok())
			.collect();

		let has_more = entries.len() > batch_size;
		let entries = if has_more {
			entries.into_iter().take(batch_size).collect()
		} else {
			entries
		};

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

	#[instrument(name = "store::single::persistent::ensure_table", level = "trace", skip(self))]
	fn ensure_table(&self) -> Result<()> {
		let conn = self.inner.conn.lock();

		conn.execute(
			&format!(
				"CREATE TABLE IF NOT EXISTS \"{}\" (
					key   BLOB NOT NULL PRIMARY KEY,
					value BLOB
				) WITHOUT ROWID",
				TABLE_NAME
			),
			[],
		)
		.map(|_| ())
		.map_err(|e| internal_error!("Failed to ensure table: {}", e))
	}

	#[instrument(name = "store::single::persistent::clear_table", level = "debug", skip(self))]
	fn clear_table(&self) -> Result<()> {
		let conn = self.inner.conn.lock();

		let result = conn.execute(&format!("DELETE FROM \"{}\"", TABLE_NAME), []);

		match result {
			Ok(_) => Ok(()),
			Err(e) if e.to_string().contains("no such table") => Ok(()),
			Err(e) => Err(internal_error!("Failed to clear table: {}", e)),
		}
	}
}

impl TierBackend for SqlitePersistentStorage {}

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

fn insert_entries_in_tx(
	tx: &SqliteTransaction,
	table_name: &str,
	entries: &[(EncodedKey, Option<CowVec<u8>>)],
) -> SqliteResult<()> {
	for (key, value) in entries {
		tx.execute(
			&format!("INSERT OR REPLACE INTO \"{}\" (key, value) VALUES (?1, ?2)", table_name),
			params![key.as_slice(), value.as_ref().map(|v| v.as_slice())],
		)?;
	}
	Ok(())
}
