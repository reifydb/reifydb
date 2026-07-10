// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{interface::catalog::id::ColumnSnapshotId, internal_error};
use reifydb_runtime::{shutdown::Shutdown, sync::mutex::Mutex};
use reifydb_sqlite::{
	SqliteConfig, SqliteTempPathGuard,
	connection::{connect, convert_flags, resolve_db_path},
	pragma,
};
use reifydb_value::{Result, util::cowvec::CowVec};
use rusqlite::{Connection, Error::QueryReturnedNoRows, params};
use tracing::{instrument, warn};

const TABLE_NAME: &str = "column_blocks";

#[derive(Clone)]
pub struct SqliteColumnStore {
	inner: Arc<SqliteColumnStoreInner>,
}

struct SqliteColumnStoreInner {
	conn: Mutex<Option<Connection>>,
}

impl SqliteColumnStore {
	#[instrument(name = "sub_store::column::persistent::new", level = "debug", skip(config), fields(db_path = ?config.path))]
	pub fn new(config: SqliteConfig) -> Self {
		let db_path = resolve_db_path(config.path.clone(), "column.db");
		let flags = convert_flags(&config.flags);

		let conn = connect(&db_path, flags).expect("Failed to connect to column database");
		pragma::apply(&conn, &config).expect("Failed to configure SQLite pragmas");

		let store = Self {
			inner: Arc::new(SqliteColumnStoreInner {
				conn: Mutex::new(Some(conn)),
			}),
		};
		store.ensure_table().expect("Failed to ensure column_blocks table");
		store
	}

	pub fn in_memory() -> (Self, SqliteTempPathGuard) {
		let (config, guard) = SqliteConfig::in_memory();
		(Self::new(config), guard)
	}

	#[instrument(name = "sub_store::column::persistent::ensure_table", level = "trace", skip(self))]
	pub fn ensure_table(&self) -> Result<()> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(());
		};

		conn.execute(
			&format!(
				"CREATE TABLE IF NOT EXISTS \"{}\" (
					snapshot_id BLOB NOT NULL PRIMARY KEY,
					data        BLOB NOT NULL
				) WITHOUT ROWID",
				TABLE_NAME
			),
			[],
		)
		.map(|_| ())
		.map_err(|e| internal_error!("Failed to ensure column_blocks table: {}", e))
	}

	#[instrument(name = "sub_store::column::persistent::put", level = "debug", skip(self, data), fields(snapshot_id = id.0, data_len = data.len()))]
	pub fn put(&self, id: ColumnSnapshotId, data: &[u8]) -> Result<()> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(());
		};

		let key = id.0.to_be_bytes();
		conn.prepare_cached(&format!(
			"INSERT OR REPLACE INTO \"{}\" (snapshot_id, data) VALUES (?1, ?2)",
			TABLE_NAME
		))
		.and_then(|mut stmt| stmt.execute(params![key.as_slice(), data]))
		.map(|_| ())
		.map_err(|e| internal_error!("Failed to put column block: {}", e))
	}

	#[instrument(name = "sub_store::column::persistent::get", level = "trace", skip(self), fields(snapshot_id = id.0))]
	pub fn get(&self, id: ColumnSnapshotId) -> Result<Option<CowVec<u8>>> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(None);
		};

		let key = id.0.to_be_bytes();
		let result = conn
			.prepare_cached(&format!("SELECT data FROM \"{}\" WHERE snapshot_id = ?1", TABLE_NAME))
			.and_then(|mut stmt| stmt.query_row(params![key.as_slice()], |row| row.get::<_, Vec<u8>>(0)));

		match result {
			Ok(data) => Ok(Some(CowVec::new(data))),
			Err(QueryReturnedNoRows) => Ok(None),
			Err(e) if e.to_string().contains("no such table") => Ok(None),
			Err(e) => Err(internal_error!("Failed to get column block: {}", e)),
		}
	}

	#[instrument(name = "sub_store::column::persistent::load_all", level = "debug", skip(self))]
	pub fn load_all(&self) -> Result<Vec<(ColumnSnapshotId, CowVec<u8>)>> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Ok(Vec::new());
		};

		let mut stmt = match conn.prepare(&format!("SELECT snapshot_id, data FROM \"{}\"", TABLE_NAME)) {
			Ok(stmt) => stmt,
			Err(e) if e.to_string().contains("no such table") => return Ok(Vec::new()),
			Err(e) => return Err(internal_error!("Failed to prepare load_all: {}", e)),
		};

		let rows = stmt
			.query_map([], |row| {
				let key: Vec<u8> = row.get(0)?;
				let data: Vec<u8> = row.get(1)?;
				Ok((key, data))
			})
			.map_err(|e| internal_error!("Failed to query column blocks: {}", e))?;

		let mut out = Vec::new();
		for row in rows {
			let (key, data) = row.map_err(|e| internal_error!("Failed to read column block row: {}", e))?;
			match decode_snapshot_id(&key) {
				Some(id) => out.push((id, CowVec::new(data))),
				None => {
					warn!("skipping column block with malformed {}-byte snapshot_id key", key.len())
				}
			}
		}
		Ok(out)
	}
}

fn decode_snapshot_id(key: &[u8]) -> Option<ColumnSnapshotId> {
	let bytes: [u8; 8] = key.try_into().ok()?;
	Some(ColumnSnapshotId(u64::from_be_bytes(bytes)))
}

impl Shutdown for SqliteColumnStore {
	fn shutdown(&self) {
		if let Some(conn) = self.inner.conn.lock().take() {
			if let Err(e) = pragma::shutdown(&conn) {
				warn!(error = %e, "column persistent close: pragma shutdown failed");
			}
			drop(conn);
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::id::ColumnSnapshotId;

	use super::SqliteColumnStore;

	#[test]
	fn put_then_get_round_trips_bytes() {
		let (store, _guard) = SqliteColumnStore::in_memory();
		let id = ColumnSnapshotId(42);
		store.put(id, &[1, 2, 3, 4, 5]).unwrap();
		let got = store.get(id).unwrap().expect("block must be present after put");
		assert_eq!(got.as_slice(), &[1, 2, 3, 4, 5]);
	}

	#[test]
	fn get_missing_returns_none() {
		let (store, _guard) = SqliteColumnStore::in_memory();
		assert!(store.get(ColumnSnapshotId(7)).unwrap().is_none());
	}

	#[test]
	fn put_replaces_existing_block() {
		let (store, _guard) = SqliteColumnStore::in_memory();
		let id = ColumnSnapshotId(1);
		store.put(id, &[9, 9]).unwrap();
		store.put(id, &[1, 2, 3]).unwrap();
		assert_eq!(store.get(id).unwrap().unwrap().as_slice(), &[1, 2, 3]);
	}

	#[test]
	fn load_all_returns_every_persisted_block() {
		let (store, _guard) = SqliteColumnStore::in_memory();
		store.put(ColumnSnapshotId(1), &[1]).unwrap();
		store.put(ColumnSnapshotId(2), &[2, 2]).unwrap();
		store.put(ColumnSnapshotId(3), &[3, 3, 3]).unwrap();

		let mut all = store.load_all().unwrap();
		all.sort_by_key(|(id, _)| id.0);
		let ids: Vec<u64> = all.iter().map(|(id, _)| id.0).collect();
		assert_eq!(ids, vec![1, 2, 3]);
		assert_eq!(all[2].1.as_slice(), &[3, 3, 3]);
	}
}
