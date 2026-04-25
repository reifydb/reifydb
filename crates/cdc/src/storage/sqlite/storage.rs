// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! SQLite-backed implementation of `CdcStorage`.
//!
//! Single table, one row per CommitVersion, payload is a postcard-encoded `Cdc`.
//! Concurrency: single `Mutex<Connection>` (rusqlite::Connection is Send but !Sync).

use std::{collections::Bound, sync::Arc};

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
use rusqlite::{Connection, Error::QueryReturnedNoRows, params, params_from_iter, types::Value as SqlValue};

use crate::{
	error::CdcError,
	storage::{CdcStorage, CdcStorageResult, DropBeforeResult, DroppedCdcEntry},
};

#[derive(Clone)]
pub struct SqliteCdcStorage {
	inner: Arc<Inner>,
}

struct Inner {
	conn: Mutex<Connection>,
}

impl SqliteCdcStorage {
	pub fn new(config: SqliteConfig) -> Self {
		let db_path = resolve_db_path(config.path.clone(), "cdc.db");
		let flags = convert_flags(&config.flags);

		let conn = connect(&db_path, flags).expect("Failed to connect to CDC SQLite database");
		pragma::apply(&conn, &config).expect("Failed to configure CDC SQLite pragmas");

		Self::ensure_schema(&conn);

		Self {
			inner: Arc::new(Inner {
				conn: Mutex::new(conn),
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
}

fn version_to_bytes(v: CommitVersion) -> [u8; 8] {
	v.0.to_be_bytes()
}

fn bytes_to_version(bytes: &[u8]) -> CdcStorageResult<CommitVersion> {
	let arr: [u8; 8] = bytes.try_into().map_err(|_| CdcError::Internal("bad version bytes".to_string()))?;
	Ok(CommitVersion(u64::from_be_bytes(arr)))
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

	fn read_range(
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

	fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		Ok(self.read(version)?.map(|c| c.system_changes.len()).unwrap_or(0))
	}

	fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		let conn = self.inner.conn.lock();
		let result =
			conn.query_row(r#"SELECT MIN(version) FROM "cdc""#, [], |row| row.get::<_, Option<Vec<u8>>>(0));
		match result {
			Ok(Some(bytes)) => Ok(Some(bytes_to_version(&bytes)?)),
			Ok(None) | Err(QueryReturnedNoRows) => Ok(None),
			Err(e) => Err(CdcError::Internal(format!("min version: {e}"))),
		}
	}

	fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		let conn = self.inner.conn.lock();
		let result =
			conn.query_row(r#"SELECT MAX(version) FROM "cdc""#, [], |row| row.get::<_, Option<Vec<u8>>>(0));
		match result {
			Ok(Some(bytes)) => Ok(Some(bytes_to_version(&bytes)?)),
			Ok(None) | Err(QueryReturnedNoRows) => Ok(None),
			Err(e) => Err(CdcError::Internal(format!("max version: {e}"))),
		}
	}

	fn drop_before(&self, version: CommitVersion) -> CdcStorageResult<DropBeforeResult> {
		let conn = self.inner.conn.lock();
		let version_bytes = version_to_bytes(version);

		let mut entries = Vec::new();
		let mut count = 0usize;
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

		conn.execute(r#"DELETE FROM "cdc" WHERE version < ?1"#, params![version_bytes.as_slice()])
			.map_err(|e| CdcError::Internal(format!("drop_before delete: {e}")))?;
		let _ = conn.execute("PRAGMA incremental_vacuum", []);

		Ok(DropBeforeResult {
			count,
			entries,
		})
	}
}
