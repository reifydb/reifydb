// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rusqlite::{Connection, ToSql};

use crate::{
	SqliteConfig,
	error::{SqliteError, SqliteResult},
};

pub fn apply(conn: &Connection, config: &SqliteConfig) -> SqliteResult<()> {
	set(conn, "page_size", config.page_size.as_bytes() as u32)?;
	set(conn, "auto_vacuum", "INCREMENTAL")?;
	set(conn, "journal_mode", config.journal_mode.as_str())?;
	set(conn, "synchronous", config.synchronous_mode.as_str())?;
	set(conn, "temp_store", config.temp_store.as_str())?;
	set(conn, "cache_size", -(config.cache_size.as_kib() as i32))?;
	set(conn, "wal_autocheckpoint", config.wal_autocheckpoint)?;
	set(conn, "mmap_size", config.mmap_size.as_bytes() as i64)?;
	conn.set_prepared_statement_cache_capacity(config.prepared_statement_cache_capacity as usize);
	Ok(())
}

pub fn apply_read_only(conn: &Connection, config: &SqliteConfig) -> SqliteResult<()> {
	set(conn, "query_only", true)?;
	set(conn, "temp_store", config.temp_store.as_str())?;
	set(conn, "cache_size", -(config.cache_size.as_kib() as i32))?;
	set(conn, "mmap_size", config.mmap_size.as_bytes() as i64)?;
	conn.set_prepared_statement_cache_capacity(config.prepared_statement_cache_capacity as usize);
	Ok(())
}

pub fn shrink_memory(conn: &Connection) -> SqliteResult<()> {
	set(conn, "shrink_memory", 0)
}

pub fn shutdown(conn: &Connection) -> SqliteResult<()> {
	set(conn, "wal_checkpoint", "TRUNCATE")?;
	set(conn, "cache_size", 0)?;
	Ok(())
}

fn set<V: ToSql>(conn: &Connection, name: &str, value: V) -> SqliteResult<()> {
	conn.pragma_update(None, name, value).map_err(|source| SqliteError::Pragma {
		name: name.into(),
		source,
	})
}

#[cfg(test)]
mod tests {
	use std::{env::temp_dir, fs::remove_file};

	use rusqlite::Connection;
	use uuid::Uuid;

	use super::apply;
	use crate::SqliteConfig;

	/// Locks in the unit conversions performed by `apply`: `cache_size` is the KiB count negated
	/// (SQLite reads a negative `cache_size` as KiB, a positive one as pages), while `page_size`
	/// and `mmap_size` are raw bytes. A future change that, say, swapped `as_kib()` for
	/// `as_bytes()` on the cache would record 2_048_000 here and fail.
	#[test]
	fn test_apply_converts_units_for_pragmas() {
		let path = temp_dir().join(format!("reifydb_pragma_{}.db", Uuid::new_v4()));
		let conn = Connection::open(&path).unwrap();

		// new(..) defaults: cache_size 2000 KiB, page_size 4096 bytes, mmap_size 64 MiB.
		apply(&conn, &SqliteConfig::new(&path)).unwrap();

		let cache_size: i64 = conn.pragma_query_value(None, "cache_size", |r| r.get(0)).unwrap();
		let page_size: i64 = conn.pragma_query_value(None, "page_size", |r| r.get(0)).unwrap();
		let mmap_size: i64 = conn.pragma_query_value(None, "mmap_size", |r| r.get(0)).unwrap();

		assert_eq!(cache_size, -2000, "cache_size must be the KiB count negated");
		assert_eq!(page_size, 4096, "page_size must be raw bytes");
		assert_eq!(mmap_size, 67_108_864, "mmap_size must be raw bytes (64 MiB)");

		drop(conn);
		let _ = remove_file(&path);
		let _ = remove_file(path.with_extension("db-wal"));
		let _ = remove_file(path.with_extension("db-shm"));
	}
}
