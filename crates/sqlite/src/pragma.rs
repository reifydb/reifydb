// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use rusqlite::{Connection, ToSql};

use crate::{
	SqliteConfig,
	error::{SqliteError, SqliteResult},
};

/// Apply the pragmas carried by `config` plus `auto_vacuum=INCREMENTAL` to `conn`.
/// Returns on the first failing pragma.
pub fn apply(conn: &Connection, config: &SqliteConfig) -> SqliteResult<()> {
	set(conn, "page_size", config.page_size)?;
	set(conn, "auto_vacuum", "INCREMENTAL")?;
	set(conn, "journal_mode", config.journal_mode.as_str())?;
	set(conn, "synchronous", config.synchronous_mode.as_str())?;
	set(conn, "temp_store", config.temp_store.as_str())?;
	set(conn, "cache_size", -(config.cache_size as i32))?;
	set(conn, "wal_autocheckpoint", config.wal_autocheckpoint)?;
	set(conn, "mmap_size", config.mmap_size as i64)?;
	conn.set_prepared_statement_cache_capacity(config.prepared_statement_cache_capacity as usize);
	Ok(())
}

/// Run `PRAGMA incremental_vacuum` and a `wal_checkpoint(TRUNCATE)` to return
/// freed pages to the OS. In WAL mode, `incremental_vacuum` alone marks pages
/// for truncation in the WAL but the main database file is not actually shrunk
/// until a TRUNCATE-mode checkpoint applies the WAL and truncates both files.
///
/// Both pragmas return result rows, so we drive them with `pragma_query` /
/// `pragma` (which step the prepared statement to completion). `execute` /
/// `execute_batch` either reject statements that return rows or only step
/// once, leaving the truncation incomplete.
pub fn incremental_vacuum(conn: &Connection) -> SqliteResult<()> {
	conn.pragma_query(None, "incremental_vacuum", |_| Ok(())).map_err(|source| SqliteError::Execute {
		statement: "PRAGMA incremental_vacuum".into(),
		source,
	})?;
	conn.pragma(None, "wal_checkpoint", "TRUNCATE", |_| Ok(())).map_err(|source| SqliteError::Execute {
		statement: "PRAGMA wal_checkpoint(TRUNCATE)".into(),
		source,
	})?;
	Ok(())
}

/// Release unused cache memory back to the allocator.
pub fn shrink_memory(conn: &Connection) -> SqliteResult<()> {
	set(conn, "shrink_memory", 0)
}

/// Explicitly checkpoint WAL and shrink the page cache.
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
