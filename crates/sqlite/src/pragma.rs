// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rusqlite::{Connection, ToSql};

use crate::{
	SqliteConfig,
	error::{SqliteError, SqliteResult},
};

pub fn apply(conn: &Connection, config: &SqliteConfig) -> SqliteResult<()> {
	if let Some(page_size) = config.page_size {
		set(conn, "page_size", page_size.as_bytes() as u32)?;
	}
	set(conn, "auto_vacuum", "INCREMENTAL")?;
	set(conn, "secure_delete", "FAST")?;
	if let Some(journal_mode) = config.journal_mode {
		set(conn, "journal_mode", journal_mode.as_str())?;
	}
	if let Some(synchronous_mode) = config.synchronous_mode {
		set(conn, "synchronous", synchronous_mode.as_str())?;
	}
	if let Some(temp_store) = config.temp_store {
		set(conn, "temp_store", temp_store.as_str())?;
	}
	if let Some(cache_size) = config.cache_size {
		set(conn, "cache_size", -(cache_size.as_kib() as i32))?;
	}
	if let Some(wal_autocheckpoint) = config.wal_autocheckpoint {
		set(conn, "wal_autocheckpoint", wal_autocheckpoint)?;
	}
	if let Some(mmap_size) = config.mmap_size {
		set(conn, "mmap_size", mmap_size.as_bytes() as i64)?;
	}
	conn.set_prepared_statement_cache_capacity(config.prepared_statement_cache_capacity as usize);
	Ok(())
}

pub fn apply_read_only(conn: &Connection, config: &SqliteConfig) -> SqliteResult<()> {
	set(conn, "query_only", true)?;
	if let Some(temp_store) = config.temp_store {
		set(conn, "temp_store", temp_store.as_str())?;
	}
	if let Some(cache_size) = config.cache_size {
		set(conn, "cache_size", -(cache_size.as_kib() as i32))?;
	}
	if let Some(mmap_size) = config.mmap_size {
		set(conn, "mmap_size", mmap_size.as_bytes() as i64)?;
	}
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
	use std::{env::temp_dir, fs::remove_file, path::PathBuf};

	use reifydb_value::byte_size::ByteSize;
	use rusqlite::Connection;
	use uuid::Uuid;

	use super::{apply, apply_read_only};
	use crate::SqliteConfig;

	/// A throwaway on-disk database. Several pragma defaults differ between file-backed and
	/// in-memory databases, so these tests have to run against a real file to mean anything.
	fn scratch(name: &str) -> (Connection, PathBuf) {
		let path = temp_dir().join(format!("reifydb_pragma_{name}_{}.db", Uuid::new_v4()));
		let conn = Connection::open(&path).unwrap();
		(conn, path)
	}

	fn cleanup(conn: Connection, path: PathBuf) {
		drop(conn);
		let _ = remove_file(&path);
		let _ = remove_file(path.with_extension("db-wal"));
		let _ = remove_file(path.with_extension("db-shm"));
	}

	/// Locks in the unit conversions performed by `apply`: `cache_size` is the KiB count negated
	/// (SQLite reads a negative `cache_size` as KiB, a positive one as pages), while `page_size`
	/// and `mmap_size` are raw bytes. A future change that, say, swapped `as_kib()` for
	/// `as_bytes()` on the cache would record 2_048_000 here and fail.
	#[test]
	fn a_none_pragma_is_never_issued_and_sqlites_own_default_survives() {
		// The point of making these fields optional: None must mean "do not touch this setting",
		// not "set it to some stand-in value". The distinctive 4321 matters - new()'s own default is
		// 2000 KiB, which SQLite also reports as -2000, so asserting -2000 against an untouched
		// new() would pass even if apply fell back to the configured value instead of skipping.
		// Setting a value first and then clearing it makes the two outcomes tell apart: a genuine
		// skip reads SQLite's -2000, any fallback to the config reads -4321. It also pins the trap
		// that None is NOT how you disable caching, which the sibling test covers from the far side.
		let (conn, path) = scratch("none");
		let config = SqliteConfig::new(&path).cache_size(ByteSize::from_kib(4321)).cache_size(None);

		apply(&conn, &config).unwrap();

		let cache_size: i64 = conn.pragma_query_value(None, "cache_size", |r| r.get(0)).unwrap();
		assert_eq!(cache_size, -2000, "an unset cache_size must leave SQLite's own default in place");

		cleanup(conn, path);
	}

	#[test]
	fn journal_mode_none_leaves_a_fresh_database_on_a_rollback_journal() {
		// The most dangerous None in the config, and the one whose failure schedule hides it: WAL is
		// persisted in the database header, so an unset journal_mode looks harmless against every
		// already-created database and only detonates on a clean install. store-multi opens a writer
		// plus a pool of readers concurrently, and outside WAL those readers take locks that block
		// the writer. Both directions are asserted so that shipping WAL stays a tested property
		// rather than a habit.
		let (unset_conn, unset_path) = scratch("journal_none");
		apply(&unset_conn, &SqliteConfig::new(&unset_path).journal_mode(None)).unwrap();
		let unset: String = unset_conn.pragma_query_value(None, "journal_mode", |r| r.get(0)).unwrap();

		let (default_conn, default_path) = scratch("journal_default");
		apply(&default_conn, &SqliteConfig::new(&default_path)).unwrap();
		let default: String = default_conn.pragma_query_value(None, "journal_mode", |r| r.get(0)).unwrap();

		assert_eq!(unset, "delete", "an unset journal_mode drops a fresh database to a rollback journal");
		assert_eq!(default, "wal", "the constructors must keep shipping WAL");

		cleanup(unset_conn, unset_path);
		cleanup(default_conn, default_path);
	}

	#[test]
	fn an_explicit_zero_still_issues_the_pragma() {
		// ByteSize::ZERO and None are different instructions and have to stay that way: zero asks
		// SQLite to retain no page cache, None declines to configure it at all. Collapsing the two
		// by treating ZERO as "unset" would silently restore the 2000 KiB default on every
		// connection, which is the per-connection cache this whole change exists to be able to
		// remove.
		let (conn, path) = scratch("zero");

		apply(&conn, &SqliteConfig::new(&path).cache_size(ByteSize::ZERO)).unwrap();

		let cache_size: i64 = conn.pragma_query_value(None, "cache_size", |r| r.get(0)).unwrap();
		assert_eq!(cache_size, 0, "an explicit zero must be issued, not mistaken for unset");

		cleanup(conn, path);
	}

	#[test]
	fn the_read_only_path_decides_each_pragma_independently() {
		// apply_read_only configures the multi store's read pool, where a per-connection pragma
		// costs the most because it is applied once per pooled connection. Each field has to be
		// decided on its own: skipping one must not skip the next, and issuing one must not drag
		// the other along with it.
		let (conn, path) = scratch("readonly");
		let config = SqliteConfig::new(&path).mmap_size(None).cache_size(ByteSize::from_kib(1500));

		apply_read_only(&conn, &config).unwrap();

		let mmap_size: i64 = conn.pragma_query_value(None, "mmap_size", |r| r.get(0)).unwrap();
		let cache_size: i64 = conn.pragma_query_value(None, "cache_size", |r| r.get(0)).unwrap();

		assert_eq!(mmap_size, 0, "an unset mmap_size must leave SQLite's default of 0");
		assert_eq!(cache_size, -1500, "a set cache_size must still be issued alongside a skipped one");

		cleanup(conn, path);
	}

	#[test]
	fn test_apply_converts_units_for_pragmas() {
		let path = temp_dir().join(format!("reifydb_pragma_{}.db", Uuid::new_v4()));
		let conn = Connection::open(&path).unwrap();

		// new(..) defaults: cache_size 2000 KiB, page_size 4096 bytes, mmap_size 64 MiB.
		apply(&conn, &SqliteConfig::new(&path)).unwrap();

		let cache_size: i64 = conn.pragma_query_value(None, "cache_size", |r| r.get(0)).unwrap();
		let page_size: i64 = conn.pragma_query_value(None, "page_size", |r| r.get(0)).unwrap();
		let mmap_size: i64 = conn.pragma_query_value(None, "mmap_size", |r| r.get(0)).unwrap();
		let secure_delete: i64 = conn.pragma_query_value(None, "secure_delete", |r| r.get(0)).unwrap();

		assert_eq!(cache_size, -2000, "cache_size must be the KiB count negated");
		assert_eq!(page_size, 4096, "page_size must be raw bytes");
		assert_eq!(mmap_size, 67_108_864, "mmap_size must be raw bytes (64 MiB)");
		// FAST (2) skips the extra I/O of zeroing wholly-freed overflow pages on DELETE, which was
		// the dominant cost of CDC eviction and persist_sweep; 1 (ON) would reintroduce that tax.
		assert_eq!(secure_delete, 2, "secure_delete must be FAST (2), not ON (1)");

		drop(conn);
		let _ = remove_file(&path);
		let _ = remove_file(path.with_extension("db-wal"));
		let _ = remove_file(path.with_extension("db-shm"));
	}
}
