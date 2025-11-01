// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

mod cdc;
mod config;
mod diagnostic;
mod gc;
mod multi;
mod query_builder;
mod read;
mod single;
mod write;

use std::{
	ops::Deref,
	sync::{Arc, Mutex, mpsc},
	thread,
};

pub use cdc::{CdcRangeIter, CdcScanIter};
pub use config::*;
pub use multi::{MultiVersionRangeIter, MultiVersionRangeRevIter, MultiVersionScanIter, MultiVersionScanRevIter};
use read::Readers;
use reifydb_type::Error;
use rusqlite::Connection;
pub use single::{SingleVersionRangeIter, SingleVersionRangeRevIter, SingleVersionScanIter, SingleVersionScanRevIter};
use write::{WriteCommand, Writer};

use crate::{
	CdcStore,
	backend::{
		diagnostic::connection_failed,
		multi::BackendMultiVersion,
		single::{BackendSingleVersion, BackendSingleVersionRemove, BackendSingleVersionSet},
	},
};

#[derive(Clone)]
pub struct SqliteBackend(Arc<SqliteBackendInner>);

pub struct SqliteBackendInner {
	writer: mpsc::Sender<WriteCommand>,
	writer_thread: Mutex<Option<thread::JoinHandle<()>>>,
	readers: Readers,
	db_path: DbPath,
}

impl Deref for SqliteBackend {
	type Target = SqliteBackendInner;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Drop for SqliteBackendInner {
	fn drop(&mut self) {
		// Send shutdown command to writer actor
		let _ = self.writer.send(WriteCommand::Shutdown);

		// Wait for writer thread to finish and release all locks
		if let Some(handle) = self.writer_thread.lock().unwrap().take() {
			let _ = handle.join();
		}

		// Close all reader connections to release file locks
		self.readers.close_all();

		// Cleanup tmpfs files for in-memory databases
		if let DbPath::Tmpfs(path) = &self.db_path {
			// Remove database file
			let _ = std::fs::remove_file(path);
			// Remove WAL file
			let _ = std::fs::remove_file(format!("{}-wal", path.display()));
			// Remove shared memory file
			let _ = std::fs::remove_file(format!("{}-shm", path.display()));
		}

		// Cleanup memory files for RAM-only databases
		if let DbPath::Memory(path) = &self.db_path {
			// Remove database file
			let _ = std::fs::remove_file(path);
			// Remove WAL file
			let _ = std::fs::remove_file(format!("{}-wal", path.display()));
			// Remove shared memory file
			let _ = std::fs::remove_file(format!("{}-shm", path.display()));
		}
	}
}

impl SqliteBackend {
	/// Create a new Sqlite storage with the given configuration
	pub fn new(config: SqliteConfig) -> Self {
		let db_path = Self::resolve_db_path(config.path);
		let flags = Self::convert_flags(&config.flags);

		let conn = connect(&db_path, flags.clone()).unwrap();

		// Page size must be set BEFORE creating tables
		conn.pragma_update(None, "page_size", config.page_size).unwrap();

		conn.pragma_update(None, "journal_mode", config.journal_mode.as_str()).unwrap();
		conn.pragma_update(None, "synchronous", config.synchronous_mode.as_str()).unwrap();
		conn.pragma_update(None, "temp_store", config.temp_store.as_str()).unwrap();
		conn.pragma_update(None, "auto_vacuum", "INCREMENTAL").unwrap();
		conn.pragma_update(None, "cache_size", -(config.cache_size as i32)).unwrap();
		conn.pragma_update(None, "wal_autocheckpoint", config.wal_autocheckpoint).unwrap();
		conn.pragma_update(None, "mmap_size", config.mmap_size as i64).unwrap();

		conn.execute_batch(
			"BEGIN;
             -- Multi-version table with WITHOUT ROWID optimization
             CREATE TABLE IF NOT EXISTS multi (
                 key          BLOB NOT NULL,
                 version      INTEGER NOT NULL,
                 value        BLOB,
                 is_tombstone INTEGER NOT NULL DEFAULT 0,
                 PRIMARY KEY (key, version)
             ) WITHOUT ROWID;

             -- Visibility index for fast 'latest visible version' queries
             CREATE INDEX IF NOT EXISTS multi_vis_idx
                 ON multi(key, version DESC)
                 WHERE is_tombstone = 0;

             -- Single version table
             CREATE TABLE IF NOT EXISTS single (
                 key     BLOB NOT NULL,
                 value   BLOB,
                 PRIMARY KEY (key)
             ) WITHOUT ROWID;

             -- CDC table
             CREATE TABLE IF NOT EXISTS cdc (
                 version INTEGER NOT NULL PRIMARY KEY,
                 value   BLOB NOT NULL
             ) WITHOUT ROWID;

             COMMIT;",
		)
		.unwrap();

		let (writer, writer_thread) = Writer::spawn(conn).unwrap();

		Self(Arc::new(SqliteBackendInner {
			writer,
			writer_thread: Mutex::new(Some(writer_thread)),
			readers: Readers::new(db_path.clone(), flags, 4).unwrap(),
			db_path,
		}))
	}

	fn convert_flags(flags: &OpenFlags) -> rusqlite::OpenFlags {
		let mut rusqlite_flags = rusqlite::OpenFlags::empty();

		if flags.read_write {
			rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE;
		}

		if flags.create {
			rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
		}

		if flags.full_mutex {
			rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_FULL_MUTEX;
		}

		if flags.no_mutex {
			rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
		}

		if flags.shared_cache {
			rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_SHARED_CACHE;
		}

		if flags.private_cache {
			rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_PRIVATE_CACHE;
		}

		if flags.uri {
			rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_URI;
		}
		rusqlite_flags
	}

	/// Get a reader connection for read operations
	pub(crate) fn get_reader(&self) -> read::Reader {
		self.readers.get_reader().unwrap()
	}

	fn resolve_db_path(db_path: DbPath) -> DbPath {
		match db_path {
			DbPath::Tmpfs(path) => {
				// Ensure parent directory exists for tmpfs paths
				if let Some(parent) = path.parent() {
					std::fs::create_dir_all(&parent).ok();
				}
				DbPath::Tmpfs(path)
			}
			DbPath::Memory(path) => {
				// Ensure parent directory exists for memory paths (/dev/shm should exist)
				if let Some(parent) = path.parent() {
					std::fs::create_dir_all(&parent).ok();
				}
				DbPath::Memory(path)
			}
			DbPath::File(config_path) => {
				// Check if this is a SQLite URI (contains ':' which indicates URI format)
				let is_uri = config_path.to_string_lossy().contains(':');

				if is_uri {
					// URI paths should be preserved as-is
					DbPath::File(config_path)
				} else if config_path.extension().is_none() {
					// Path is a directory, ensure it exists and create database file inside
					std::fs::create_dir_all(&config_path).ok();
					DbPath::File(config_path.join("reify.db"))
				} else {
					// Path is a file, ensure parent directory exists
					if let Some(parent) = config_path.parent() {
						std::fs::create_dir_all(&parent).ok();
					}
					DbPath::File(config_path)
				}
			}
		}
	}
}

pub(crate) fn connect(path: &DbPath, flags: rusqlite::OpenFlags) -> crate::Result<Connection> {
	match path {
		DbPath::File(path) => {
			let path_str = path.to_string_lossy();
			// Check if this is a URI (contains ':')
			let is_uri = path_str.contains(':');

			if is_uri {
				// For URIs, we must pass them as strings with the URI flag
				let uri_flags = flags | rusqlite::OpenFlags::SQLITE_OPEN_URI;
				Connection::open_with_flags(path_str.as_ref(), uri_flags)
					.map_err(|e| Error(connection_failed(path_str.to_string(), e.to_string())))
			} else {
				Connection::open_with_flags(path, flags).map_err(|e| {
					Error(connection_failed(path.display().to_string(), e.to_string()))
				})
			}
		}
		DbPath::Tmpfs(path) => Connection::open_with_flags(path, flags)
			.map_err(|e| Error(connection_failed(path.display().to_string(), e.to_string()))),
		DbPath::Memory(path) => Connection::open_with_flags(path, flags)
			.map_err(|e| Error(connection_failed(path.display().to_string(), e.to_string()))),
	}
}

impl BackendMultiVersion for SqliteBackend {}
impl BackendSingleVersion for SqliteBackend {}
impl BackendSingleVersionSet for SqliteBackend {}
impl BackendSingleVersionRemove for SqliteBackend {}
impl CdcStore for SqliteBackend {}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;

	use reifydb_testing::tempdir::temp_dir;

	use super::*;

	#[test]
	fn test_resolve_db_path_with_directory() {
		temp_dir(|temp_path| {
			let dir_path = temp_path.join("mydb");
			let db_path = DbPath::File(dir_path.clone());

			// Test with directory path (no extension)
			let result = SqliteBackend::resolve_db_path(db_path);

			// Should append reify.db to directory
			assert_eq!(result, DbPath::File(dir_path.join("reify.db")));

			// Directory should be created
			assert!(dir_path.exists());
			assert!(dir_path.is_dir());

			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_resolve_db_path_with_file() {
		temp_dir(|temp_path| {
			let file_path = temp_path.join("custom.db");

			// Test with file path (has extension)
			let result = SqliteBackend::resolve_db_path(DbPath::File(file_path.clone()));

			// Should use the exact path provided
			assert_eq!(result, DbPath::File(file_path));

			// Parent directory should exist
			assert!(temp_path.exists());

			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_resolve_db_path_nested_directory() {
		temp_dir(|temp_path| {
			let nested_path = temp_path.join("level1").join("level2").join("mydb");

			// Test with nested directory path
			let result = SqliteBackend::resolve_db_path(DbPath::File(nested_path.clone()));

			// Should create nested directories and append reify.db
			assert_eq!(result, DbPath::File(nested_path.join("reify.db")));
			assert!(nested_path.exists());
			assert!(nested_path.is_dir());

			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_resolve_db_path_nested_file() {
		temp_dir(|temp_path| {
			let nested_file = temp_path.join("level1").join("level2").join("database.sqlite");

			// Test with nested file path
			let result = SqliteBackend::resolve_db_path(DbPath::File(nested_file.clone()));

			// Should create parent directories and use exact
			// filename
			assert_eq!(result, DbPath::File(nested_file));
			assert!(temp_path.join("level1").join("level2").exists());

			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_resolve_db_path_with_various_extensions() {
		temp_dir(|temp_path| {
			// Test with .db extension
			let db_file = temp_path.join("test.db");
			assert_eq!(
				SqliteBackend::resolve_db_path(DbPath::File(db_file.clone())),
				DbPath::File(db_file)
			);

			// Test with .sqlite extension
			let sqlite_file = temp_path.join("test.sqlite");
			assert_eq!(
				SqliteBackend::resolve_db_path(DbPath::File(sqlite_file.clone())),
				DbPath::File(sqlite_file)
			);

			// Test with .reifydb extension
			let reifydb_file = temp_path.join("test.reifydb");
			assert_eq!(
				SqliteBackend::resolve_db_path(DbPath::File(reifydb_file.clone())),
				DbPath::File(reifydb_file)
			);

			// Test with no extension (directory)
			let no_ext = temp_path.join("testdb");
			assert_eq!(
				SqliteBackend::resolve_db_path(DbPath::File(no_ext.clone())),
				DbPath::File(no_ext.join("reify.db"))
			);

			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_sqlite_creation_with_new_config() {
		temp_dir(|db_path| {
			let config = SqliteConfig::new(db_path.join("test.reifydb"));
			let storage = SqliteBackend::new(config);

			// Verify we can get a reader connection
			let conn = storage.get_reader();
			let _guard = conn.lock().unwrap();
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_sqlite_creation_with_safe_config() {
		temp_dir(|db_path| {
			let config = SqliteConfig::safe(db_path.join("safe.reifydb"));
			let storage = SqliteBackend::new(config);

			// Verify we can get a reader connection
			let conn = storage.get_reader();
			let _guard = conn.lock().unwrap();
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_sqlite_creation_with_fast_config() {
		temp_dir(|db_path| {
			let config = SqliteConfig::fast(db_path.join("fast.reifydb"));
			let storage = SqliteBackend::new(config);

			// Verify we can get a reader connection
			let conn = storage.get_reader();
			let _guard = conn.lock().unwrap();
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_directory_path_handling() {
		temp_dir(|db_path| {
			let config = SqliteConfig::new(db_path);
			let storage = SqliteBackend::new(config);

			let conn = storage.get_reader();
			let _guard = conn.lock().unwrap();

			assert!(db_path.join("reify.db").exists());
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_file_path_handling() {
		temp_dir(|db_path| {
			// Test with specific file path
			let db_file = db_path.join("custom.reifydb");
			let config = SqliteConfig::new(&db_file);
			let storage = SqliteBackend::new(config);

			// Verify we can get a reader connection
			let conn = storage.get_reader();
			let _guard = conn.lock().unwrap();

			// Verify the specific database file was created
			assert!(db_file.exists());
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_custom_flags_conversion() {
		temp_dir(|db_path| {
			let config = SqliteConfig::new(db_path.join("flags.reifydb")).flags(OpenFlags::new()
				.read_write(true)
				.create(true)
				.no_mutex(true)
				.shared_cache(true)
				.uri(true));

			let storage = SqliteBackend::new(config);

			let conn = storage.get_reader();
			let _guard = conn.lock().unwrap();
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_sources_created() {
		temp_dir(|db_path| {
			let config = SqliteConfig::new(db_path.join("sources.reifydb"));
			let storage = SqliteBackend::new(config);
			let conn = storage.get_reader();
			let conn_guard = conn.lock().unwrap();

			// Check that both sources exist
			let mut stmt = conn_guard
				.prepare(
					"SELECT name FROM sqlite_master WHERE type='table' AND name IN ('multi', 'single')",
				)
				.unwrap();
			let source_names: Vec<String> =
				stmt.query_map([], |values| Ok(values.get(0)?)).unwrap().map(Result::unwrap).collect();

			assert_eq!(source_names.len(), 2);
			assert!(source_names.contains(&"multi".to_string()));
			assert!(source_names.contains(&"single".to_string()));
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_in_memory_database() {
		// Test SqliteConfig::in_memory() creates a working backend
		let config = SqliteConfig::in_memory();
		let storage = SqliteBackend::new(config);

		// Verify we can get a reader connection
		let conn = storage.get_reader();
		let conn_guard = conn.lock().unwrap();

		// Verify tables are created in the in-memory database
		let mut stmt = conn_guard
			.prepare(
				"SELECT name FROM sqlite_master WHERE type='table' AND name IN ('multi', 'single', 'cdc')",
			)
			.unwrap();
		let table_names: Vec<String> =
			stmt.query_map([], |row| Ok(row.get(0)?)).unwrap().map(Result::unwrap).collect();

		assert_eq!(table_names.len(), 3);
		assert!(table_names.contains(&"multi".to_string()));
		assert!(table_names.contains(&"single".to_string()));
		assert!(table_names.contains(&"cdc".to_string()));
	}

	#[test]
	fn test_in_memory_test_config() {
		// Test SqliteConfig::test() creates a working backend
		let config = SqliteConfig::test();
		let storage = SqliteBackend::new(config);

		// Verify we can get a reader connection
		let conn = storage.get_reader();
		let conn_guard = conn.lock().unwrap();

		// Verify tables are created
		let mut stmt = conn_guard.prepare("SELECT name FROM sqlite_master WHERE type='table'").unwrap();
		let table_names: Vec<String> =
			stmt.query_map([], |row| Ok(row.get(0)?)).unwrap().map(Result::unwrap).collect();

		// Should have multi, single, and cdc tables
		assert!(table_names.len() >= 3);
		assert!(table_names.contains(&"multi".to_string()));
		assert!(table_names.contains(&"single".to_string()));
		assert!(table_names.contains(&"cdc".to_string()));
	}

	#[test]
	fn test_uri_path_preserved() {
		// Test that URI paths are preserved by resolve_db_path
		let uri = PathBuf::from("file:memdb?mode=memory&cache=shared");
		let resolved = SqliteBackend::resolve_db_path(DbPath::File(uri.clone()));
		assert_eq!(resolved, DbPath::File(uri));

		// Test :memory: path
		let memory = PathBuf::from(":memory:");
		let resolved_memory = SqliteBackend::resolve_db_path(DbPath::File(memory.clone()));
		assert_eq!(resolved_memory, DbPath::File(memory));

		// Test regular file path still works
		let file_path = PathBuf::from("/tmp/test.db");
		let resolved_file = SqliteBackend::resolve_db_path(DbPath::File(file_path.clone()));
		assert_eq!(resolved_file, DbPath::File(file_path));
	}
}
