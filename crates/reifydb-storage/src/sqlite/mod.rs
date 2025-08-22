// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

mod cdc;
mod config;
mod unversioned;
mod versioned;

use std::{
	ops::Deref,
	path::{Path, PathBuf},
	sync::Arc,
};

pub use config::*;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use reifydb_core::interface::{
	CdcStorage, UnversionedInsert, UnversionedRemove, UnversionedStorage,
	VersionedStorage,
};

#[derive(Clone)]
pub struct Sqlite(Arc<SqliteInner>);

pub struct SqliteInner {
	pool: Arc<Pool<SqliteConnectionManager>>,
}

impl Deref for Sqlite {
	type Target = SqliteInner;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Sqlite {
	/// Create a new Sqlite storage with the given configuration
	pub fn new(config: SqliteConfig) -> Self {
		let db_path = Self::resolve_db_path(&config.path);

		let manager = SqliteConnectionManager::file(db_path)
			.with_flags(Self::convert_flags(&config.flags));

		let pool = Pool::builder()
			.max_size(config.max_pool_size)
			.build(manager)
			.unwrap();
		{
			let conn = pool.get().unwrap();
			conn.pragma_update(
				None,
				"journal_mode",
				config.journal_mode.as_str(),
			)
			.unwrap();
			conn.pragma_update(
				None,
				"synchronous",
				config.synchronous_mode.as_str(),
			)
			.unwrap();
			conn.pragma_update(
				None,
				"temp_store",
				config.temp_store.as_str(),
			)
			.unwrap();

			conn.execute_batch(
				"BEGIN;
                 CREATE TABLE IF NOT EXISTS versioned (
                     key     BLOB NOT NULL,
                     version INTEGER NOT NULL,
                     value   BLOB NOT NULL,
                     PRIMARY KEY (key, version)
                 );

                 CREATE TABLE IF NOT EXISTS unversioned (
                     key     BLOB NOT NULL,
                     value   BLOB NOT NULL,
                     PRIMARY KEY (key)
                 );

                 CREATE TABLE IF NOT EXISTS cdc (
                     key     BLOB NOT NULL,
                     version INTEGER NOT NULL,
                     value   BLOB NOT NULL,
                     PRIMARY KEY (key, version)
                 );
                 COMMIT;",
			)
			.unwrap();
		}

		Self(Arc::new(SqliteInner {
			pool: Arc::new(pool),
		}))
	}

	fn convert_flags(flags: &OpenFlags) -> rusqlite::OpenFlags {
		let mut rusqlite_flags = rusqlite::OpenFlags::empty();

		if flags.read_write {
			rusqlite_flags |=
				rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE;
		}

		if flags.create {
			rusqlite_flags |=
				rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
		}

		if flags.full_mutex {
			rusqlite_flags |=
				rusqlite::OpenFlags::SQLITE_OPEN_FULL_MUTEX;
		}

		if flags.no_mutex {
			rusqlite_flags |=
				rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
		}

		if flags.shared_cache {
			rusqlite_flags |=
				rusqlite::OpenFlags::SQLITE_OPEN_SHARED_CACHE;
		}

		if flags.private_cache {
			rusqlite_flags |=
				rusqlite::OpenFlags::SQLITE_OPEN_PRIVATE_CACHE;
		}

		if flags.uri {
			rusqlite_flags |= rusqlite::OpenFlags::SQLITE_OPEN_URI;
		}
		rusqlite_flags
	}

	fn get_conn(&self) -> PooledConnection<SqliteConnectionManager> {
		self.pool.get().unwrap()
	}

	fn resolve_db_path(config_path: &Path) -> PathBuf {
		if config_path.extension().is_none() {
			// Path is a directory, ensure it exists and create db
			// file inside
			std::fs::create_dir_all(config_path).ok();
			config_path.join("reify.db")
		} else {
			// Path is a file, ensure parent directory exists
			if let Some(parent) = config_path.parent() {
				std::fs::create_dir_all(parent).ok();
			}
			config_path.to_path_buf()
		}
	}
}

impl VersionedStorage for Sqlite {}
impl UnversionedStorage for Sqlite {}
impl UnversionedInsert for Sqlite {}
impl UnversionedRemove for Sqlite {}
impl CdcStorage for Sqlite {}

#[cfg(test)]
mod tests {
	use reifydb_testing::tempdir::temp_dir;

	use super::*;

	#[test]
	fn test_resolve_db_path_with_directory() {
		temp_dir(|temp_path| {
			let dir_path = temp_path.join("mydb");

			// Test with directory path (no extension)
			let result = Sqlite::resolve_db_path(&dir_path);

			// Should append reify.db to directory
			assert_eq!(result, dir_path.join("reify.db"));

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
			let result = Sqlite::resolve_db_path(&file_path);

			// Should use the exact path provided
			assert_eq!(result, file_path);

			// Parent directory should exist
			assert!(temp_path.exists());

			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_resolve_db_path_nested_directory() {
		temp_dir(|temp_path| {
			let nested_path = temp_path
				.join("level1")
				.join("level2")
				.join("mydb");

			// Test with nested directory path
			let result = Sqlite::resolve_db_path(&nested_path);

			// Should create nested directories and append reify.db
			assert_eq!(result, nested_path.join("reify.db"));
			assert!(nested_path.exists());
			assert!(nested_path.is_dir());

			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_resolve_db_path_nested_file() {
		temp_dir(|temp_path| {
			let nested_file = temp_path
				.join("level1")
				.join("level2")
				.join("database.sqlite");

			// Test with nested file path
			let result = Sqlite::resolve_db_path(&nested_file);

			// Should create parent directories and use exact
			// filename
			assert_eq!(result, nested_file);
			assert!(temp_path
				.join("level1")
				.join("level2")
				.exists());

			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_resolve_db_path_with_various_extensions() {
		temp_dir(|temp_path| {
			// Test with .db extension
			let db_file = temp_path.join("test.db");
			assert_eq!(Sqlite::resolve_db_path(&db_file), db_file);

			// Test with .sqlite extension
			let sqlite_file = temp_path.join("test.sqlite");
			assert_eq!(
				Sqlite::resolve_db_path(&sqlite_file),
				sqlite_file
			);

			// Test with .reifydb extension
			let reifydb_file = temp_path.join("test.reifydb");
			assert_eq!(
				Sqlite::resolve_db_path(&reifydb_file),
				reifydb_file
			);

			// Test with no extension (directory)
			let no_ext = temp_path.join("testdb");
			assert_eq!(
				Sqlite::resolve_db_path(&no_ext),
				no_ext.join("reify.db")
			);

			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_sqlite_creation_with_new_config() {
		temp_dir(|db_path| {
			let config =
				SqliteConfig::new(db_path.join("test.reifydb"));
			let storage = Sqlite::new(config);

			// Verify we can get a connection
			let _conn = storage.get_conn();
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_sqlite_creation_with_safe_config() {
		temp_dir(|db_path| {
			let config = SqliteConfig::safe(
				db_path.join("safe.reifydb"),
			);
			let storage = Sqlite::new(config);

			// Verify we can get a connection
			let _conn = storage.get_conn();
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_sqlite_creation_with_fast_config() {
		temp_dir(|db_path| {
			let config = SqliteConfig::fast(
				db_path.join("fast.reifydb"),
			);
			let storage = Sqlite::new(config);

			// Verify we can get a connection
			let _conn = storage.get_conn();
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_directory_path_handling() {
		temp_dir(|db_path| {
			let config = SqliteConfig::new(db_path);
			let storage = Sqlite::new(config);

			let _conn = storage.get_conn();

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
			let storage = Sqlite::new(config);

			// Verify we can get a connection
			let _conn = storage.get_conn();

			// Verify the specific database file was created
			assert!(db_file.exists());
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_custom_flags_conversion() {
		temp_dir(|db_path| {
			let config = SqliteConfig::new(
				db_path.join("flags.reifydb"),
			)
			.flags(OpenFlags::new()
				.read_write(true)
				.create(true)
				.no_mutex(true)
				.shared_cache(true)
				.uri(true));

			let storage = Sqlite::new(config);
			let _conn = storage.get_conn();
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_custom_pool_size() {
		temp_dir(|db_path| {
			let config =
				SqliteConfig::new(db_path.join("pool.reifydb"))
					.max_pool_size(1);

			let storage = Sqlite::new(config);

			// Should be able to get at least one connection
			let _conn1 = storage.get_conn();
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_pragma_settings_applied() {
		temp_dir(|db_path| {
			let config = SqliteConfig::new(
				db_path.join("pragma.reifydb"),
			)
			.journal_mode(JournalMode::Delete)
			.synchronous_mode(SynchronousMode::Extra)
			.temp_store(TempStore::File);

			let storage = Sqlite::new(config);
			let conn = storage.get_conn();

			// Verify pragma settings were applied (simplified
			// check)
			let journal_mode: String = conn
				.pragma_query_value(
					None,
					"journal_mode",
					|row| Ok(row.get(0)?),
				)
				.unwrap();
			assert_eq!(journal_mode.to_uppercase(), "DELETE");

			let synchronous: i32 = conn
				.pragma_query_value(
					None,
					"synchronous",
					|row| Ok(row.get(0)?),
				)
				.unwrap();
			assert_eq!(synchronous, 3); // EXTRA = 3

			let temp_store: i32 = conn
				.pragma_query_value(None, "temp_store", |row| {
					Ok(row.get(0)?)
				})
				.unwrap();
			assert_eq!(temp_store, 1); // FILE = 1
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_tables_created() {
		temp_dir(|db_path| {
			let config = SqliteConfig::new(db_path.join("tables.reifydb"));
			let storage = Sqlite::new(config);
			let conn = storage.get_conn();

			// Check that both tables exist
			let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('versioned', 'unversioned')").unwrap();
			let table_names: Vec<String> = stmt.query_map([], |row| {
				Ok(row.get(0)?)
			}).unwrap().map(Result::unwrap).collect();

			assert_eq!(table_names.len(), 2);
			assert!(table_names.contains(&"versioned".to_string()));
			assert!(table_names.contains(&"unversioned".to_string()));
			Ok(())
		}).expect("test failed");
	}
}
