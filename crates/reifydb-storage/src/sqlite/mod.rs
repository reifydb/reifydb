// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

mod cdc;
mod config;
mod unversioned;
mod versioned;

use std::{
	collections::HashSet,
	ops::Deref,
	path::{Path, PathBuf},
	sync::{Arc, Mutex, mpsc},
	thread,
};

pub use config::*;
use reifydb_core::{
	CowVec, Version,
	delta::Delta,
	interface::{
		CdcStorage, TransactionId, UnversionedInsert,
		UnversionedRemove, UnversionedStorage, VersionedStorage,
	},
};
use rusqlite::Connection;

#[derive(Clone)]
pub struct Sqlite(Arc<SqliteInner>);

pub struct SqliteInner {
	// Multiple reader connections (one per thread accessing it)
	readers: Arc<Mutex<Vec<Arc<Mutex<Connection>>>>>,
	// Channel to send write commands to the writer actor
	writer: mpsc::Sender<WriteCommand>,
	// Store the path for creating new reader connections
	db_path: PathBuf,

	flags: rusqlite::OpenFlags,
}

enum WriteCommand {
	Transaction {
		deltas: CowVec<Delta>,
		response: mpsc::Sender<rusqlite::Result<()>>,
	},
	VersionedCommit {
		deltas: CowVec<Delta>,
		version: Version,
		transaction: TransactionId,
		timestamp: u64,
		response: mpsc::Sender<rusqlite::Result<()>>,
	},
	Shutdown,
}

impl Deref for Sqlite {
	type Target = SqliteInner;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Drop for SqliteInner {
	fn drop(&mut self) {
		// Send shutdown command to writer actor
		let _ = self.writer.send(WriteCommand::Shutdown);
	}
}

impl Sqlite {
	/// Create a new Sqlite storage with the given configuration
	pub fn new(config: SqliteConfig) -> Self {
		let db_path = Self::resolve_db_path(&config.path);
		let flags = Self::convert_flags(&config.flags);

		// Create the database and set up tables with a temporary
		// connection
		{
			let conn = Connection::open_with_flags(&db_path, flags)
				.unwrap();
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
                     version INTEGER NOT NULL PRIMARY KEY,
                     value   BLOB NOT NULL
                 );
                 COMMIT;",
			)
			.unwrap();
		}

		// Create writer actor channel
		let (writer_tx, writer_rx) = mpsc::channel();

		// Spawn writer actor thread
		let writer_path = db_path.clone();
		let writer_flags = flags;
		let writer_config = config.clone();
		thread::spawn(move || {
			Self::writer_actor(
				writer_path,
				writer_flags,
				writer_config,
				writer_rx,
			);
		});

		// Create initial reader pool
		let mut readers = Vec::new();
		for _ in 0..4 {
			let conn = Connection::open_with_flags(&db_path, flags)
				.unwrap();
			readers.push(Arc::new(Mutex::new(conn)));
		}

		Self(Arc::new(SqliteInner {
			readers: Arc::new(Mutex::new(readers)),
			writer: writer_tx,
			db_path,
			flags,
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

	/// Get a reader connection for read operations
	pub(crate) fn get_reader(&self) -> Arc<Mutex<Connection>> {
		let mut pool = self.readers.lock().unwrap();

		// Simple round-robin: take first, use it, put it back at end
		if let Some(conn) = pool.pop() {
			let conn_clone = conn.clone();
			pool.insert(0, conn);
			conn_clone
		} else {
			// Create a new reader if pool is empty
			let conn = Connection::open_with_flags(
				&self.db_path,
				self.flags,
			)
			.unwrap();
			let arc_conn = Arc::new(Mutex::new(conn));
			pool.push(arc_conn.clone());
			arc_conn
		}
	}

	/// Execute a transaction through the writer actor
	pub(crate) fn execute_transaction(
		&self,
		deltas: CowVec<Delta>,
	) -> rusqlite::Result<()> {
		let (tx, rx) = mpsc::channel();
		self.writer
			.send(WriteCommand::Transaction {
				deltas,
				response: tx,
			})
			.map_err(|_| {
				rusqlite::Error::SqliteFailure(
					rusqlite::ffi::Error::new(
						rusqlite::ffi::SQLITE_MISUSE,
					),
					Some("Writer actor disconnected"
						.to_string()),
				)
			})?;
		rx.recv().map_err(|_| {
			rusqlite::Error::SqliteFailure(
				rusqlite::ffi::Error::new(
					rusqlite::ffi::SQLITE_MISUSE,
				),
				Some("Writer actor response failed"
					.to_string()),
			)
		})?
	}

	/// Execute a versioned commit through the writer actor
	pub(crate) fn execute_versioned_commit(
		&self,
		deltas: CowVec<Delta>,
		version: Version,
		transaction: TransactionId,
	) -> rusqlite::Result<()> {
		// Calculate timestamp in client thread where mock time is set
		let timestamp = reifydb_core::util::now_millis();
		let (tx, rx) = mpsc::channel();
		self.writer
			.send(WriteCommand::VersionedCommit {
				deltas,
				version,
				transaction,
				timestamp,
				response: tx,
			})
			.map_err(|_| {
				rusqlite::Error::SqliteFailure(
					rusqlite::ffi::Error::new(
						rusqlite::ffi::SQLITE_MISUSE,
					),
					Some("Writer actor disconnected"
						.to_string()),
				)
			})?;
		rx.recv().map_err(|_| {
			rusqlite::Error::SqliteFailure(
				rusqlite::ffi::Error::new(
					rusqlite::ffi::SQLITE_MISUSE,
				),
				Some("Writer actor response failed"
					.to_string()),
			)
		})?
	}

	/// Writer actor that handles all write operations
	fn writer_actor(
		db_path: PathBuf,
		flags: rusqlite::OpenFlags,
		config: SqliteConfig,
		rx: mpsc::Receiver<WriteCommand>,
	) {
		let mut conn =
			Connection::open_with_flags(&db_path, flags).unwrap();

		// Set pragmas for writer connection
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

		// Track ensured tables for versioned commits
		let mut ensured_tables: HashSet<String> = HashSet::new();

		while let Ok(cmd) = rx.recv() {
			match cmd {
				WriteCommand::Transaction {
					deltas,
					response,
				} => {
					let result = (|| {
						let tx = conn.transaction()?;
						for delta in deltas {
							match delta {
								Delta::Set { key, row: bytes } => {
									tx.execute(
										"INSERT OR REPLACE INTO unversioned (key,value) VALUES (?1, ?2)",
										rusqlite::params![key.to_vec(), bytes.to_vec()],
									)?;
								}
								Delta::Remove { key } => {
									tx.execute(
										"DELETE FROM unversioned WHERE key = ?1",
										rusqlite::params![key.to_vec()],
									)?;
								}
							}
						}
						tx.commit()?;
						Ok(())
					})();
					let _ = response.send(result);
				}
				WriteCommand::VersionedCommit {
					deltas,
					version,
					transaction,
					timestamp,
					response,
				} => {
					let result =
						Self::handle_versioned_commit(
							&mut conn,
							&mut ensured_tables,
							deltas,
							version,
							transaction,
							timestamp,
						);
					let _ = response.send(result);
				}
				WriteCommand::Shutdown => break,
			}
		}
	}

	/// Handle versioned commit in the writer actor
	fn handle_versioned_commit(
		conn: &mut Connection,
		ensured_tables: &mut HashSet<String>,
		deltas: CowVec<Delta>,
		version: Version,
		transaction: TransactionId,
		timestamp: u64,
	) -> rusqlite::Result<()> {
		use reifydb_core::row::EncodedRow;

		use crate::{
			cdc::{
				CdcTransaction, CdcTransactionChange,
				generate_cdc_change,
			},
			sqlite::{
				cdc::{
					fetch_before_value,
					store_cdc_transaction,
				},
				versioned::{ensure_table_exists, table_name},
			},
		};

		let tx = conn.transaction()?;
		let mut cdc_changes = Vec::new();

		for (idx, delta) in deltas.iter().enumerate() {
			let sequence = match u16::try_from(idx + 1) {
				Ok(seq) => seq,
				Err(_) => return Err(rusqlite::Error::SqliteFailure(
					rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
					Some("Transaction sequence exhausted".to_string()),
				)),
			};

			let table =
				table_name(delta.key()).map_err(|e| {
					rusqlite::Error::SqliteFailure(
					rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
					Some(e.to_string()),
				)
				})?;

			let before_value =
				fetch_before_value(&tx, delta.key(), table)
					.ok()
					.flatten();

			// Apply the data change
			match &delta {
				Delta::Set {
					key,
					row,
				} => {
					let table = table_name(&key).map_err(
						|e| {
							rusqlite::Error::SqliteFailure(
							rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
							Some(e.to_string()),
						)
						},
					)?;

					if table != "versioned"
						&& !ensured_tables
							.contains(table)
					{
						ensure_table_exists(
							&tx, &table,
						);
						ensured_tables
							.insert(table
								.to_string());
					}

					let query = format!(
						"INSERT OR REPLACE INTO {} (key, version, value) VALUES (?1, ?2, ?3)",
						table
					);
					tx.execute(
						&query,
						rusqlite::params![
							key.to_vec(),
							version,
							row.to_vec()
						],
					)?;
				}
				Delta::Remove {
					key,
				} => {
					let table = table_name(&key).map_err(
						|e| {
							rusqlite::Error::SqliteFailure(
							rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
							Some(e.to_string()),
						)
						},
					)?;

					let query = format!(
						"INSERT OR REPLACE INTO {} (key, version, value) VALUES (?1, ?2, ?3)",
						table
					);
					tx.execute(
						&query,
						rusqlite::params![
							key.to_vec(),
							version,
							EncodedRow::deleted()
								.to_vec()
						],
					)?;
				}
			}

			cdc_changes.push(CdcTransactionChange {
				sequence,
				change: generate_cdc_change(
					delta.clone(),
					before_value,
				),
			});
		}

		// Store CDC transaction using optimized format
		if !cdc_changes.is_empty() {
			let cdc_transaction = CdcTransaction::new(
				version,
				timestamp,
				transaction,
				cdc_changes,
			);
			store_cdc_transaction(&tx, cdc_transaction).map_err(
				|e| {
					rusqlite::Error::SqliteFailure(
					rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
					Some(e.to_string()),
				)
				},
			)?;
		}

		tx.commit()?;
		Ok(())
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
			let config = SqliteConfig::safe(
				db_path.join("safe.reifydb"),
			);
			let storage = Sqlite::new(config);

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
			let config = SqliteConfig::fast(
				db_path.join("fast.reifydb"),
			);
			let storage = Sqlite::new(config);

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
			let storage = Sqlite::new(config);

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
			let storage = Sqlite::new(config);

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
			let conn = storage.get_reader();
			let _guard = conn.lock().unwrap();
			Ok(())
		})
		.expect("test failed");
	}

	#[test]
	fn test_tables_created() {
		temp_dir(|db_path| {
			let config = SqliteConfig::new(db_path.join("tables.reifydb"));
			let storage = Sqlite::new(config);
			let conn = storage.get_reader();
			let conn_guard = conn.lock().unwrap();

			// Check that both tables exist
			let mut stmt = conn_guard.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('versioned', 'unversioned')").unwrap();
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
