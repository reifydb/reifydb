// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fs;

use rusqlite::{Connection, OpenFlags as SqliteOpenFlags};

use crate::{DbPath, OpenFlags, error::SqliteError};

/// Connect to a SQLite database.
pub fn connect(path: &DbPath, flags: SqliteOpenFlags) -> Result<Connection, SqliteError> {
	match path {
		DbPath::File(path) => {
			let path_str = path.to_string_lossy();
			let is_uri = path_str.contains(':');

			if is_uri {
				let uri_flags = flags | SqliteOpenFlags::SQLITE_OPEN_URI;
				let path_string = path_str.to_string();
				Connection::open_with_flags(path_string, uri_flags).map_err(|source| {
					SqliteError::Connect {
						path: path_str.to_string(),
						source,
					}
				})
			} else {
				let path_clone = path.clone();
				Connection::open_with_flags(path_clone, flags).map_err(|source| SqliteError::Connect {
					path: path.display().to_string(),
					source,
				})
			}
		}
		DbPath::Tmpfs(path) => {
			let path_clone = path.clone();
			Connection::open_with_flags(path_clone, flags).map_err(|source| SqliteError::Connect {
				path: path.display().to_string(),
				source,
			})
		}
		DbPath::Memory(path) => {
			let path_clone = path.clone();
			Connection::open_with_flags(path_clone, flags).map_err(|source| SqliteError::Connect {
				path: path.display().to_string(),
				source,
			})
		}
	}
}

/// Resolve the database path, creating directories as needed.
///
/// `default_filename` is appended when the caller passes a `DbPath::File`
/// that points at a directory (no extension), so each subsystem can keep its
/// own default (e.g. `"cdc.db"` or `"primitive.db"`).
pub fn resolve_db_path(db_path: DbPath, default_filename: &str) -> DbPath {
	match db_path {
		DbPath::Tmpfs(path) => {
			if let Some(parent) = path.parent() {
				fs::create_dir_all(parent).ok();
			}
			DbPath::Tmpfs(path)
		}
		DbPath::Memory(path) => {
			if let Some(parent) = path.parent() {
				fs::create_dir_all(parent).ok();
			}
			DbPath::Memory(path)
		}
		DbPath::File(config_path) => {
			let is_uri = config_path.to_string_lossy().contains(':');
			if is_uri {
				DbPath::File(config_path)
			} else if config_path.extension().is_none() {
				fs::create_dir_all(&config_path).ok();
				DbPath::File(config_path.join(default_filename))
			} else {
				if let Some(parent) = config_path.parent() {
					fs::create_dir_all(parent).ok();
				}
				DbPath::File(config_path)
			}
		}
	}
}

/// Convert our `OpenFlags` to `rusqlite::OpenFlags`.
pub fn convert_flags(flags: &OpenFlags) -> SqliteOpenFlags {
	let mut rusqlite_flags = SqliteOpenFlags::empty();

	if flags.read_write {
		rusqlite_flags |= SqliteOpenFlags::SQLITE_OPEN_READ_WRITE;
	}
	if flags.create {
		rusqlite_flags |= SqliteOpenFlags::SQLITE_OPEN_CREATE;
	}
	if flags.full_mutex {
		rusqlite_flags |= SqliteOpenFlags::SQLITE_OPEN_FULL_MUTEX;
	}
	if flags.no_mutex {
		rusqlite_flags |= SqliteOpenFlags::SQLITE_OPEN_NO_MUTEX;
	}
	if flags.shared_cache {
		rusqlite_flags |= SqliteOpenFlags::SQLITE_OPEN_SHARED_CACHE;
	}
	if flags.private_cache {
		rusqlite_flags |= SqliteOpenFlags::SQLITE_OPEN_PRIVATE_CACHE;
	}
	if flags.uri {
		rusqlite_flags |= SqliteOpenFlags::SQLITE_OPEN_URI;
	}

	rusqlite_flags
}
