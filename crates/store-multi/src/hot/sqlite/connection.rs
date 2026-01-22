// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! SQLite connection utilities.

use reifydb_core::error::diagnostic::internal::internal;
use reifydb_type::{Result, error};
use rusqlite::Connection;

use super::DbPath;

/// Connect to a SQLite database asynchronously.
pub(super) fn connect(path: &DbPath, flags: rusqlite::OpenFlags) -> Result<Connection> {
	fn connection_failed(path: String, error: String) -> String {
		format!("Failed to connect to database at {}: {}", path, error)
	}

	match path {
		DbPath::File(path) => {
			let path_str = path.to_string_lossy();
			let is_uri = path_str.contains(':');

			if is_uri {
				let uri_flags = flags | rusqlite::OpenFlags::SQLITE_OPEN_URI;
				let path_string = path_str.to_string();
				Connection::open_with_flags(path_string, uri_flags).map_err(|e| {
					error!(internal(connection_failed(path_str.to_string(), e.to_string())))
				})
			} else {
				let path_clone = path.clone();
				Connection::open_with_flags(path_clone, flags).map_err(|e| {
					error!(internal(connection_failed(path.display().to_string(), e.to_string())))
				})
			}
		}
		DbPath::Tmpfs(path) => {
			let path_clone = path.clone();
			Connection::open_with_flags(path_clone, flags).map_err(|e| {
				error!(internal(connection_failed(path.display().to_string(), e.to_string())))
			})
		}
		DbPath::Memory(path) => {
			let path_clone = path.clone();
			Connection::open_with_flags(path_clone, flags).map_err(|e| {
				error!(internal(connection_failed(path.display().to_string(), e.to_string())))
			})
		}
	}
}

/// Resolve the database path, creating directories as needed.
pub(super) fn resolve_db_path(db_path: DbPath) -> DbPath {
	match db_path {
		DbPath::Tmpfs(path) => {
			if let Some(parent) = path.parent() {
				std::fs::create_dir_all(parent).ok();
			}
			DbPath::Tmpfs(path)
		}
		DbPath::Memory(path) => {
			if let Some(parent) = path.parent() {
				std::fs::create_dir_all(parent).ok();
			}
			DbPath::Memory(path)
		}
		DbPath::File(config_path) => {
			let is_uri = config_path.to_string_lossy().contains(':');
			if is_uri {
				DbPath::File(config_path)
			} else if config_path.extension().is_none() {
				std::fs::create_dir_all(&config_path).ok();
				DbPath::File(config_path.join("primitive.db"))
			} else {
				if let Some(parent) = config_path.parent() {
					std::fs::create_dir_all(parent).ok();
				}
				DbPath::File(config_path)
			}
		}
	}
}

/// Convert our OpenFlags to rusqlite OpenFlags.
pub(super) fn convert_flags(flags: &super::OpenFlags) -> rusqlite::OpenFlags {
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
