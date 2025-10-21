// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{CommitVersion, EncodedKey, Result};
use rusqlite::params;

use super::source_name;
use crate::backend::{multi::BackendMultiVersionContains, sqlite::SqliteBackend};

impl BackendMultiVersionContains for SqliteBackend {
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool> {
		let reader = self.get_reader();
		let guard = reader.lock().unwrap();

		let source = source_name(key)?;
		let query = format!("SELECT EXISTS(SELECT 1 FROM {} WHERE key = ? AND version <= ?)", source);

		Ok(guard.query_row(&query, params![key.to_vec(), version.0], |row| row.get(0)).unwrap())
	}
}
