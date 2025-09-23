// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{CommitVersion, EncodedKey, Result, interface::MultiVersionContains};
use rusqlite::params;

use super::table_name;
use crate::sqlite::Sqlite;

impl MultiVersionContains for Sqlite {
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool> {
		let conn = self.get_reader();
		let conn_guard = conn.lock().unwrap();

		let table = table_name(key)?;
		let query = format!("SELECT EXISTS(SELECT 1 FROM {} WHERE key = ? AND version <= ?)", table);

		Ok(conn_guard.query_row(&query, params![key.to_vec(), version], |row| row.get(0)).unwrap())
	}
}
