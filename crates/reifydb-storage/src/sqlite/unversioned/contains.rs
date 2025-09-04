// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{EncodedKey, Result, interface::UnversionedContains};
use rusqlite::params;

use crate::sqlite::Sqlite;

impl UnversionedContains for Sqlite {
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		let conn = self.get_reader();
		let conn_guard = conn.lock().unwrap();
		let exists: bool = conn_guard
			.query_row(
				"SELECT EXISTS(SELECT 1 FROM unversioned WHERE key = ?)",
				params![key.to_vec()],
				|row| row.get(0),
			)
			.unwrap();
		Ok(exists)
	}
}
