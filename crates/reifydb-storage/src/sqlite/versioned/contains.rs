// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{EncodedKey, Result, Version, interface::VersionedContains};
use rusqlite::params;

use super::table_name;
use crate::sqlite::Sqlite;

impl VersionedContains for Sqlite {
	fn contains(&self, key: &EncodedKey, version: Version) -> Result<bool> {
		let conn = self.get_conn();

		let table = table_name(key);
		let query = format!(
			"SELECT EXISTS(SELECT 1 FROM {} WHERE key = ? AND version <= ?)",
			table
		);

		Ok(conn.query_row(
			&query,
			params![key.to_vec(), version],
			|row| row.get(0),
		)
		.unwrap())
	}
}
