// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, Result,
	interface::{MultiVersionGet, MultiVersionRow},
	value::row::EncodedRow,
};
use rusqlite::{OptionalExtension, params};

use super::table_name;
use crate::sqlite::Sqlite;

impl MultiVersionGet for Sqlite {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>> {
		let conn = self.get_reader();
		let conn_guard = conn.lock().unwrap();

		let table = table_name(key)?;
		let query = format!(
			"SELECT key, value, version FROM {} WHERE key = ?1 AND version <= ?2 ORDER BY version DESC LIMIT 1",
			table
		);

		Ok(conn_guard
			.query_row(&query, params![key.to_vec(), version], |row| {
				// Check if value is NULL (which indicates deletion)
				let value: Option<Vec<u8>> = row.get(1)?;
				match value {
					Some(val) => {
						let encoded_row = EncodedRow(CowVec::new(val));
						Ok(Some(MultiVersionRow {
							key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
							row: encoded_row,
							version: row.get(2)?,
						}))
					}
					None => Ok(None), // NULL value means deleted
				}
			})
			.optional()
			.unwrap()
			.flatten())
	}
}
