// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{
	CowVec, EncodedKey, Result, Version,
	interface::{Versioned, VersionedGet},
	row::EncodedRow,
};
use rusqlite::{OptionalExtension, params};

use super::table_name;
use crate::sqlite::Sqlite;

impl VersionedGet for Sqlite {
	fn get(
		&self,
		key: &EncodedKey,
		version: Version,
	) -> Result<Option<Versioned>> {
		let conn = self.get_conn();

		let table = table_name(key)?;
		let query = format!(
			"SELECT key, value, version FROM {} WHERE key = ?1 AND version <= ?2 ORDER BY version DESC LIMIT 1",
			table
		);

		Ok(conn.query_row(
			&query,
			params![key.to_vec(), version],
			|row| {
				let encoded_row: EncodedRow = EncodedRow(
					CowVec::new(row.get::<_, Vec<u8>>(1)?),
				);
				if encoded_row.is_deleted() {
					Ok(None)
				} else {
					Ok(Some(Versioned {
						key: EncodedKey::new(
							row.get::<_, Vec<u8>>(
								0,
							)?,
						),
						row: encoded_row,
						version: row.get(2)?,
					}))
				}
			},
		)
		.optional()
		.unwrap()
		.flatten())
	}
}
