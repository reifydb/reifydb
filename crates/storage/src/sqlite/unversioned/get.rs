// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, EncodedKey, Result,
	interface::{Unversioned, UnversionedGet},
	value::row::EncodedRow,
};
use rusqlite::{OptionalExtension, params};

use crate::sqlite::Sqlite;

impl UnversionedGet for Sqlite {
	fn get(&self, key: &EncodedKey) -> Result<Option<Unversioned>> {
		let conn = self.get_reader();
		let conn_guard = conn.lock().unwrap();
		Ok(conn_guard
			.query_row(
				"SELECT key, value FROM unversioned WHERE key = ?1  LIMIT 1",
				params![key.to_vec()],
				|row| {
					Ok(Unversioned {
						key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
						row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
					})
				},
			)
			.optional()
			.unwrap())
	}
}
