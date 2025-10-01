// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, EncodedKey, Result,
	interface::{SingleVersionGet, SingleVersionRow},
	value::row::EncodedRow,
};
use rusqlite::{OptionalExtension, params};

use crate::backend::sqlite::Sqlite;

impl SingleVersionGet for Sqlite {
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionRow>> {
		let conn = self.get_reader();
		let conn_guard = conn.lock().unwrap();
		Ok(conn_guard
			.query_row(
				"SELECT key, value FROM single WHERE key = ?1  LIMIT 1",
				params![key.to_vec()],
				|row| {
					Ok(SingleVersionRow {
						key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
						row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
					})
				},
			)
			.optional()
			.unwrap())
	}
}
