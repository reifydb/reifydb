// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CowVec, EncodedKey, Result, interface::SingleVersionValues, value::encoded::EncodedValues};
use rusqlite::{OptionalExtension, params};

use crate::{SingleVersionGet, backend::sqlite::SqliteBackend};

impl SingleVersionGet for SqliteBackend {
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionValues>> {
		let reader = self.get_reader();
		let guard = reader.lock().unwrap();
		Ok(guard.query_row(
			"SELECT key, value FROM single WHERE key = ?1  LIMIT 1",
			params![key.to_vec()],
			|row| {
				Ok(SingleVersionValues {
					key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
					values: EncodedValues(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
				})
			},
		)
		.optional()
		.unwrap())
	}
}
