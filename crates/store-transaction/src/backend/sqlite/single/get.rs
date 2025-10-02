// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CowVec, EncodedKey, Result, interface::SingleVersionValues, value::encoded::EncodedValues};
use rusqlite::{OptionalExtension, params};

use crate::backend::{result::SingleVersionGetResult, single::BackendSingleVersionGet, sqlite::SqliteBackend};

impl BackendSingleVersionGet for SqliteBackend {
	fn get(&self, key: &EncodedKey) -> Result<SingleVersionGetResult> {
		let reader = self.get_reader();
		let guard = reader.lock().unwrap();
		match guard
			.query_row(
				"SELECT key, value FROM single WHERE key = ?1  LIMIT 1",
				params![key.to_vec()],
				|row| {
					let key = EncodedKey::new(row.get::<_, Vec<u8>>(0)?);
					let value: Option<Vec<u8>> = row.get(1)?;
					match value {
						Some(val) => Ok(SingleVersionGetResult::Value(SingleVersionValues {
							key,
							values: EncodedValues(CowVec::new(val)),
						})),
						None => Ok(SingleVersionGetResult::Tombstone {
							key,
						}),
					}
				},
			)
			.optional()
			.unwrap()
		{
			Some(result) => Ok(result),
			None => Ok(SingleVersionGetResult::NotFound),
		}
	}
}
