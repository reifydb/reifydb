// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, Result, interface::MultiVersionValues, value::encoded::EncodedValues,
};
use rusqlite::{OptionalExtension, params};

use super::table_name;
use crate::backend::{multi::BackendMultiVersionGet, result::MultiVersionGetResult, sqlite::SqliteBackend};

impl BackendMultiVersionGet for SqliteBackend {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<MultiVersionGetResult> {
		let reader = self.get_reader();
		let guard = reader.lock().unwrap();

		let table = table_name(key)?;
		let query = format!(
			"SELECT key, value, version FROM {} WHERE key = ?1 AND version <= ?2 ORDER BY version DESC LIMIT 1",
			table
		);

		match guard
			.query_row(&query, params![key.to_vec(), version.0], |row| {
				let value: Option<Vec<u8>> = row.get(1)?;
				let version = CommitVersion(row.get(2)?);
				match value {
					Some(val) => {
						let encoded_row = EncodedValues(CowVec::new(val));
						Ok(MultiVersionGetResult::Value(MultiVersionValues {
							key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
							values: encoded_row,
							version,
						}))
					}
					None => Ok(MultiVersionGetResult::Tombstone {
						key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
						version,
					}),
				}
			})
			.optional()
			.unwrap()
		{
			Some(result) => Ok(result),
			None => Ok(MultiVersionGetResult::NotFound),
		}
	}
}
