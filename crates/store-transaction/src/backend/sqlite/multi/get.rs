// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, Result, interface::MultiVersionValues, value::encoded::EncodedValues,
};
use reifydb_type::Error;
use rusqlite::{OptionalExtension, params};

use super::{as_flow_node_state_key, operator_name, source_name};
use crate::backend::{
	diagnostic::database_error, multi::BackendMultiVersionGet, result::MultiVersionGetResult, sqlite::SqliteBackend,
};

/// Helper function to get the appropriate table name for a given key
fn get_table_name(key: &EncodedKey) -> Result<&'static str> {
	// Check if it's a FlowNodeStateKey first
	if as_flow_node_state_key(key).is_some() {
		operator_name(key)
	} else {
		// Use source_name for everything else (RowKey or multi)
		source_name(key)
	}
}

impl BackendMultiVersionGet for SqliteBackend {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<MultiVersionGetResult> {
		let reader = self.get_reader();
		let guard = reader
			.lock()
			.map_err(|e| Error(database_error(format!("Failed to acquire reader lock: {}", e))))?;

		let table = get_table_name(key)?;

		let query = format!(
			"SELECT key, value, version FROM {} WHERE key = ?1 AND version <= ?2 ORDER BY version DESC LIMIT 1",
			table
		);

		let query_result = guard
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
			.optional();

		match query_result {
			Err(e) => {
				// Check if this is a "no such table" error
				if let rusqlite::Error::SqliteFailure(err, msg) = &e {
					if err.code == rusqlite::ErrorCode::Unknown {
						if let Some(msg_str) = msg {
							if msg_str.contains("no such table") {
								// Source doesn't exist - return NotFound
								return Ok(MultiVersionGetResult::NotFound);
							}
						}
					}
				}
				Err(Error(database_error(format!("Database query failed: {}", e))))
			}
			Ok(result) => match result {
				Some(result) => Ok(result),
				None => Ok(MultiVersionGetResult::NotFound),
			},
		}
	}
}
