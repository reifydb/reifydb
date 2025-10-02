// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, CowVec, Result, interface::CdcCount, value::encoded::EncodedValues};
use rusqlite::{OptionalExtension, params};

use crate::{cdc::codec::decode_cdc_transaction, sqlite::SqliteBackend};

impl CdcCount for SqliteBackend {
	fn count(&self, version: CommitVersion) -> Result<usize> {
		let conn = self.get_reader();
		let conn_guard = conn.lock().unwrap();

		let mut stmt = conn_guard.prepare_cached("SELECT value FROM cdc WHERE version = ?").unwrap();

		let result = stmt
			.query_row(params![version.0], |values| {
				let bytes: Vec<u8> = values.get(0)?;
				Ok(EncodedValues(CowVec::new(bytes)))
			})
			.optional()
			.unwrap();

		if let Some(encoded_transaction) = result {
			let transaction = decode_cdc_transaction(&encoded_transaction)?;
			Ok(transaction.changes.len())
		} else {
			Ok(0)
		}
	}
}
