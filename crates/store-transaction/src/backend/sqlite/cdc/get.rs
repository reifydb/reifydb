// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, CowVec, Result, interface::Cdc, value::encoded::EncodedValues};
use rusqlite::{OptionalExtension, params};

use crate::{
	CdcGet,
	cdc::{codec::decode_internal_cdc, converter::CdcConverter},
	sqlite::SqliteBackend,
};

impl CdcGet for SqliteBackend {
	fn get(&self, version: CommitVersion) -> Result<Option<Cdc>> {
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
			// Decode the internal CDC which has version references
			let internal_cdc = decode_internal_cdc(&encoded_transaction)?;

			// Convert to public CDC using the converter
			let cdc = self.convert(internal_cdc)?;
			Ok(Some(cdc))
		} else {
			Ok(None)
		}
	}
}
