// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, Result, Version, interface::CdcCount, row::EncodedRow,
};
use rusqlite::{OptionalExtension, params};

use crate::{cdc::codec::decode_cdc_transaction, sqlite::Sqlite};

impl CdcCount for Sqlite {
	fn count(&self, version: Version) -> Result<usize> {
		let conn = self.get_conn();

		let mut stmt = conn
			.prepare_cached(
				"SELECT value FROM cdc WHERE version = ?",
			)
			.unwrap();

		let result = stmt
			.query_row(params![version as i64], |row| {
				let bytes: Vec<u8> = row.get(0)?;
				Ok(EncodedRow(CowVec::new(bytes)))
			})
			.optional()
			.unwrap();

		if let Some(encoded_transaction) = result {
			let transaction =
				decode_cdc_transaction(&encoded_transaction)?;
			Ok(transaction.changes.len())
		} else {
			Ok(0)
		}
	}
}
