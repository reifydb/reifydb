// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CowVec, EncodedKey, interface::Cdc, value::encoded::EncodedValues};
use rusqlite::{Error::ToSqlConversionFailure, OptionalExtension, Transaction, params};

use crate::cdc::codec::encode_cdc_transaction;

mod count;
mod get;
mod range;
mod scan;

/// Helper to fetch the current value of a key before it's modified
pub(crate) fn fetch_pre_value(
	tx: &Transaction,
	key: &EncodedKey,
	table: &str,
) -> rusqlite::Result<Option<EncodedValues>> {
	let query = format!("SELECT value FROM {} WHERE key = ? ORDER BY version DESC LIMIT 1", table);

	let mut stmt = tx.prepare_cached(&query)?;

	stmt.query_row(params![key.to_vec()], |row| {
		let value: Vec<u8> = row.get(0)?;
		Ok(EncodedValues(CowVec::new(value)))
	})
	.optional()
}

/// Store a CDC transaction in the database
pub(crate) fn store_cdc_transaction(tx: &Transaction, transaction: Cdc) -> rusqlite::Result<()> {
	let encoded_transaction =
		encode_cdc_transaction(&transaction).map_err(|e| ToSqlConversionFailure(Box::new(e)))?;

	tx.execute(
		"INSERT OR REPLACE INTO cdc (version, value) VALUES (?1, ?2)",
		params![transaction.version, encoded_transaction.to_vec()],
	)?;

	Ok(())
}
