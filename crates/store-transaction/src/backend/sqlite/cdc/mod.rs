// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use rusqlite::{Error::ToSqlConversionFailure, Transaction, params};

use crate::cdc::{InternalCdc, codec::encode_internal_cdc};

mod count;
mod get;
mod range;
mod scan;

pub use range::CdcRangeIter;
pub use scan::CdcScanIter;

/// Store an internal CDC transaction in the database
pub(crate) fn store_internal_cdc(tx: &Transaction, transaction: InternalCdc) -> rusqlite::Result<()> {
	let encoded_transaction =
		encode_internal_cdc(&transaction).map_err(|e| ToSqlConversionFailure(Box::new(e)))?;

	tx.execute(
		"INSERT OR REPLACE INTO cdc (version, value) VALUES (?1, ?2)",
		params![transaction.version.0, encoded_transaction.to_vec()],
	)?;

	Ok(())
}
