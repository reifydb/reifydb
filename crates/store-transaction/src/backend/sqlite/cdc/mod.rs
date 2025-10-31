// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use rusqlite::{Error::ToSqlConversionFailure, Transaction, params_from_iter, types::Value};

use crate::cdc::{InternalCdc, codec::encode_internal_cdc};

mod count;
mod get;
mod range;
mod scan;

pub use range::CdcRangeIter;
pub use scan::CdcScanIter;

const CDC_BATCH_SIZE: usize = 499; // 999 params / 2 columns (SQLite max)

/// Store multiple internal CDC transactions in a single batched insert
pub(crate) fn store_cdc_changes(tx: &Transaction, cdc_entries: Vec<InternalCdc>) -> rusqlite::Result<()> {
	if cdc_entries.is_empty() {
		return Ok(());
	}

	for chunk in cdc_entries.chunks(CDC_BATCH_SIZE) {
		if chunk.is_empty() {
			continue;
		}

		// Build the multi-row INSERT statement
		let placeholders: Vec<String> = (0..chunk.len())
			.map(|i| {
				let base = i * 2;
				format!("(?{}, ?{})", base + 1, base + 2)
			})
			.collect();

		let query = format!("INSERT OR REPLACE INTO cdc (version, value) VALUES {}", placeholders.join(", "));

		// Collect all parameters
		let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 2);
		for cdc_entry in chunk {
			let encoded =
				encode_internal_cdc(cdc_entry).map_err(|e| ToSqlConversionFailure(Box::new(e)))?;

			params.push(Value::Integer(cdc_entry.version.0 as i64));
			params.push(Value::Blob(encoded.to_vec()));
		}

		tx.execute(&query, params_from_iter(params))?;
	}

	Ok(())
}
