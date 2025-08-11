// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::cdc::codec::encode_cdc_event;
use reifydb_core::interface::{CdcEvent, CdcEventKey, EncodableKey};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, Version};
use rusqlite::{OptionalExtension, Transaction, params};

mod count;
mod get;
mod range;
mod scan;

/// Helper to fetch the current value of a key before it's modified
pub(crate) fn fetch_before_value(
    tx: &Transaction,
    key: &EncodedKey,
    table: &str,
) -> rusqlite::Result<Option<EncodedRow>> {
    let query = format!("SELECT value FROM {} WHERE key = ? ORDER BY version DESC LIMIT 1", table);

    let mut stmt = tx.prepare_cached(&query)?;

    stmt.query_row(params![key.to_vec()], |row| {
        let value: Vec<u8> = row.get(0)?;
        Ok(EncodedRow(CowVec::new(value)))
    })
    .optional()
}

/// Store a CDC event in the database
pub(crate) fn store_cdc_event(
    tx: &Transaction,
    event: CdcEvent,
    version: Version,
    sequence: u16,
) -> rusqlite::Result<()> {
    let cdc_key = CdcEventKey { version, sequence };
    let encoded_event = encode_cdc_event(&event)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    tx.execute(
        "INSERT OR REPLACE INTO cdc (key, version, value) VALUES (?1, ?2, ?3)",
        params![cdc_key.encode().to_vec(), version, encoded_event.to_vec()],
    )?;

    Ok(())
}
