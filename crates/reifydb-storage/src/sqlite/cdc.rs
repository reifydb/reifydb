// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::cdc::codec::{decode_cdc_event, encode_cdc_event};
use crate::sqlite::Sqlite;
use reifydb_core::interface::{CdcEvent, CdcEventKey, CdcStorage, EncodableKey};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, Result, Version};
use rusqlite::{OptionalExtension, Transaction, params};

/// Helper to fetch the current value of a key before it's modified
pub(super) fn fetch_before_value(
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
pub(super) fn store_cdc_event(
    tx: &Transaction,
    event: CdcEvent,
    version: Version,
    sequence: u16,
) -> rusqlite::Result<()> {
    let cdc_key = CdcEventKey { version, sequence };
    let encoded_event = encode_cdc_event(&event)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    tx.execute(
        "INSERT INTO cdc (key, version, value) VALUES (?1, ?2, ?3)",
        params![cdc_key.encode().to_vec(), version, encoded_event.to_vec()],
    )?;

    Ok(())
}

impl CdcStorage for Sqlite {
    fn get_cdc_event(&self, version: Version, sequence: u16) -> Result<Option<CdcEvent>> {
        let conn = self.get_conn();
        let cdc_key = CdcEventKey { version, sequence };

        let mut stmt = conn.prepare_cached("SELECT value FROM cdc WHERE key = ?").unwrap();

        let event_bytes: Option<Vec<u8>> = stmt
            .query_row(params![cdc_key.encode().to_vec()], |row| row.get(0))
            .optional()
            .unwrap();

        if let Some(bytes) = event_bytes {
            let event = decode_cdc_event(&EncodedRow(CowVec::new(bytes)))?;
            Ok(Some(event))
        } else {
            Ok(None)
        }
    }

    fn cdc_range(&self, start_version: Version, end_version: Version) -> Result<Vec<CdcEvent>> {
        let conn = self.get_conn();

        let mut stmt = conn
            .prepare_cached(
                "SELECT value FROM cdc WHERE version >= ? AND version <= ? ORDER BY version, key",
            )
            .unwrap();

        let events = stmt
            .query_map(params![start_version as i64, end_version as i64], |row| {
                let bytes: Vec<u8> = row.get(0)?;
                Ok(EncodedRow(CowVec::new(bytes)))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();

        let mut result = Vec::new();
        for encoded in events {
            result.push(decode_cdc_event(&encoded)?);
        }

        Ok(result)
    }

    fn cdc_scan(&self, limit: Option<usize>) -> Result<Vec<CdcEvent>> {
        let conn = self.get_conn();

        let query = if let Some(limit) = limit {
            format!("SELECT value FROM cdc ORDER BY version, key LIMIT {}", limit)
        } else {
            "SELECT value FROM cdc ORDER BY version, key".to_string()
        };

        let mut stmt = conn.prepare_cached(&query).unwrap();

        let events = stmt
            .query_map([], |row| {
                let bytes: Vec<u8> = row.get(0)?;
                Ok(EncodedRow(CowVec::new(bytes)))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();

        let mut result = Vec::new();
        for encoded in events {
            result.push(decode_cdc_event(&encoded)?);
        }

        Ok(result)
    }

    fn cdc_count(&self, version: Version) -> Result<usize> {
        let conn = self.get_conn();

        let mut stmt = conn.prepare_cached("SELECT COUNT(*) FROM cdc WHERE version = ?").unwrap();

        let count: usize = stmt.query_row(params![version as i64], |row| row.get(0)).unwrap();

        Ok(count)
    }

    fn cdc_events_for_version(&self, version: Version) -> Result<Vec<CdcEvent>> {
        self.cdc_range(version, version)
    }
}
