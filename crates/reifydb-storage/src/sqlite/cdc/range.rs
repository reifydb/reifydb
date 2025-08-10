// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::cdc::codec::decode_cdc_event;
use crate::sqlite::Sqlite;
use reifydb_core::interface::{CdcEvent, CdcRange};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, Result, Version};
use rusqlite::params;

impl CdcRange for Sqlite {
    fn range(&self, start_version: Version, end_version: Version) -> Result<Vec<CdcEvent>> {
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
}