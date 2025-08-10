// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::cdc::codec::decode_cdc_event;
use crate::sqlite::Sqlite;
use reifydb_core::interface::{CdcEvent, CdcScan};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, Result};

impl CdcScan for Sqlite {
    fn scan(&self) -> Result<Vec<CdcEvent>> {
        let conn = self.get_conn();

        let mut stmt = conn.prepare_cached("SELECT value FROM cdc ORDER BY version ASC, key DESC").unwrap();

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
}