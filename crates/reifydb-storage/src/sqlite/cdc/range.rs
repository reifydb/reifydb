// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::cdc::codec::decode_cdc_event;
use crate::sqlite::Sqlite;
use reifydb_core::interface::{CdcEvent, CdcRange};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, Result, Version};
use rusqlite::params;
use std::ops::Bound;

impl CdcRange for Sqlite {
    fn range(&self, start: Bound<Version>, end: Bound<Version>) -> Result<Vec<CdcEvent>> {
        let conn = self.get_conn();

        let (where_clause, param_count) = match (&start, &end) {
            (Bound::Unbounded, Bound::Unbounded) => (String::new(), 0),
            (Bound::Unbounded, Bound::Included(_)) => ("WHERE version <= ?".to_string(), 1),
            (Bound::Unbounded, Bound::Excluded(_)) => ("WHERE version < ?".to_string(), 1),
            (Bound::Included(_), Bound::Unbounded) => ("WHERE version >= ?".to_string(), 1),
            (Bound::Excluded(_), Bound::Unbounded) => ("WHERE version > ?".to_string(), 1),
            (Bound::Included(_), Bound::Included(_)) => {
                ("WHERE version >= ? AND version <= ?".to_string(), 2)
            }
            (Bound::Included(_), Bound::Excluded(_)) => {
                ("WHERE version >= ? AND version < ?".to_string(), 2)
            }
            (Bound::Excluded(_), Bound::Included(_)) => {
                ("WHERE version > ? AND version <= ?".to_string(), 2)
            }
            (Bound::Excluded(_), Bound::Excluded(_)) => {
                ("WHERE version > ? AND version < ?".to_string(), 2)
            }
        };

        let query = if where_clause.is_empty() {
            "SELECT value FROM cdc ORDER BY version ASC, key DESC".to_string()
        } else {
            format!("SELECT value FROM cdc {} ORDER BY version ASC, key DESC", where_clause)
        };

        let mut stmt = conn.prepare_cached(&query).unwrap();

        let events = match param_count {
            0 => stmt
                .query_map(params![], |row| {
                    let bytes: Vec<u8> = row.get(0)?;
                    Ok(EncodedRow(CowVec::new(bytes)))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap(),
            1 => {
                let version = match (&start, &end) {
                    (_, Bound::Included(v)) | (_, Bound::Excluded(v)) => *v,
                    (Bound::Included(v), _) | (Bound::Excluded(v), _) => *v,
                    _ => unreachable!(),
                };
                stmt.query_map(params![version as i64], |row| {
                    let bytes: Vec<u8> = row.get(0)?;
                    Ok(EncodedRow(CowVec::new(bytes)))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
            }
            2 => {
                let start_version = match start {
                    Bound::Included(v) | Bound::Excluded(v) => v,
                    Bound::Unbounded => unreachable!(),
                };
                let end_version = match end {
                    Bound::Included(v) | Bound::Excluded(v) => v,
                    Bound::Unbounded => unreachable!(),
                };
                stmt.query_map(params![start_version as i64, end_version as i64], |row| {
                    let bytes: Vec<u8> = row.get(0)?;
                    Ok(EncodedRow(CowVec::new(bytes)))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
            }
            _ => unreachable!(),
        };

        let mut result = Vec::new();
        for encoded in events {
            result.push(decode_cdc_event(&encoded)?);
        }

        Ok(result)
    }
}
