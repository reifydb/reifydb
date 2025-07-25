// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::sqlite::Sqlite;
use super::table_name_for_range;
use reifydb_core::interface::{Versioned, VersionedScanRangeRev};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange, Version};
use rusqlite::params;
use std::ops::Bound;

impl VersionedScanRangeRev for Sqlite {
    type ScanRangeIterRev<'a> = Box<dyn Iterator<Item = Versioned> + Send + 'a>;

    fn scan_range_rev(
        &self,
        range: EncodedKeyRange,
        version: Version,
    ) -> Self::ScanRangeIterRev<'_> {
        let conn = self.get_conn();
        
        let table = table_name_for_range(&range);
        
        // Build query and parameters based on bounds
        let (query_template, param_count) = match (&range.start, &range.end) {
            (Bound::Unbounded, Bound::Unbounded) => {
                ("SELECT key, value, version FROM {} WHERE version <= ? ORDER BY key DESC", 1)
            }
            (Bound::Included(_), Bound::Unbounded) => {
                ("SELECT key, value, version FROM {} WHERE key >= ? AND version <= ? ORDER BY key DESC", 2)
            }
            (Bound::Excluded(_), Bound::Unbounded) => {
                ("SELECT key, value, version FROM {} WHERE key > ? AND version <= ? ORDER BY key DESC", 2)
            }
            (Bound::Unbounded, Bound::Included(_)) => {
                ("SELECT key, value, version FROM {} WHERE key <= ? AND version <= ? ORDER BY key DESC", 2)
            }
            (Bound::Unbounded, Bound::Excluded(_)) => {
                ("SELECT key, value, version FROM {} WHERE key < ? AND version <= ? ORDER BY key DESC", 2)
            }
            (Bound::Included(_), Bound::Included(_)) => {
                ("SELECT key, value, version FROM {} WHERE key >= ? AND key <= ? AND version <= ? ORDER BY key DESC", 3)
            }
            (Bound::Included(_), Bound::Excluded(_)) => {
                ("SELECT key, value, version FROM {} WHERE key >= ? AND key < ? AND version <= ? ORDER BY key DESC", 3)
            }
            (Bound::Excluded(_), Bound::Included(_)) => {
                ("SELECT key, value, version FROM {} WHERE key > ? AND key <= ? AND version <= ? ORDER BY key DESC", 3)
            }
            (Bound::Excluded(_), Bound::Excluded(_)) => {
                ("SELECT key, value, version FROM {} WHERE key > ? AND key < ? AND version <= ? ORDER BY key DESC", 3)
            }
        };
        
        let query = query_template.replace("{}", table);
        let mut stmt = conn.prepare(&query).unwrap();
        
        let rows = match param_count {
            1 => {
                stmt.query_map(params![version], |row| {
                    Ok(Versioned {
                        key: EncodedKey(CowVec::new(row.get(0)?)),
                        row: EncodedRow(CowVec::new(row.get(1)?)),
                        version: row.get(2)?,
                    })
                }).unwrap().map(Result::unwrap).collect::<Vec<_>>()
            }
            2 => {
                let param = match (&range.start, &range.end) {
                    (Bound::Included(key), _) | (Bound::Excluded(key), _) => key.to_vec(),
                    (_, Bound::Included(key)) | (_, Bound::Excluded(key)) => key.to_vec(),
                    _ => unreachable!(),
                };
                stmt.query_map(params![param, version], |row| {
                    Ok(Versioned {
                        key: EncodedKey(CowVec::new(row.get(0)?)),
                        row: EncodedRow(CowVec::new(row.get(1)?)),
                        version: row.get(2)?,
                    })
                }).unwrap().map(Result::unwrap).collect::<Vec<_>>()
            }
            3 => {
                let start_param = match &range.start {
                    Bound::Included(key) | Bound::Excluded(key) => key.to_vec(),
                    _ => unreachable!(),
                };
                let end_param = match &range.end {
                    Bound::Included(key) | Bound::Excluded(key) => key.to_vec(),
                    _ => unreachable!(),
                };
                stmt.query_map(params![start_param, end_param, version], |row| {
                    Ok(Versioned {
                        key: EncodedKey(CowVec::new(row.get(0)?)),
                        row: EncodedRow(CowVec::new(row.get(1)?)),
                        version: row.get(2)?,
                    })
                }).unwrap().map(Result::unwrap).collect::<Vec<_>>()
            }
            _ => unreachable!(),
        };

        Box::new(rows.into_iter())
    }
}