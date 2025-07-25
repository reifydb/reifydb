// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::sqlite::Sqlite;
use reifydb_core::interface::{Versioned, VersionedScanRange};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange, Version};
use rusqlite::params;
use std::ops::Bound;

impl VersionedScanRange for Sqlite {
    type ScanRangeIter<'a> = Box<dyn Iterator<Item = Versioned> + Send + 'a>;

    fn scan_range(&self, range: EncodedKeyRange, version: Version) -> Self::ScanRangeIter<'_> {
        let conn = self.get_conn();
        
        // Build query and parameters based on bounds 
        let (query, param_count) = match (&range.start, &range.end) {
            (Bound::Unbounded, Bound::Unbounded) => {
                ("SELECT key, value, version FROM versioned WHERE version <= ? ORDER BY key ASC", 1)
            }
            (Bound::Included(_), Bound::Unbounded) => {
                ("SELECT key, value, version FROM versioned WHERE key >= ? AND version <= ? ORDER BY key ASC", 2)
            }
            (Bound::Excluded(_), Bound::Unbounded) => {
                ("SELECT key, value, version FROM versioned WHERE key > ? AND version <= ? ORDER BY key ASC", 2)
            }
            (Bound::Unbounded, Bound::Included(_)) => {
                ("SELECT key, value, version FROM versioned WHERE key <= ? AND version <= ? ORDER BY key ASC", 2)
            }
            (Bound::Unbounded, Bound::Excluded(_)) => {
                ("SELECT key, value, version FROM versioned WHERE key < ? AND version <= ? ORDER BY key ASC", 2)
            }
            (Bound::Included(_), Bound::Included(_)) => {
                ("SELECT key, value, version FROM versioned WHERE key >= ? AND key <= ? AND version <= ? ORDER BY key ASC", 3)
            }
            (Bound::Included(_), Bound::Excluded(_)) => {
                ("SELECT key, value, version FROM versioned WHERE key >= ? AND key < ? AND version <= ? ORDER BY key ASC", 3)
            }
            (Bound::Excluded(_), Bound::Included(_)) => {
                ("SELECT key, value, version FROM versioned WHERE key > ? AND key <= ? AND version <= ? ORDER BY key ASC", 3)
            }
            (Bound::Excluded(_), Bound::Excluded(_)) => {
                ("SELECT key, value, version FROM versioned WHERE key > ? AND key < ? AND version <= ? ORDER BY key ASC", 3)
            }
        };
        
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
