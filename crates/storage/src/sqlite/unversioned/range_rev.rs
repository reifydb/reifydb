// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::sqlite::Sqlite;
use reifydb_core::interface::{Unversioned, UnversionedScanRangeRev};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange, Error};
use rusqlite::params;
use std::ops::Bound;

impl UnversionedScanRangeRev for Sqlite {
    type ScanRangeRev<'a> = Box<dyn Iterator<Item = Unversioned> + Send + 'a>
    where
        Self: 'a;

    fn scan_range_rev(&self, range: EncodedKeyRange) -> Result<Self::ScanRangeRev<'_>, Error> {
        let conn = self.get_conn();
        
        // Build query and parameters based on bounds
        let (query, param_count) = match (&range.start, &range.end) {
            (Bound::Unbounded, Bound::Unbounded) => {
                ("SELECT key, value FROM unversioned ORDER BY key DESC", 0)
            }
            (Bound::Included(_), Bound::Unbounded) => {
                ("SELECT key, value FROM unversioned WHERE key >= ? ORDER BY key DESC", 1)
            }
            (Bound::Excluded(_), Bound::Unbounded) => {
                ("SELECT key, value FROM unversioned WHERE key > ? ORDER BY key DESC", 1)
            }
            (Bound::Unbounded, Bound::Included(_)) => {
                ("SELECT key, value FROM unversioned WHERE key <= ? ORDER BY key DESC", 1)
            }
            (Bound::Unbounded, Bound::Excluded(_)) => {
                ("SELECT key, value FROM unversioned WHERE key < ? ORDER BY key DESC", 1)
            }
            (Bound::Included(_), Bound::Included(_)) => {
                ("SELECT key, value FROM unversioned WHERE key >= ? AND key <= ? ORDER BY key DESC", 2)
            }
            (Bound::Included(_), Bound::Excluded(_)) => {
                ("SELECT key, value FROM unversioned WHERE key >= ? AND key < ? ORDER BY key DESC", 2)
            }
            (Bound::Excluded(_), Bound::Included(_)) => {
                ("SELECT key, value FROM unversioned WHERE key > ? AND key <= ? ORDER BY key DESC", 2)
            }
            (Bound::Excluded(_), Bound::Excluded(_)) => {
                ("SELECT key, value FROM unversioned WHERE key > ? AND key < ? ORDER BY key DESC", 2)
            }
        };
        
        let mut stmt = conn.prepare(&query).unwrap();
        
        let rows = match param_count {
            0 => {
                stmt.query_map(params![], |row| {
                    Ok(Unversioned {
                        key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                        row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
                    })
                }).unwrap().map(Result::unwrap).collect::<Vec<_>>()
            }
            1 => {
                let param = match (&range.start, &range.end) {
                    (Bound::Included(key), _) | (Bound::Excluded(key), _) => key.to_vec(),
                    (_, Bound::Included(key)) | (_, Bound::Excluded(key)) => key.to_vec(),
                    _ => unreachable!(),
                };
                stmt.query_map(params![param], |row| {
                    Ok(Unversioned {
                        key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                        row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
                    })
                }).unwrap().map(Result::unwrap).collect::<Vec<_>>()
            }
            2 => {
                let start_param = match &range.start {
                    Bound::Included(key) | Bound::Excluded(key) => key.to_vec(),
                    _ => unreachable!(),
                };
                let end_param = match &range.end {
                    Bound::Included(key) | Bound::Excluded(key) => key.to_vec(),
                    _ => unreachable!(),
                };
                stmt.query_map(params![start_param, end_param], |row| {
                    Ok(Unversioned {
                        key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                        row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
                    })
                }).unwrap().map(Result::unwrap).collect::<Vec<_>>()
            }
            _ => unreachable!(),
        };

        Ok(Box::new(rows.into_iter()))
    }
}
