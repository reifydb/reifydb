// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

mod apply;
mod contains;
mod get;
mod iter;
mod iter_rev;
mod range;
mod range_rev;

use reifydb_core::interface::Unversioned;
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange};
use rusqlite::Statement;
use std::ops::Bound;

/// Helper function to execute range queries without version parameter
pub fn execute_range_query(
    stmt: &mut Statement,
    range: &EncodedKeyRange,
    param_count: u8,
) -> Vec<Unversioned> {
    match param_count {
        0 => {
            stmt.query_map(rusqlite::params![], |row| {
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
            stmt.query_map(rusqlite::params![param], |row| {
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
            stmt.query_map(rusqlite::params![start_param, end_param], |row| {
                Ok(Unversioned {
                    key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                    row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
                })
            }).unwrap().map(Result::unwrap).collect::<Vec<_>>()
        }
        _ => unreachable!(),
    }
}