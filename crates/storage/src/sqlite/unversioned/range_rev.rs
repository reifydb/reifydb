// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::sqlite::Sqlite;
use super::execute_range_query;
use reifydb_core::interface::{Unversioned, UnversionedScanRangeRev};
use reifydb_core::{EncodedKeyRange, Error};
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
        
        let rows = execute_range_query(&mut stmt, &range, param_count);
        Ok(Box::new(rows.into_iter()))
    }
}
