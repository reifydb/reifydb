// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::execute_range_query;
use crate::sqlite::Sqlite;
use reifydb_core::interface::{Unversioned, UnversionedScanRange};
use reifydb_core::{EncodedKeyRange, Error};
use std::ops::Bound;

impl UnversionedScanRange for Sqlite {
    type ScanRange<'a>
        = Box<dyn Iterator<Item = Unversioned> + Send + 'a>
    where
        Self: 'a;

    fn scan_range(&self, range: EncodedKeyRange) -> Result<Self::ScanRange<'_>, Error> {
        let conn = self.get_conn();

        // Build query and parameters based on bounds
        let (query, param_count) = match (&range.start, &range.end) {
            (Bound::Unbounded, Bound::Unbounded) => {
                ("SELECT key, value FROM unversioned ORDER BY key ASC", 0)
            }
            (Bound::Included(_), Bound::Unbounded) => {
                ("SELECT key, value FROM unversioned WHERE key >= ? ORDER BY key ASC", 1)
            }
            (Bound::Excluded(_), Bound::Unbounded) => {
                ("SELECT key, value FROM unversioned WHERE key > ? ORDER BY key ASC", 1)
            }
            (Bound::Unbounded, Bound::Included(_)) => {
                ("SELECT key, value FROM unversioned WHERE key <= ? ORDER BY key ASC", 1)
            }
            (Bound::Unbounded, Bound::Excluded(_)) => {
                ("SELECT key, value FROM unversioned WHERE key < ? ORDER BY key ASC", 1)
            }
            (Bound::Included(_), Bound::Included(_)) => (
                "SELECT key, value FROM unversioned WHERE key >= ? AND key <= ? ORDER BY key ASC",
                2,
            ),
            (Bound::Included(_), Bound::Excluded(_)) => (
                "SELECT key, value FROM unversioned WHERE key >= ? AND key < ? ORDER BY key ASC",
                2,
            ),
            (Bound::Excluded(_), Bound::Included(_)) => (
                "SELECT key, value FROM unversioned WHERE key > ? AND key <= ? ORDER BY key ASC",
                2,
            ),
            (Bound::Excluded(_), Bound::Excluded(_)) => {
                ("SELECT key, value FROM unversioned WHERE key > ? AND key < ? ORDER BY key ASC", 2)
            }
        };

        let mut stmt = conn.prepare(&query).unwrap();

        let rows = execute_range_query(&mut stmt, &range, param_count);
        Ok(Box::new(rows.into_iter()))
    }
}
