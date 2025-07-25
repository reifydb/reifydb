// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::sqlite::Sqlite;
use super::{execute_range_query, table_name_for_range};
use reifydb_core::interface::{Versioned, VersionedScanRangeRev};
use reifydb_core::{EncodedKeyRange, Version};
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
        
        let rows = execute_range_query(&mut stmt, &range, version, param_count);
        Box::new(rows.into_iter())
    }
}