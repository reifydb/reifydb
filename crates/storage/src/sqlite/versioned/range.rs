// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::sqlite::{Sqlite, bound_to_bytes};
use reifydb_core::interface::{Versioned, VersionedScanRange};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange, Version};
use rusqlite::params;

impl VersionedScanRange for Sqlite {
    type ScanRangeIter<'a> = Box<dyn Iterator<Item = Versioned> + Send + 'a>;

    fn scan_range(&self, range: EncodedKeyRange, version: Version) -> Self::ScanRangeIter<'_> {
        let conn = self.get_conn();
        let mut stmt = conn
			.prepare("SELECT key, value, version FROM versioned WHERE key >= ?1 AND key <= ?2 AND version <= ?3 ORDER BY key ASC")
			.unwrap();

        let start_bytes = bound_to_bytes(&range.start);
        let end_bytes = bound_to_bytes(&range.end);

        let rows = stmt
            .query_map(params![start_bytes, end_bytes, version], |row| {
                Ok(Versioned {
                    key: EncodedKey(CowVec::new(row.get(0)?)),
                    row: EncodedRow(CowVec::new(row.get(1)?)),
                    version: row.get(2)?,
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();

        Box::new(rows.into_iter())
    }
}
