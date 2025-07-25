// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::sqlite::Sqlite;
use reifydb_core::interface::{Versioned, VersionedScanRev};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, Version};
use rusqlite::params;

impl VersionedScanRev for Sqlite {
    type ScanIterRev<'a> = Box<dyn Iterator<Item = Versioned> + Send + 'a>;

    fn scan_rev(&self, version: Version) -> Self::ScanIterRev<'_> {

        let conn = self.get_conn();
        let mut stmt = conn
            .prepare(
                "SELECT key, value, version FROM versioned WHERE version <= ? ORDER BY key DESC",
            )
            .unwrap();

        let rows = stmt
            .query_map(params![version], |row| {
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