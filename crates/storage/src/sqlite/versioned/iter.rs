// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::sqlite::Sqlite;
use reifydb_core::interface::{Versioned, VersionedScan};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, Version};
use rusqlite::params;

impl VersionedScan for Sqlite {
    type ScanIter<'a> = Box<dyn Iterator<Item = Versioned> + Send + 'a>;

    fn scan(&self, version: Version) -> Self::ScanIter<'_> {

        let conn = self.get_conn();
        let mut stmt = conn
            .prepare(
                "SELECT key, value, version FROM versioned WHERE version <= ? ORDER BY key ASC",
            )
            .unwrap();

        let rows = stmt
            .query_map(params![version], |row| {
                Ok(Versioned {
                    key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                    row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
                    version: row.get(2)?,
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();

        Box::new(rows.into_iter())
    }
}