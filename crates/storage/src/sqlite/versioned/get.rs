// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::sqlite::Sqlite;
use super::table_name;
use reifydb_core::interface::{Versioned, VersionedGet};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, Version};
use rusqlite::{OptionalExtension, params};

impl VersionedGet for Sqlite {
    fn get(&self, key: &EncodedKey, version: Version) -> Option<Versioned> {
        let conn = self.get_conn();
        
        let table = table_name(key);
        let query = format!(
            "SELECT key, value, version FROM {} WHERE key = ?1 AND version <= ?2 ORDER BY version DESC LIMIT 1",
            table
        );
        
        conn.query_row(
            &query,
            params![key.to_vec(), version],
            |row| {
                Ok(Versioned {
                    key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                    row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
                    version: row.get(2)?,
                })
            },
        )
        .optional()
        .unwrap()
    }
}