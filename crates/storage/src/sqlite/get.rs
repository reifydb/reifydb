// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Unversioned;
use crate::sqlite::Sqlite;
use crate::unversioned::UnversionedGet;
use reifydb_core::row::EncodedRow;
use reifydb_core::{AsyncCowVec, EncodedKey};
use rusqlite::{OptionalExtension, params};

impl UnversionedGet for Sqlite {
    fn get_unversioned(&self, key: &EncodedKey) -> Option<Unversioned> {
        let conn = self.get_conn();
        conn.query_row(
            "SELECT key, value FROM unversioned WHERE key = ?1  LIMIT 1",
            params![key.to_vec()],
            |row| {
                Ok(Unversioned {
                    key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                    row: EncodedRow(AsyncCowVec::new(row.get::<_, Vec<u8>>(1)?)),
                })
            },
        )
        .optional()
        .unwrap()
    }
}
