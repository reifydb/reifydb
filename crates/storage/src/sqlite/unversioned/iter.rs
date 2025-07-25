// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::sqlite::Sqlite;
use reifydb_core::Error;
use reifydb_core::interface::{Unversioned, UnversionedScan};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey};
use rusqlite::params;

impl UnversionedScan for Sqlite {
    type ScanIter<'a> = Box<dyn Iterator<Item = Unversioned> + Send + 'a>;

    fn scan(&self) -> Result<Self::ScanIter<'_>, Error> {
        let conn = self.get_conn();
        let mut stmt = conn
            .prepare("SELECT key, value FROM unversioned ORDER BY key ASC")
            .unwrap();

        let rows = stmt
            .query_map(params![], |row| {
                Ok(Unversioned {
                    key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                    row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();

        Ok(Box::new(rows.into_iter()))
    }
}
