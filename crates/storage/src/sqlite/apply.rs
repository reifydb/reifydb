// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::sqlite::Sqlite;
use reifydb_core::delta::Delta;
use reifydb_core::interface::UnversionedApply;
use reifydb_core::{AsyncCowVec, Error};
use rusqlite::params;

impl UnversionedApply for Sqlite {
    fn apply(&mut self, delta: AsyncCowVec<Delta>) -> Result<(), Error> {
        let mut conn = self.get_conn();
        let tx = conn.transaction().unwrap();

        for delta in delta {
            match delta {
                Delta::Set { key, row: bytes } => {
                    tx.execute(
                        "INSERT OR REPLACE INTO unversioned (key,value) VALUES (?1, ?2)",
                        params![key.to_vec(), bytes.to_vec()],
                    )
                    .unwrap();
                }
                Delta::Remove { key } => {
                    tx.execute("DELETE FROM unversioned WHERE key = ?1", params![key.to_vec()])
                        .unwrap();
                }
            }
        }
        tx.commit().unwrap();
        Ok(())
    }
}
