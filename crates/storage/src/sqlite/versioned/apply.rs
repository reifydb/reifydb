// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::sqlite::Sqlite;
use reifydb_core::delta::Delta;
use reifydb_core::interface::VersionedApply;
use reifydb_core::{CowVec, Version};
use rusqlite::params;

impl VersionedApply for Sqlite {
    fn apply(&self, delta: CowVec<Delta>, version: Version) {
        let mut conn = self.get_conn();
        let tx = conn.transaction().unwrap();

        for delta in delta {
            match delta {
                Delta::Set { key, row: bytes } => {
                    tx.execute(
                        "INSERT OR REPLACE INTO versioned (key, version, value) VALUES (?1, ?2, ?3)",
                        params![key.to_vec(), version, bytes.to_vec()],
                    )
                    .unwrap();
                }
                Delta::Remove { key } => {
                    tx.execute(
                        "DELETE FROM versioned WHERE key = ?1 AND version = ?2",
                        params![key.to_vec(), version],
                    )
                    .unwrap();
                }
            }
        }

        tx.commit().unwrap();
    }
}