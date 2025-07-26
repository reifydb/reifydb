// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use super::{ensure_table_exists, table_name};
use crate::sqlite::Sqlite;
use reifydb_core::delta::Delta;
use reifydb_core::interface::VersionedApply;
use reifydb_core::{CowVec, Version, Result};
use rusqlite::params;
use std::collections::HashSet;
use std::sync::{LazyLock, RwLock};

static ENSURED_TABLES: LazyLock<RwLock<HashSet<String>>> =
    LazyLock::new(|| RwLock::new(HashSet::new()));

impl VersionedApply for Sqlite {
    fn apply(&self, delta: CowVec<Delta>, version: Version) -> Result<()> {
        let mut conn = self.get_conn();
        let tx = conn.transaction().unwrap();

        for delta in delta {
            match delta {
                Delta::Insert { key, row }
                | Delta::Update { key, row }
                | Delta::Upsert { key, row } => {
                    let table = table_name(&key);

                    if table != "versioned" {
                        let ensured_tables = ENSURED_TABLES.read().unwrap();
                        if !ensured_tables.contains(table) {
                            drop(ensured_tables);
                            let mut ensured_tables = ENSURED_TABLES.write().unwrap();
                            if !ensured_tables.contains(table) {
                                ensure_table_exists(&tx, &table);
                                ensured_tables.insert(table.to_string());
                            }
                        }
                    }

                    let query = format!(
                        "INSERT OR REPLACE INTO {} (key, version, value) VALUES (?1, ?2, ?3)",
                        table
                    );
                    tx.execute(&query, params![key.to_vec(), version, row.to_vec()]).unwrap();
                }
                Delta::Remove { key } => {
                    let table = table_name(&key);
                    let query = format!("DELETE FROM {} WHERE key = ?1 AND version = ?2", table);
                    tx.execute(&query, params![key.to_vec(), version]).unwrap();
                }
            }
        }

        tx.commit().unwrap();
        Ok(())
    }
}
