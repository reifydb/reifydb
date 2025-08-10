// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use super::{ensure_table_exists, table_name};
use crate::cdc::generate_cdc_event;
use crate::sqlite::Sqlite;
use crate::sqlite::cdc::{fetch_before_value, store_cdc_event};
use reifydb_core::delta::Delta;
use reifydb_core::interface::VersionedApply;
use reifydb_core::result::error::diagnostic::sequence;
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, Result, Version, return_error};
use rusqlite::params;
use std::collections::HashSet;
use std::sync::{LazyLock, RwLock};

static ENSURED_TABLES: LazyLock<RwLock<HashSet<String>>> =
    LazyLock::new(|| RwLock::new(HashSet::new()));

impl VersionedApply for Sqlite {
    fn apply(&self, delta: CowVec<Delta>, version: Version) -> Result<()> {
        let mut conn = self.get_conn();
        let tx = conn.transaction().unwrap();

        let timestamp = self.clock.now_millis();

        for delta in delta {
            let sequence = match self.cdc_seq.next_sequence(version) {
                Some(seq) => seq,
                None => return_error!(sequence::transaction_sequence_exhausted()),
            };
            
            // Get before value for updates and deletes
            let before_value = match &delta {
                Delta::Insert { .. } => None,
                Delta::Update { key, .. } | Delta::Remove { key } => {
                    let table = table_name(&key);
                    fetch_before_value(&tx, &key, table).ok().flatten()
                }
            };

            // Apply the data change
            match &delta {
                Delta::Insert { key, row } | Delta::Update { key, row } => {
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
                    let query = format!(
                        "INSERT OR REPLACE INTO {} (key, version, value) VALUES (?1, ?2, ?3)",
                        table
                    );
                    tx.execute(
                        &query,
                        params![key.to_vec(), version, EncodedRow::deleted().to_vec()],
                    )
                    .unwrap();
                }
            }

            // Generate and store CDC event
            let cdc_event = generate_cdc_event(&delta, version, sequence, timestamp, before_value);
            store_cdc_event(&tx, cdc_event, version, sequence).unwrap();
        }

        tx.commit().unwrap();
        Ok(())
    }
}
