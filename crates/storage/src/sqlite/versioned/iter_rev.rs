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
        
        // Get all table names that exist
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND (name='versioned' OR name LIKE 'table_%')")
            .unwrap();
        
        let table_names: Vec<String> = stmt
            .query_map([], |row| Ok(row.get::<_, String>(0)?))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        
        let mut all_rows = Vec::new();
        
        // Query each table
        for table_name in table_names {
            let query = format!(
                "SELECT key, value, version FROM {} WHERE version <= ? ORDER BY key ASC",
                table_name
            );
            let mut stmt = conn.prepare(&query).unwrap();
            
            let rows: Vec<Versioned> = stmt
                .query_map(params![version], |row| {
                    Ok(Versioned {
                        key: EncodedKey(CowVec::new(row.get(0)?)),
                        row: EncodedRow(CowVec::new(row.get(1)?)),
                        version: row.get(2)?,
                    })
                })
                .unwrap()
                .map(Result::unwrap)
                .collect();
                
            all_rows.extend(rows);
        }
        
        // Sort all rows by key in descending order for reverse iteration
        all_rows.sort_by(|a, b| b.key.cmp(&a.key));
        
        Box::new(all_rows.into_iter())
    }
}