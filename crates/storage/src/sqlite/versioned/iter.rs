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
                        key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                        row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
                        version: row.get(2)?,
                    })
                })
                .unwrap()
                .map(Result::unwrap)
                .collect();
                
            all_rows.extend(rows);
        }
        
        // Sort all rows by key
        all_rows.sort_by(|a, b| a.key.cmp(&b.key));
        
        Box::new(all_rows.into_iter())
    }
}