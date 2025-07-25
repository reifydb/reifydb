// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

mod apply;
mod contains;
mod get;
mod iter;
mod iter_rev;
mod range;
mod range_rev;

use reifydb_core::interface::{Key, TableId, TableRowKey, Versioned};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange, Version};
use rusqlite::{Connection, Statement};
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::{Mutex, OnceLock};

/// Cache for table names to avoid repeated string allocations
static TABLE_NAME_CACHE: OnceLock<Mutex<HashMap<TableId, String>>> = OnceLock::new();

/// Checks if an EncodedKey represents a TableRowKey
pub(crate) fn as_table_row_key(key: &EncodedKey) -> Option<TableRowKey> {
    match Key::decode(key) {
        None => None,
        Some(key) => match key {
            Key::TableRow(key) => Some(key),
            _ => None,
        },
    }
}

/// Returns the appropriate table name for a given key, with caching
pub(crate) fn table_name(key: &EncodedKey) -> &'static str {
    if let Some(key) = as_table_row_key(key) {
        let cache = TABLE_NAME_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
        let mut cache_guard = cache.lock().unwrap();

        let table_name =
            cache_guard.entry(key.table).or_insert_with(|| format!("table_{}", key.table.0));

        // SAFETY: We're returning a reference to a string that's stored in the static cache
        // The cache is never cleared, so the reference remains valid for the lifetime of the program
        unsafe { std::mem::transmute(table_name.as_str()) }
    } else {
        "versioned"
    }
}

/// Ensures a table exists for the given TableId
pub(crate) fn ensure_table_exists(conn: &Connection, table: &str) {
    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            key     BLOB NOT NULL,
            version INTEGER NOT NULL,
            value   BLOB NOT NULL,
            PRIMARY KEY (key, version)
        )",
        table
    );
    conn.execute(&create_sql, []).unwrap();
}

/// Returns the appropriate table name for a range operation based on range bounds
pub(crate) fn table_name_for_range(range: &EncodedKeyRange) -> &'static str {
    // Check if any bound is a TableRowKey and use that table
    let start_key = match &range.start {
        Bound::Included(key) | Bound::Excluded(key) => Some(key),
        Bound::Unbounded => None,
    };
    let end_key = match &range.end {
        Bound::Included(key) | Bound::Excluded(key) => Some(key),
        Bound::Unbounded => None,
    };

    // Use the first TableRowKey we find, or default to versioned
    if let Some(key) = start_key {
        if as_table_row_key(key).is_some() {
            return table_name(key);
        }
    }
    if let Some(key) = end_key {
        if as_table_row_key(key).is_some() {
            return table_name(key);
        }
    }

    "versioned"
}

/// Helper function to execute range queries with version parameter
pub(crate) fn execute_range_query(
    stmt: &mut Statement,
    range: &EncodedKeyRange,
    version: Version,
    param_count: u8,
) -> Vec<Versioned> {
    match param_count {
        1 => stmt
            .query_map(rusqlite::params![version], |row| {
                Ok(Versioned {
                    key: EncodedKey(CowVec::new(row.get(0)?)),
                    row: EncodedRow(CowVec::new(row.get(1)?)),
                    version: row.get(2)?,
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>(),
        2 => {
            let param = match (&range.start, &range.end) {
                (Bound::Included(key), _) | (Bound::Excluded(key), _) => key.to_vec(),
                (_, Bound::Included(key)) | (_, Bound::Excluded(key)) => key.to_vec(),
                _ => unreachable!(),
            };
            stmt.query_map(rusqlite::params![param, version], |row| {
                Ok(Versioned {
                    key: EncodedKey(CowVec::new(row.get(0)?)),
                    row: EncodedRow(CowVec::new(row.get(1)?)),
                    version: row.get(2)?,
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
        }
        3 => {
            let start_param = match &range.start {
                Bound::Included(key) | Bound::Excluded(key) => key.to_vec(),
                _ => unreachable!(),
            };
            let end_param = match &range.end {
                Bound::Included(key) | Bound::Excluded(key) => key.to_vec(),
                _ => unreachable!(),
            };
            stmt.query_map(rusqlite::params![start_param, end_param, version], |row| {
                Ok(Versioned {
                    key: EncodedKey(CowVec::new(row.get(0)?)),
                    row: EncodedRow(CowVec::new(row.get(1)?)),
                    version: row.get(2)?,
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
        }
        _ => unreachable!(),
    }
}
