// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

mod apply;
mod contains;
mod get;
mod iter;
mod iter_rev;
mod range;
mod range_rev;

use reifydb_core::interface::{Key, TableId, TableRowKey};
use reifydb_core::{EncodedKey, EncodedKeyRange};
use rusqlite::Connection;
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::{Mutex, OnceLock};

/// Cache for table names to avoid repeated string allocations
static TABLE_NAME_CACHE: OnceLock<Mutex<HashMap<TableId, String>>> = OnceLock::new();

/// Checks if an EncodedKey represents a TableRowKey
pub fn as_table_row_key(key: &EncodedKey) -> Option<TableRowKey> {
    match Key::decode(key) {
        None => None,
        Some(key) => match key {
            Key::TableRow(key) => Some(key),
            _ => None,
        },
    }
}

/// Returns the appropriate table name for a given key, with caching
pub fn table_name(key: &EncodedKey) -> &'static str {
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
pub fn ensure_table_exists(conn: &Connection, table: &str) {
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
pub fn table_name_for_range(range: &EncodedKeyRange) -> &'static str {
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
