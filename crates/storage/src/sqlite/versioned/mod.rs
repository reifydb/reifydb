// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

mod apply;
mod contains;
mod get;
mod iter;
mod iter_rev;
mod range;
mod range_rev;

use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use reifydb_core::interface::{
    EncodableKeyRange, Key, TableId, TableRowKey, TableRowKeyRange, Versioned,
};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange, Version};
use rusqlite::{Connection, Statement};
use std::collections::{HashMap, VecDeque};
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
pub(crate) fn table_name_for_range(range: &EncodedKeyRange) -> String {
    if let (Some(start), _) = TableRowKeyRange::decode(range) {
        return format!("table_{}", start.table);
    }
    "versioned".to_string()
}

/// Helper function to build query template and determine parameter count
pub(crate) fn build_range_query(
    start_bound: Bound<&EncodedKey>,
    end_bound: Bound<&EncodedKey>,
    order: &str, // "ASC" or "DESC"
) -> (&'static str, u8) {
    match (start_bound, end_bound) {
        (Bound::Unbounded, Bound::Unbounded) => match order {
            "ASC" => (
                "SELECT key, value, version FROM {} WHERE version <= ? ORDER BY key ASC LIMIT ?",
                1,
            ),
            "DESC" => (
                "SELECT key, value, version FROM {} WHERE version <= ? ORDER BY key DESC LIMIT ?",
                1,
            ),
            _ => unreachable!(),
        },
        (Bound::Included(_), Bound::Unbounded) => match order {
            "ASC" => (
                "SELECT key, value, version FROM {} WHERE key >= ? AND version <= ? ORDER BY key ASC LIMIT ?",
                2,
            ),
            "DESC" => (
                "SELECT key, value, version FROM {} WHERE key >= ? AND version <= ? ORDER BY key DESC LIMIT ?",
                2,
            ),
            _ => unreachable!(),
        },
        (Bound::Excluded(_), Bound::Unbounded) => match order {
            "ASC" => (
                "SELECT key, value, version FROM {} WHERE key > ? AND version <= ? ORDER BY key ASC LIMIT ?",
                2,
            ),
            "DESC" => (
                "SELECT key, value, version FROM {} WHERE key > ? AND version <= ? ORDER BY key DESC LIMIT ?",
                2,
            ),
            _ => unreachable!(),
        },
        (Bound::Unbounded, Bound::Included(_)) => match order {
            "ASC" => (
                "SELECT key, value, version FROM {} WHERE key <= ? AND version <= ? ORDER BY key ASC LIMIT ?",
                2,
            ),
            "DESC" => (
                "SELECT key, value, version FROM {} WHERE key <= ? AND version <= ? ORDER BY key DESC LIMIT ?",
                2,
            ),
            _ => unreachable!(),
        },
        (Bound::Unbounded, Bound::Excluded(_)) => match order {
            "ASC" => (
                "SELECT key, value, version FROM {} WHERE key < ? AND version <= ? ORDER BY key ASC LIMIT ?",
                2,
            ),
            "DESC" => (
                "SELECT key, value, version FROM {} WHERE key < ? AND version <= ? ORDER BY key DESC LIMIT ?",
                2,
            ),
            _ => unreachable!(),
        },
        (Bound::Included(_), Bound::Included(_)) => match order {
            "ASC" => (
                "SELECT key, value, version FROM {} WHERE key >= ? AND key <= ? AND version <= ? ORDER BY key ASC LIMIT ?",
                3,
            ),
            "DESC" => (
                "SELECT key, value, version FROM {} WHERE key >= ? AND key <= ? AND version <= ? ORDER BY key DESC LIMIT ?",
                3,
            ),
            _ => unreachable!(),
        },
        (Bound::Included(_), Bound::Excluded(_)) => match order {
            "ASC" => (
                "SELECT key, value, version FROM {} WHERE key >= ? AND key < ? AND version <= ? ORDER BY key ASC LIMIT ?",
                3,
            ),
            "DESC" => (
                "SELECT key, value, version FROM {} WHERE key >= ? AND key < ? AND version <= ? ORDER BY key DESC LIMIT ?",
                3,
            ),
            _ => unreachable!(),
        },
        (Bound::Excluded(_), Bound::Included(_)) => match order {
            "ASC" => (
                "SELECT key, value, version FROM {} WHERE key > ? AND key <= ? AND version <= ? ORDER BY key ASC LIMIT ?",
                3,
            ),
            "DESC" => (
                "SELECT key, value, version FROM {} WHERE key > ? AND key <= ? AND version <= ? ORDER BY key DESC LIMIT ?",
                3,
            ),
            _ => unreachable!(),
        },
        (Bound::Excluded(_), Bound::Excluded(_)) => match order {
            "ASC" => (
                "SELECT key, value, version FROM {} WHERE key > ? AND key < ? AND version <= ? ORDER BY key ASC LIMIT ?",
                3,
            ),
            "DESC" => (
                "SELECT key, value, version FROM {} WHERE key > ? AND key < ? AND version <= ? ORDER BY key DESC LIMIT ?",
                3,
            ),
            _ => unreachable!(),
        },
    }
}

/// Helper function to execute batched range queries
pub(crate) fn execute_batched_range_query(
    stmt: &mut Statement,
    start_bound: Bound<&EncodedKey>,
    end_bound: Bound<&EncodedKey>,
    version: Version,
    batch_size: usize,
    param_count: u8,
    buffer: &mut VecDeque<Versioned>,
) -> usize {
    let mut count = 0;
    match param_count {
        1 => {
            let rows = stmt
                .query_map(rusqlite::params![version, batch_size], |row| {
                    Ok(Versioned {
                        key: EncodedKey(CowVec::new(row.get(0)?)),
                        row: EncodedRow(CowVec::new(row.get(1)?)),
                        version: row.get(2)?,
                    })
                })
                .unwrap();

            for result in rows {
                match result {
                    Ok(versioned) => {
                        buffer.push_back(versioned);
                        count += 1;
                    }
                    Err(_) => break,
                }
            }
        }
        2 => {
            let param = match (start_bound, end_bound) {
                (Bound::Included(key), _) | (Bound::Excluded(key), _) => key.to_vec(),
                (_, Bound::Included(key)) | (_, Bound::Excluded(key)) => key.to_vec(),
                _ => unreachable!(),
            };
            let rows = stmt
                .query_map(rusqlite::params![param, version, batch_size], |row| {
                    Ok(Versioned {
                        key: EncodedKey(CowVec::new(row.get(0)?)),
                        row: EncodedRow(CowVec::new(row.get(1)?)),
                        version: row.get(2)?,
                    })
                })
                .unwrap();

            for result in rows {
                match result {
                    Ok(versioned) => {
                        buffer.push_back(versioned);
                        count += 1;
                    }
                    Err(_) => break,
                }
            }
        }
        3 => {
            let start_param = match start_bound {
                Bound::Included(key) | Bound::Excluded(key) => key.to_vec(),
                _ => unreachable!(),
            };
            let end_param = match end_bound {
                Bound::Included(key) | Bound::Excluded(key) => key.to_vec(),
                _ => unreachable!(),
            };
            let rows = stmt
                .query_map(rusqlite::params![start_param, end_param, version, batch_size], |row| {
                    Ok(Versioned {
                        key: EncodedKey(CowVec::new(row.get(0)?)),
                        row: EncodedRow(CowVec::new(row.get(1)?)),
                        version: row.get(2)?,
                    })
                })
                .unwrap();

            for result in rows {
                match result {
                    Ok(versioned) => {
                        buffer.push_back(versioned);
                        count += 1;
                    }
                    Err(_) => break,
                }
            }
        }
        _ => unreachable!(),
    }
    count
}

/// Helper function to get all table names for iteration
pub(crate) fn get_table_names(conn: &PooledConnection<SqliteConnectionManager>) -> Vec<String> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND (name='versioned' OR name LIKE 'table_%')")
        .unwrap();

    stmt.query_map([], |row| Ok(row.get::<_, String>(0)?)).unwrap().map(Result::unwrap).collect()
}

/// Helper function to execute batched iteration queries across multiple tables
pub(crate) fn execute_iter_query(
    conn: &PooledConnection<SqliteConnectionManager>,
    table_names: &[String],
    version: Version,
    batch_size: usize,
    last_key: Option<&EncodedKey>,
    order: &str, // "ASC" or "DESC"
    buffer: &mut VecDeque<Versioned>,
) -> usize {
    let mut all_rows = Vec::new();

    // Query each table
    for table_name in table_names {
        let (query, params): (String, Vec<Box<dyn rusqlite::ToSql>>) = match (last_key, order) {
            (None, "ASC") => (
                format!(
                    "SELECT key, value, version FROM {} WHERE version <= ? ORDER BY key ASC LIMIT ?",
                    table_name
                ),
                vec![Box::new(version), Box::new(batch_size)],
            ),
            (None, "DESC") => (
                format!(
                    "SELECT key, value, version FROM {} WHERE version <= ? ORDER BY key DESC LIMIT ?",
                    table_name
                ),
                vec![Box::new(version), Box::new(batch_size)],
            ),
            (Some(key), "ASC") => (
                format!(
                    "SELECT key, value, version FROM {} WHERE key > ? AND version <= ? ORDER BY key ASC LIMIT ?",
                    table_name
                ),
                vec![Box::new(key.to_vec()), Box::new(version), Box::new(batch_size)],
            ),
            (Some(key), "DESC") => (
                format!(
                    "SELECT key, value, version FROM {} WHERE key < ? AND version <= ? ORDER BY key DESC LIMIT ?",
                    table_name
                ),
                vec![Box::new(key.to_vec()), Box::new(version), Box::new(batch_size)],
            ),
            _ => unreachable!(),
        };

        let mut stmt = conn.prepare(&query).unwrap();

        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                Ok(Versioned {
                    key: EncodedKey(CowVec::new(row.get(0)?)),
                    row: EncodedRow(CowVec::new(row.get(1)?)),
                    version: row.get(2)?,
                })
            })
            .unwrap();

        for result in rows {
            match result {
                Ok(versioned) => all_rows.push(versioned),
                Err(_) => break,
            }
        }
    }

    // Sort the combined results
    match order {
        "ASC" => all_rows.sort_by(|a, b| a.key.cmp(&b.key)),
        "DESC" => all_rows.sort_by(|a, b| b.key.cmp(&a.key)),
        _ => unreachable!(),
    }

    // Take only the requested batch size from the sorted results
    let count = all_rows.len().min(batch_size);
    for versioned in all_rows.into_iter().take(batch_size) {
        buffer.push_back(versioned);
    }

    count
}
