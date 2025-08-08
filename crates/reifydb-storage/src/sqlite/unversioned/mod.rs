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
use reifydb_core::interface::Unversioned;
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey};
use rusqlite::Statement;
use std::collections::VecDeque;
use std::ops::Bound;

/// Helper function to build unversioned query template and determine parameter count
pub(crate) fn build_unversioned_query(
    start_bound: Bound<&EncodedKey>,
    end_bound: Bound<&EncodedKey>,
    order: &str, // "ASC" or "DESC"
) -> (&'static str, u8) {
    match (start_bound, end_bound) {
        (Bound::Unbounded, Bound::Unbounded) => match order {
            "ASC" => ("SELECT key, value FROM unversioned ORDER BY key ASC LIMIT ?", 0),
            "DESC" => ("SELECT key, value FROM unversioned ORDER BY key DESC LIMIT ?", 0),
            _ => unreachable!(),
        },
        (Bound::Included(_), Bound::Unbounded) => match order {
            "ASC" => {
                ("SELECT key, value FROM unversioned WHERE key >= ? ORDER BY key ASC LIMIT ?", 1)
            }
            "DESC" => {
                ("SELECT key, value FROM unversioned WHERE key >= ? ORDER BY key DESC LIMIT ?", 1)
            }
            _ => unreachable!(),
        },
        (Bound::Excluded(_), Bound::Unbounded) => match order {
            "ASC" => {
                ("SELECT key, value FROM unversioned WHERE key > ? ORDER BY key ASC LIMIT ?", 1)
            }
            "DESC" => {
                ("SELECT key, value FROM unversioned WHERE key > ? ORDER BY key DESC LIMIT ?", 1)
            }
            _ => unreachable!(),
        },
        (Bound::Unbounded, Bound::Included(_)) => match order {
            "ASC" => {
                ("SELECT key, value FROM unversioned WHERE key <= ? ORDER BY key ASC LIMIT ?", 1)
            }
            "DESC" => {
                ("SELECT key, value FROM unversioned WHERE key <= ? ORDER BY key DESC LIMIT ?", 1)
            }
            _ => unreachable!(),
        },
        (Bound::Unbounded, Bound::Excluded(_)) => match order {
            "ASC" => {
                ("SELECT key, value FROM unversioned WHERE key < ? ORDER BY key ASC LIMIT ?", 1)
            }
            "DESC" => {
                ("SELECT key, value FROM unversioned WHERE key < ? ORDER BY key DESC LIMIT ?", 1)
            }
            _ => unreachable!(),
        },
        (Bound::Included(_), Bound::Included(_)) => match order {
            "ASC" => (
                "SELECT key, value FROM unversioned WHERE key >= ? AND key <= ? ORDER BY key ASC LIMIT ?",
                2,
            ),
            "DESC" => (
                "SELECT key, value FROM unversioned WHERE key >= ? AND key <= ? ORDER BY key DESC LIMIT ?",
                2,
            ),
            _ => unreachable!(),
        },
        (Bound::Included(_), Bound::Excluded(_)) => match order {
            "ASC" => (
                "SELECT key, value FROM unversioned WHERE key >= ? AND key < ? ORDER BY key ASC LIMIT ?",
                2,
            ),
            "DESC" => (
                "SELECT key, value FROM unversioned WHERE key >= ? AND key < ? ORDER BY key DESC LIMIT ?",
                2,
            ),
            _ => unreachable!(),
        },
        (Bound::Excluded(_), Bound::Included(_)) => match order {
            "ASC" => (
                "SELECT key, value FROM unversioned WHERE key > ? AND key <= ? ORDER BY key ASC LIMIT ?",
                2,
            ),
            "DESC" => (
                "SELECT key, value FROM unversioned WHERE key > ? AND key <= ? ORDER BY key DESC LIMIT ?",
                2,
            ),
            _ => unreachable!(),
        },
        (Bound::Excluded(_), Bound::Excluded(_)) => match order {
            "ASC" => (
                "SELECT key, value FROM unversioned WHERE key > ? AND key < ? ORDER BY key ASC LIMIT ?",
                2,
            ),
            "DESC" => (
                "SELECT key, value FROM unversioned WHERE key > ? AND key < ? ORDER BY key DESC LIMIT ?",
                2,
            ),
            _ => unreachable!(),
        },
    }
}

/// Helper function to execute batched unversioned range queries
pub(crate) fn execute_range_query(
    stmt: &mut Statement,
    start_bound: Bound<&EncodedKey>,
    end_bound: Bound<&EncodedKey>,
    batch_size: usize,
    param_count: u8,
    buffer: &mut VecDeque<Unversioned>,
) -> usize {
    let mut count = 0;
    match param_count {
        0 => {
            let rows = stmt
                .query_map(rusqlite::params![batch_size], |row| {
                    Ok(Unversioned {
                        key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                        row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
                    })
                })
                .unwrap();

            for result in rows {
                match result {
                    Ok(unversioned) => {
                        buffer.push_back(unversioned);
                        count += 1;
                    }
                    Err(_) => break,
                }
            }
        }
        1 => {
            let param = match (start_bound, end_bound) {
                (Bound::Included(key), _) | (Bound::Excluded(key), _) => key.to_vec(),
                (_, Bound::Included(key)) | (_, Bound::Excluded(key)) => key.to_vec(),
                _ => unreachable!(),
            };
            let rows = stmt
                .query_map(rusqlite::params![param, batch_size], |row| {
                    Ok(Unversioned {
                        key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                        row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
                    })
                })
                .unwrap();

            for result in rows {
                match result {
                    Ok(unversioned) => {
                        buffer.push_back(unversioned);
                        count += 1;
                    }
                    Err(_) => break,
                }
            }
        }
        2 => {
            let start_param = match start_bound {
                Bound::Included(key) | Bound::Excluded(key) => key.to_vec(),
                _ => unreachable!(),
            };
            let end_param = match end_bound {
                Bound::Included(key) | Bound::Excluded(key) => key.to_vec(),
                _ => unreachable!(),
            };
            let rows = stmt
                .query_map(rusqlite::params![start_param, end_param, batch_size], |row| {
                    Ok(Unversioned {
                        key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                        row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
                    })
                })
                .unwrap();

            for result in rows {
                match result {
                    Ok(unversioned) => {
                        buffer.push_back(unversioned);
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

/// Helper function to execute batched unversioned iteration queries
pub(crate) fn execute_iter_query(
    conn: &PooledConnection<SqliteConnectionManager>,
    batch_size: usize,
    last_key: Option<&EncodedKey>,
    order: &str, // "ASC" or "DESC"
    buffer: &mut VecDeque<Unversioned>,
) -> usize {
    let (query, params): (String, Vec<Box<dyn rusqlite::ToSql>>) = match (last_key, order) {
        (None, "ASC") => (
            "SELECT key, value FROM unversioned ORDER BY key ASC LIMIT ?".to_string(),
            vec![Box::new(batch_size)],
        ),
        (None, "DESC") => (
            "SELECT key, value FROM unversioned ORDER BY key DESC LIMIT ?".to_string(),
            vec![Box::new(batch_size)],
        ),
        (Some(key), "ASC") => (
            "SELECT key, value FROM unversioned WHERE key > ? ORDER BY key ASC LIMIT ?".to_string(),
            vec![Box::new(key.to_vec()), Box::new(batch_size)],
        ),
        (Some(key), "DESC") => (
            "SELECT key, value FROM unversioned WHERE key < ? ORDER BY key DESC LIMIT ?"
                .to_string(),
            vec![Box::new(key.to_vec()), Box::new(batch_size)],
        ),
        _ => unreachable!(),
    };

    let mut stmt = conn.prepare(&query).unwrap();

    let rows = stmt
        .query_map(rusqlite::params_from_iter(params.iter()), |row| {
            Ok(Unversioned {
                key: EncodedKey::new(row.get::<_, Vec<u8>>(0)?),
                row: EncodedRow(CowVec::new(row.get::<_, Vec<u8>>(1)?)),
            })
        })
        .unwrap();

    let mut count = 0;
    for result in rows {
        match result {
            Ok(unversioned) => {
                buffer.push_back(unversioned);
                count += 1;
            }
            Err(_) => break,
        }
    }

    count
}
