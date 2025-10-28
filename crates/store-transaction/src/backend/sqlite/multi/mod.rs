// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

mod commit;
mod contains;
mod get;
mod range;
mod range_rev;
mod scan;
mod scan_rev;

use std::{
	collections::{HashMap, VecDeque},
	ops::Bound,
	sync::{Mutex, OnceLock},
};

pub use range::MultiVersionRangeIter;
pub use range_rev::MultiVersionRangeRevIter;
use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange,
	interface::{EncodableKeyRange, Key, MultiVersionValues, RowKey, RowKeyRange},
	value::encoded::EncodedValues,
};
use rusqlite::{Connection, Statement, params};
pub use scan::MultiVersionScanIter;
pub use scan_rev::MultiVersionScanRevIter;

use crate::backend::{result::MultiVersionIterResult, sqlite::read::ReadConnection};

/// Cache for source names to avoid repeated string allocations
static INTERNAL_NAME_CACHE: OnceLock<Mutex<HashMap<u64, String>>> = OnceLock::new();

pub(crate) fn as_row_key(key: &EncodedKey) -> Option<RowKey> {
	match Key::decode(key) {
		None => None,
		Some(key) => match key {
			Key::Row(key) => Some(key),
			_ => None,
		},
	}
}

/// Returns the appropriate source name for a given key, with caching
pub(crate) fn source_name(key: &EncodedKey) -> crate::Result<&'static str> {
	if let Some(key) = as_row_key(key) {
		let source_id = key.source.as_u64();
		let cache = INTERNAL_NAME_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
		let mut cache_guard = cache.lock().unwrap();

		let source_name = cache_guard.entry(source_id).or_insert_with(|| format!("source_{}", source_id));

		// SAFETY: We're returning a reference to a string that's stored
		// in the static cache The cache is never cleared, so the
		// reference remains valid for the lifetime of the program
		unsafe { Ok(std::mem::transmute(source_name.as_str())) }
	} else {
		Ok("multi")
	}
}

pub(crate) fn ensure_source_exists(conn: &Connection, source: &str) {
	let create_sql = format!(
		"CREATE TABLE IF NOT EXISTS {} (
            key     BLOB NOT NULL,
            version INTEGER NOT NULL,
            value   BLOB,
            PRIMARY KEY (key, version)
        )",
		source
	);
	conn.execute(&create_sql, []).unwrap();
}

pub(crate) fn source_name_for_range(range: &EncodedKeyRange) -> String {
	if let (Some(start), _) = RowKeyRange::decode(range) {
		let internal_id = start.source.as_u64();
		return format!("source_{}", internal_id);
	}
	"multi".to_string()
}

/// Fetch the previous version for a given key
pub(crate) fn fetch_pre_version(
	conn: &Connection,
	key: &[u8],
	source: &str,
) -> rusqlite::Result<Option<CommitVersion>> {
	let query = format!(
		"SELECT MAX(version) FROM {} WHERE key = ?",
		source
	);

	conn.query_row(&query, params![key], |row| {
		let version: Option<i64> = row.get(0)?;
		Ok(version.map(|v| CommitVersion(v as u64)))
	}).or_else(|e| match e {
		rusqlite::Error::QueryReturnedNoRows => Ok(None),
		_ => Err(e),
	})
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
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key ASC LIMIT ?",
				1,
			),
			"DESC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key DESC LIMIT ?",
				1,
			),
			_ => unreachable!(),
		},
		(Bound::Included(_), Bound::Unbounded) => match order {
			"ASC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key >= ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key ASC LIMIT ?",
				2,
			),
			"DESC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key >= ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key DESC LIMIT ?",
				2,
			),
			_ => unreachable!(),
		},
		(Bound::Excluded(_), Bound::Unbounded) => match order {
			"ASC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key > ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key ASC LIMIT ?",
				2,
			),
			"DESC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key > ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key DESC LIMIT ?",
				2,
			),
			_ => unreachable!(),
		},
		(Bound::Unbounded, Bound::Included(_)) => match order {
			"ASC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key <= ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key ASC LIMIT ?",
				2,
			),
			"DESC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key <= ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key DESC LIMIT ?",
				2,
			),
			_ => unreachable!(),
		},
		(Bound::Unbounded, Bound::Excluded(_)) => match order {
			"ASC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key < ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key ASC LIMIT ?",
				2,
			),
			"DESC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key < ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key DESC LIMIT ?",
				2,
			),
			_ => unreachable!(),
		},
		(Bound::Included(_), Bound::Included(_)) => match order {
			"ASC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key >= ? AND key <= ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key ASC LIMIT ?",
				3,
			),
			"DESC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key >= ? AND key <= ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key DESC LIMIT ?",
				3,
			),
			_ => unreachable!(),
		},
		(Bound::Included(_), Bound::Excluded(_)) => match order {
			"ASC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key >= ? AND key < ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key ASC LIMIT ?",
				3,
			),
			"DESC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key >= ? AND key < ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key DESC LIMIT ?",
				3,
			),
			_ => unreachable!(),
		},
		(Bound::Excluded(_), Bound::Included(_)) => match order {
			"ASC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key > ? AND key <= ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key ASC LIMIT ?",
				3,
			),
			"DESC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key > ? AND key <= ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key DESC LIMIT ?",
				3,
			),
			_ => unreachable!(),
		},
		(Bound::Excluded(_), Bound::Excluded(_)) => match order {
			"ASC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key > ? AND key < ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key ASC LIMIT ?",
				3,
			),
			"DESC" => (
				"SELECT t1.key, t1.value, t1.version FROM {} t1 INNER JOIN (SELECT key, MAX(version) as max_version FROM {} WHERE key > ? AND key < ? AND version <= ? GROUP BY key) t2 ON t1.key = t2.key AND t1.version = t2.max_version ORDER BY t1.key DESC LIMIT ?",
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
	version: CommitVersion,
	batch_size: usize,
	param_count: u8,
	buffer: &mut VecDeque<MultiVersionIterResult>,
) -> usize {
	let mut count = 0;
	match param_count {
		1 => {
			let rows = stmt
				.query_map(params![version.0, batch_size], |values| {
					let key = EncodedKey(CowVec::new(values.get::<_, Vec<u8>>(0)?));
					let value: Option<Vec<u8>> = values.get(1)?;
					let version = CommitVersion(values.get(2)?);
					match value {
						Some(val) => Ok(MultiVersionIterResult::Value(MultiVersionValues {
							key,
							values: EncodedValues(CowVec::new(val)),
							version,
						})),
						None => Ok(MultiVersionIterResult::Tombstone {
							key,
							version,
						}), // NULL value means deleted
					}
				})
				.unwrap();

			for result in rows {
				match result {
					Ok(v) => {
						buffer.push_back(v);
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
				.query_map(params![param, version.0, batch_size], |values| {
					let key = EncodedKey(CowVec::new(values.get::<_, Vec<u8>>(0)?));
					let value: Option<Vec<u8>> = values.get(1)?;
					let version = CommitVersion(values.get(2)?);
					match value {
						Some(val) => Ok(MultiVersionIterResult::Value(MultiVersionValues {
							key,
							values: EncodedValues(CowVec::new(val)),
							version,
						})),
						None => Ok(MultiVersionIterResult::Tombstone {
							key,
							version,
						}), // NULL value means deleted
					}
				})
				.unwrap();

			for result in rows {
				match result {
					Ok(v) => {
						buffer.push_back(v);
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
				.query_map(params![start_param, end_param, version.0, batch_size], |values| {
					let key = EncodedKey(CowVec::new(values.get::<_, Vec<u8>>(0)?));
					let value: Option<Vec<u8>> = values.get(1)?;
					let version = CommitVersion(values.get(2)?);
					match value {
						Some(val) => Ok(MultiVersionIterResult::Value(MultiVersionValues {
							key,
							values: EncodedValues(CowVec::new(val)),
							version,
						})),
						None => Ok(MultiVersionIterResult::Tombstone {
							key,
							version,
						}), // NULL value means deleted
					}
				})
				.unwrap();

			for result in rows {
				match result {
					Ok(v) => {
						buffer.push_back(v);
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

/// Helper function to get all source names for iteration
pub(crate) fn get_source_names(conn: &ReadConnection) -> Vec<String> {
	let conn_guard = conn.lock().unwrap();
	let mut stmt = conn_guard
		.prepare("SELECT name FROM sqlite_master WHERE type='table' AND (name='multi' OR name LIKE 'source_%')")
		.unwrap();

	stmt.query_map([], |values| Ok(values.get::<_, String>(0)?)).unwrap().map(Result::unwrap).collect()
}

/// Helper function to execute batched iteration queries across multiple sources
pub(crate) fn execute_scan_query(
	conn: &ReadConnection,
	source_names: &[String],
	version: CommitVersion,
	batch_size: usize,
	last_key: Option<&EncodedKey>,
	order: &str, // "ASC" or "DESC"
	buffer: &mut VecDeque<MultiVersionIterResult>,
) -> usize {
	let mut all_rows = Vec::new();

	// Query each source
	for source_name in source_names {
		let (query, params): (String, Vec<Box<dyn rusqlite::ToSql>>) = match (last_key, order) {
			(None, "ASC") => (
				format!(
					"SELECT t1.key, t1.value, t1.version FROM {} t1 
					 INNER JOIN (
					   SELECT key, MAX(version) as max_version 
					   FROM {} 
					   WHERE version <= ? 
					   GROUP BY key
					 ) t2 ON t1.key = t2.key AND t1.version = t2.max_version
					 ORDER BY t1.key ASC LIMIT ?",
					source_name, source_name
				),
				vec![Box::new(version.0), Box::new(batch_size)],
			),
			(None, "DESC") => (
				format!(
					"SELECT t1.key, t1.value, t1.version FROM {} t1 
					 INNER JOIN (
					   SELECT key, MAX(version) as max_version 
					   FROM {} 
					   WHERE version <= ? 
					   GROUP BY key
					 ) t2 ON t1.key = t2.key AND t1.version = t2.max_version
					 ORDER BY t1.key DESC LIMIT ?",
					source_name, source_name
				),
				vec![Box::new(version.0), Box::new(batch_size)],
			),
			(Some(key), "ASC") => (
				format!(
					"SELECT t1.key, t1.value, t1.version FROM {} t1 
					 INNER JOIN (
					   SELECT key, MAX(version) as max_version 
					   FROM {} 
					   WHERE key > ? AND version <= ? 
					   GROUP BY key
					 ) t2 ON t1.key = t2.key AND t1.version = t2.max_version
					 ORDER BY t1.key ASC LIMIT ?",
					source_name, source_name
				),
				vec![Box::new(key.to_vec()), Box::new(version.0), Box::new(batch_size)],
			),
			(Some(key), "DESC") => (
				format!(
					"SELECT t1.key, t1.value, t1.version FROM {} t1 
					 INNER JOIN (
					   SELECT key, MAX(version) as max_version 
					   FROM {} 
					   WHERE key < ? AND version <= ? 
					   GROUP BY key
					 ) t2 ON t1.key = t2.key AND t1.version = t2.max_version
					 ORDER BY t1.key DESC LIMIT ?",
					source_name, source_name
				),
				vec![Box::new(key.to_vec()), Box::new(version.0), Box::new(batch_size)],
			),
			_ => unreachable!(),
		};

		let conn_guard = conn.lock().unwrap();
		let mut stmt = conn_guard.prepare(&query).unwrap();

		let rows = stmt
			.query_map(rusqlite::params_from_iter(params.iter()), |values| {
				let key = EncodedKey(CowVec::new(values.get::<_, Vec<u8>>(0)?));
				let value: Option<Vec<u8>> = values.get(1)?;
				let version = CommitVersion(values.get(2)?);
				match value {
					Some(val) => Ok(MultiVersionIterResult::Value(MultiVersionValues {
						key,
						values: EncodedValues(CowVec::new(val)),
						version,
					})),
					None => Ok(MultiVersionIterResult::Tombstone {
						key,
						version,
					}), // NULL value means deleted
				}
			})
			.unwrap();

		for result in rows {
			match result {
				Ok(v) => all_rows.push(v),
				Err(_) => break,
			}
		}
	}

	// Sort the combined results
	match order {
		"ASC" => all_rows.sort_by(|a, b| {
			let key_a = match a {
				MultiVersionIterResult::Value(v) => &v.key,
				MultiVersionIterResult::Tombstone {
					key,
					..
				} => key,
			};
			let key_b = match b {
				MultiVersionIterResult::Value(v) => &v.key,
				MultiVersionIterResult::Tombstone {
					key,
					..
				} => key,
			};
			key_a.cmp(key_b)
		}),
		"DESC" => all_rows.sort_by(|a, b| {
			let key_a = match a {
				MultiVersionIterResult::Value(v) => &v.key,
				MultiVersionIterResult::Tombstone {
					key,
					..
				} => key,
			};
			let key_b = match b {
				MultiVersionIterResult::Value(v) => &v.key,
				MultiVersionIterResult::Tombstone {
					key,
					..
				} => key,
			};
			key_b.cmp(key_a)
		}),
		_ => unreachable!(),
	}

	// Take only the requested batch size from the sorted results
	let count = all_rows.len().min(batch_size);
	for multi in all_rows.into_iter().take(batch_size) {
		buffer.push_back(multi);
	}

	count
}
