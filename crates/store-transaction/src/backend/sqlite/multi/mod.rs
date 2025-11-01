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
	interface::{
		EncodableKeyRange, FlowNodeStateKey, FlowNodeStateKeyRange, Key, MultiVersionValues, RowKey,
		RowKeyRange,
	},
	value::encoded::EncodedValues,
};
use rusqlite::{Connection, Statement, params_from_iter};
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

pub(crate) fn as_flow_node_state_key(key: &EncodedKey) -> Option<FlowNodeStateKey> {
	match Key::decode(key) {
		None => None,
		Some(key) => match key {
			Key::FlowNodeState(key) => Some(key),
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

pub(crate) fn ensure_source_exists(conn: &Connection, source: &str) -> rusqlite::Result<()> {
	// Create table with WITHOUT ROWID optimization
	let create_sql = format!(
		"CREATE TABLE IF NOT EXISTS {} (
            key          BLOB NOT NULL,
            version      INTEGER NOT NULL,
            value        BLOB,
            is_tombstone INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (key, version)
        ) WITHOUT ROWID",
		source
	);
	conn.execute(&create_sql, [])?;

	// Create visibility index - critical for MVCC queries
	// This partial index is used by:
	// - Range queries with is_tombstone = 0 filter
	// - fetch_pre_versions for batch lookups
	// - Scan queries that need latest non-tombstone version
	let vis_idx_sql = format!(
		"CREATE INDEX IF NOT EXISTS {}_vis_idx
         ON {}(key, version DESC)
         WHERE is_tombstone = 0",
		source, source
	);
	conn.execute(&vis_idx_sql, [])?;

	Ok(())
}

pub(crate) fn source_name_for_range(range: &EncodedKeyRange) -> String {
	if let (Some(start), _) = RowKeyRange::decode(range) {
		let internal_id = start.source.as_u64();
		return format!("source_{}", internal_id);
	}
	"multi".to_string()
}

/// Returns the appropriate operator table name for a given FlowNodeStateKey, with caching
pub(crate) fn operator_name(key: &EncodedKey) -> crate::Result<&'static str> {
	if let Some(key) = as_flow_node_state_key(key) {
		let node_id = key.node.0;
		let cache = INTERNAL_NAME_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
		let mut cache_guard = cache.lock().unwrap();

		let operator_name = cache_guard.entry(node_id).or_insert_with(|| format!("operator_{}", node_id));

		// SAFETY: We're returning a reference to a string that's stored
		// in the static cache The cache is never cleared, so the
		// reference remains valid for the lifetime of the program
		unsafe { Ok(std::mem::transmute(operator_name.as_str())) }
	} else {
		Ok("multi")
	}
}

pub(crate) fn operator_name_for_range(range: &EncodedKeyRange) -> String {
	if let (Some(start), _) = FlowNodeStateKeyRange::decode(range) {
		let node_id = start.node.0;
		return format!("operator_{}", node_id);
	}
	"multi".to_string()
}

/// Batch fetch previous versions for multiple keys from a specific table
/// Returns a HashMap of key -> (version, is_latest_tombstone)
/// - version: The latest non-tombstone version (for CDC pre-value lookup)
/// - is_latest_tombstone: True if the absolute latest version is a tombstone
///
/// This distinction is critical for CDC:
/// - If latest is tombstone → key was deleted → next SET is an INSERT
/// - If latest is not tombstone → key exists → next SET is an UPDATE
pub(crate) fn fetch_pre_versions(
	conn: &Connection,
	keys: &[&[u8]],
	source: &str,
) -> rusqlite::Result<HashMap<Vec<u8>, (CommitVersion, bool)>> {
	if keys.is_empty() {
		return Ok(HashMap::new());
	}

	let mut results = HashMap::new();
	const MAX_BATCH_SIZE: usize = 999;

	for chunk in keys.chunks(MAX_BATCH_SIZE) {
		// Build placeholders for IN clause
		let placeholders = (0..chunk.len()).map(|_| "?").collect::<Vec<_>>().join(",");

		// Query returns:
		// - non_tombstone_version: Latest version where is_tombstone=0 (may be NULL)
		// - latest_version: Absolute latest version regardless of tombstone status
		// - latest_is_tombstone: Whether the latest version is a tombstone
		let query = format!(
			"SELECT key,
			        MAX(CASE WHEN is_tombstone = 0 THEN version END) as non_tombstone_version,
			        MAX(version) as latest_version,
			        (SELECT is_tombstone FROM {} WHERE key = outer.key ORDER BY version DESC LIMIT 1) as latest_is_tombstone
			 FROM {} as outer
			 WHERE key IN ({})
			 GROUP BY key",
			source, source, placeholders,
		);

		let mut stmt = conn.prepare(&query)?;
		let params: Vec<&dyn rusqlite::ToSql> = chunk.iter().map(|k| k as &dyn rusqlite::ToSql).collect();

		let rows = stmt.query_map(params_from_iter(params), |row| {
			let key = row.get::<_, Vec<u8>>(0)?;
			let non_tombstone_version: Option<i64> = row.get(1)?;
			let latest_is_tombstone: i64 = row.get(3)?;

			// If there's a non-tombstone version, use it
			if let Some(version) = non_tombstone_version {
				Ok(Some((key, (CommitVersion(version as u64), latest_is_tombstone != 0))))
			} else {
				// No non-tombstone version exists (all are tombstones)
				Ok(None)
			}
		})?;

		for result in rows {
			if let Some((key, version_info)) = result? {
				results.insert(key, version_info);
			}
		}
	}

	Ok(results)
}

/// Helper function to build query template
/// Now uses correlated subquery for better performance
pub(crate) fn build_range_query(
	start_bound: Bound<&EncodedKey>,
	end_bound: Bound<&EncodedKey>,
	order: &str, // "ASC" or "DESC"
) -> String {
	use crate::backend::sqlite::query_builder::{SortOrder, build_range_query as build_windowed_query};

	let sort_order = match order {
		"ASC" => SortOrder::Asc,
		"DESC" => SortOrder::Desc,
		_ => SortOrder::Asc,
	};

	let start = start_bound.map(|k| k.as_ref());
	let end = end_bound.map(|k| k.as_ref());

	let (sql, _param_count) = build_windowed_query("{}", sort_order, start, end);

	sql
}

/// Helper function to execute batched range queries using the new windowed query
pub(crate) fn execute_batched_range_query(
	stmt: &mut Statement,
	start_bound: Bound<&EncodedKey>,
	end_bound: Bound<&EncodedKey>,
	version: CommitVersion,
	batch_size: usize,
	buffer: &mut VecDeque<MultiVersionIterResult>,
) -> usize {
	use crate::backend::sqlite::query_builder::bind_range_params;

	// Convert bounds to byte slices
	let start = start_bound.map(|k| k.as_ref());
	let end = end_bound.map(|k| k.as_ref());

	// Build parameters in the correct order
	let params = bind_range_params(start, end, version.0 as i64, batch_size as i64);

	let rows = match stmt.query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |values| {
		let key = EncodedKey(CowVec::new(values.get::<_, Vec<u8>>(0)?));
		let value: Option<Vec<u8>> = values.get(1)?;
		let version = CommitVersion(values.get::<_, i64>(2)? as u64);

		match value {
			Some(val) => Ok(MultiVersionIterResult::Value(MultiVersionValues {
				key,
				values: EncodedValues(CowVec::new(val)),
				version,
			})),
			None => Ok(MultiVersionIterResult::Tombstone {
				key,
				version,
			}), // NULL value means tombstone (should be filtered by query)
		}
	}) {
		Ok(rows) => rows,
		Err(_) => return 0,
	};

	let mut count = 0;
	for result in rows {
		match result {
			Ok(v) => {
				buffer.push_back(v);
				count += 1;
			}
			Err(_) => break,
		}
	}

	count
}

/// Helper function to get all source names for iteration
pub(crate) fn get_source_names(conn: &ReadConnection) -> Vec<String> {
	let conn_guard = match conn.lock() {
		Ok(guard) => guard,
		Err(_) => return Vec::new(), // Lock poisoned, return empty list
	};

	let mut stmt = match conn_guard.prepare(
		"SELECT name FROM sqlite_master WHERE type='table' AND (name='multi' OR name LIKE 'source_%' OR name LIKE 'operator_%')",
	) {
		Ok(stmt) => stmt,
		Err(_) => return Vec::new(), // Failed to prepare statement
	};

	let rows = match stmt.query_map([], |values| Ok(values.get::<_, String>(0)?)) {
		Ok(rows) => rows,
		Err(_) => return Vec::new(), // Query failed
	};

	rows.filter_map(Result::ok).collect()
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

		let conn_guard = match conn.lock() {
			Ok(guard) => guard,
			Err(_) => continue, // Lock poisoned, skip this source
		};

		let mut stmt = match conn_guard.prepare(&query) {
			Ok(stmt) => stmt,
			Err(_) => continue, // Failed to prepare statement, skip this source
		};

		let rows = match stmt.query_map(rusqlite::params_from_iter(params.iter()), |values| {
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
		}) {
			Ok(rows) => rows,
			Err(_) => continue, // Query failed, skip this source
		};

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
