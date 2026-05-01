// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ops::Bound;

use reifydb_core::common::CommitVersion;

#[inline]
pub(super) fn version_to_bytes(version: CommitVersion) -> [u8; 8] {
	version.0.to_be_bytes()
}

#[inline]
pub(super) fn version_from_bytes(bytes: &[u8]) -> CommitVersion {
	CommitVersion(u64::from_be_bytes(bytes.try_into().expect("version must be 8 bytes")))
}

/// DDL: create the `__current` table for a logical table.
/// One row per logical key; `key` is the sole primary key.
pub(super) fn build_create_current_sql(table_name: &str) -> String {
	format!(
		"CREATE TABLE IF NOT EXISTS \"{}\" (\
			key BLOB PRIMARY KEY,\
			version BLOB NOT NULL,\
			value BLOB\
		) WITHOUT ROWID",
		table_name
	)
}

/// DDL: create the `__historical` table for a logical table.
/// Many rows per logical key; PK is `(key, version)` so that
/// `WHERE key = ? AND version <= ? ORDER BY version DESC LIMIT 1`
/// is a single index seek + step.
pub(super) fn build_create_historical_sql(table_name: &str) -> String {
	format!(
		"CREATE TABLE IF NOT EXISTS \"{}\" (\
			key BLOB NOT NULL,\
			version BLOB NOT NULL,\
			value BLOB,\
			PRIMARY KEY (key, version)\
		) WITHOUT ROWID",
		table_name
	)
}

/// Point-get from `__current`. Returns the row whose `version` may or may
/// not satisfy the snapshot constraint; the caller checks and falls back
/// to `__historical` if not.
pub(super) fn build_get_current_sql(current_name: &str) -> String {
	format!("SELECT version, value FROM \"{}\" WHERE key = ?1", current_name)
}

/// Point-get from `__historical` for a key, latest visible <= snapshot.
pub(super) fn build_get_historical_sql(historical_name: &str) -> String {
	format!(
		"SELECT version, value FROM \"{}\" WHERE key = ?1 AND version <= ?2 ORDER BY version DESC LIMIT 1",
		historical_name
	)
}

/// Range scan over `__current`, optionally filtered by inclusive/exclusive
/// key bounds. Streamable via the PK index; SQLite stops after `LIMIT N`.
pub(super) fn build_range_current_query(
	current_name: &str,
	start: Bound<&[u8]>,
	end: Bound<&[u8]>,
	reverse: bool,
	limit: usize,
) -> (String, Vec<Vec<u8>>) {
	let mut conditions: Vec<String> = Vec::new();
	let mut params: Vec<Vec<u8>> = Vec::new();

	match start {
		Bound::Included(v) => {
			conditions.push(format!("key >= ?{}", params.len() + 1));
			params.push(v.to_vec());
		}
		Bound::Excluded(v) => {
			conditions.push(format!("key > ?{}", params.len() + 1));
			params.push(v.to_vec());
		}
		Bound::Unbounded => {}
	}

	match end {
		Bound::Included(v) => {
			conditions.push(format!("key <= ?{}", params.len() + 1));
			params.push(v.to_vec());
		}
		Bound::Excluded(v) => {
			conditions.push(format!("key < ?{}", params.len() + 1));
			params.push(v.to_vec());
		}
		Bound::Unbounded => {}
	}

	let where_clause = if conditions.is_empty() {
		String::new()
	} else {
		format!(" WHERE {}", conditions.join(" AND "))
	};

	let order = if reverse {
		"DESC"
	} else {
		"ASC"
	};

	let query = format!(
		"SELECT key, version, value FROM \"{}\"{} ORDER BY key {} LIMIT {}",
		current_name, where_clause, order, limit
	);

	(query, params)
}

/// Get all versions of a key by unioning the row from `__current` with all
/// rows in `__historical`. Result is sorted descending by version.
pub(super) fn build_get_all_versions_sql(current_name: &str, historical_name: &str) -> String {
	format!(
		"SELECT version, value FROM \"{current}\" WHERE key = ?1 \
		 UNION ALL \
		 SELECT version, value FROM \"{historical}\" WHERE key = ?1 \
		 ORDER BY version DESC",
		current = current_name,
		historical = historical_name,
	)
}
