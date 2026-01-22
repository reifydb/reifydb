// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! SQL query builders for SQLite backend with MVCC versioning.
//!
//! The table schema uses (key, version) composite primary key:
//! ```sql
//! CREATE TABLE IF NOT EXISTS "{table_name}" (
//!     key BLOB NOT NULL,
//!     version BLOB NOT NULL,
//!     value BLOB,
//!     PRIMARY KEY (key, version)
//! ) WITHOUT ROWID;
//! ```

use std::ops::Bound;

use reifydb_core::common::CommitVersion;

#[inline]
pub(super) fn version_to_bytes(version: CommitVersion) -> [u8; 8] {
	version.0.to_be_bytes()
}

/// Build a range query that returns the latest version <= requested version for each key.
///
/// Uses a subquery with window function to get the most recent version per key:
/// ```sql
/// SELECT key, version, value FROM (
///     SELECT key, version, value,
///            ROW_NUMBER() OVER (PARTITION BY key ORDER BY version DESC) as rn
///     FROM "{table}" WHERE key >= ?1 AND key < ?2 AND version <= ?3
/// ) WHERE rn = 1 ORDER BY key LIMIT ?4
/// ```
pub(super) fn build_versioned_range_query(
	table_name: &str,
	start: Bound<&[u8]>,
	end: Bound<&[u8]>,
	version: CommitVersion,
	reverse: bool,
	limit: usize,
) -> (String, Vec<QueryParam>) {
	let mut conditions = Vec::new();
	let mut params: Vec<QueryParam> = Vec::new();

	match start {
		Bound::Included(v) => {
			conditions.push(format!("key >= ?{}", params.len() + 1));
			params.push(QueryParam::Blob(v.to_vec()));
		}
		Bound::Excluded(v) => {
			conditions.push(format!("key > ?{}", params.len() + 1));
			params.push(QueryParam::Blob(v.to_vec()));
		}
		Bound::Unbounded => {}
	}

	match end {
		Bound::Included(v) => {
			conditions.push(format!("key <= ?{}", params.len() + 1));
			params.push(QueryParam::Blob(v.to_vec()));
		}
		Bound::Excluded(v) => {
			conditions.push(format!("key < ?{}", params.len() + 1));
			params.push(QueryParam::Blob(v.to_vec()));
		}
		Bound::Unbounded => {}
	}

	conditions.push(format!("version <= ?{}", params.len() + 1));
	params.push(QueryParam::Version(version_to_bytes(version)));

	let where_clause = format!(" WHERE {}", conditions.join(" AND "));

	let order = if reverse {
		"DESC"
	} else {
		"ASC"
	};

	// Use window function to get the most recent version per key
	let query = format!(
		"SELECT key, version, value FROM (\
			SELECT key, version, value, \
				ROW_NUMBER() OVER (PARTITION BY key ORDER BY version DESC) as rn \
			FROM \"{}\"{}\
		) WHERE rn = 1 ORDER BY key {} LIMIT {}",
		table_name, where_clause, order, limit
	);

	(query, params)
}

/// Query parameter type for SQLite queries.
#[derive(Debug, Clone)]
pub(super) enum QueryParam {
	Blob(Vec<u8>),
	Version([u8; 8]),
}

impl rusqlite::ToSql for QueryParam {
	fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
		match self {
			QueryParam::Blob(v) => v.to_sql(),
			QueryParam::Version(v) => v.as_slice().to_sql(),
		}
	}
}
