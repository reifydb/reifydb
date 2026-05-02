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

/// DDL: create the warm `__warm_current` table for a logical table.
/// One row per logical key; `key` is the sole primary key. No historical chain.
pub(super) fn build_create_warm_current_sql(table_name: &str) -> String {
	format!(
		"CREATE TABLE IF NOT EXISTS \"{}\" (\
			key BLOB PRIMARY KEY,\
			version BLOB NOT NULL,\
			value BLOB\
		) WITHOUT ROWID",
		table_name
	)
}

/// Point-get from `__warm_current`. Returns the row if any.
pub(super) fn build_get_warm_current_sql(table_name: &str) -> String {
	format!("SELECT version, value FROM \"{}\" WHERE key = ?1", table_name)
}

/// Upsert into `__warm_current`. The conflict guard `WHERE excluded.version >= "<table>".version`
/// prevents an out-of-order or stale flush from regressing warm to an older version.
pub(super) fn build_upsert_warm_current_sql(table_name: &str) -> String {
	format!(
		"INSERT INTO \"{0}\" (key, version, value) VALUES (?1, ?2, ?3) \
		 ON CONFLICT(key) DO UPDATE SET \
		     version = excluded.version, \
		     value = excluded.value \
		 WHERE excluded.version >= \"{0}\".version",
		table_name
	)
}

/// Range scan over `__warm_current` at a snapshot version.
///
/// Bind parameters in declaration order: `[start?, end?, last_key?, version, limit]`.
/// The `version` filter (`version <= ?`) skips rows whose stored version is newer
/// than the requested snapshot, mirroring `build_get_warm_current_sql`.
///
/// `version` is stored as a big-endian 8-byte BLOB; SQLite compares BLOBs
/// lexicographically, so `<=` over those bytes is numeric `<=`.
pub(super) fn build_range_warm_current_sql(
	table_name: &str,
	start: Bound<()>,
	end: Bound<()>,
	has_last_key: bool,
	descending: bool,
) -> String {
	let mut sql = format!("SELECT key, version, value FROM \"{}\" WHERE 1=1", table_name);
	match start {
		Bound::Included(()) => sql.push_str(" AND key >= ?"),
		Bound::Excluded(()) => sql.push_str(" AND key > ?"),
		Bound::Unbounded => {}
	}
	match end {
		Bound::Included(()) => sql.push_str(" AND key <= ?"),
		Bound::Excluded(()) => sql.push_str(" AND key < ?"),
		Bound::Unbounded => {}
	}
	if has_last_key {
		sql.push_str(if descending {
			" AND key < ?"
		} else {
			" AND key > ?"
		});
	}
	sql.push_str(" AND version <= ?");
	if descending {
		sql.push_str(" ORDER BY key DESC LIMIT ?");
	} else {
		sql.push_str(" ORDER BY key ASC LIMIT ?");
	}
	sql
}
