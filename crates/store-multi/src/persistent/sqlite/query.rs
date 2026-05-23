// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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

pub(super) fn build_create_current_sql(table_name: &str) -> String {
	format!(
		"CREATE TABLE IF NOT EXISTS \"{0}\" (\
			key BLOB PRIMARY KEY,\
			version BLOB NOT NULL,\
			value BLOB,\
			created_nanos INTEGER NOT NULL DEFAULT 0,\
			updated_nanos INTEGER NOT NULL DEFAULT 0\
		) WITHOUT ROWID;\
		CREATE INDEX IF NOT EXISTS \"{0}__created_nanos\" ON \"{0}\" (created_nanos);\
		CREATE INDEX IF NOT EXISTS \"{0}__updated_nanos\" ON \"{0}\" (updated_nanos);",
		table_name
	)
}

pub(super) fn build_get_current_sql(table_name: &str) -> String {
	format!("SELECT version, value FROM \"{}\" WHERE key = ?1", table_name)
}

pub(super) fn build_get_many_current_sql(table_name: &str, key_count: usize) -> String {
	let placeholders = (1..=key_count).map(|i| format!("?{i}")).collect::<Vec<_>>().join(",");
	format!("SELECT key, version, value FROM \"{}\" WHERE key IN ({})", table_name, placeholders)
}

pub(super) fn build_upsert_current_sql(table_name: &str) -> String {
	format!(
		"INSERT INTO \"{0}\" (key, version, value, created_nanos, updated_nanos) VALUES (?1, ?2, ?3, ?4, ?5) \
		 ON CONFLICT(key) DO UPDATE SET \
		     version = excluded.version, \
		     value = excluded.value, \
		     created_nanos = excluded.created_nanos, \
		     updated_nanos = excluded.updated_nanos \
		 WHERE excluded.version >= \"{0}\".version",
		table_name
	)
}

pub(super) fn build_delete_expired_sql(table_name: &str, anchor_column: &str) -> String {
	format!("DELETE FROM \"{0}\" WHERE {1} > 0 AND {1} <= ?1", table_name, anchor_column)
}

pub(super) fn build_delete_keys_sql(table_name: &str, key_count: usize) -> String {
	let placeholders = (1..=key_count).map(|i| format!("?{i}")).collect::<Vec<_>>().join(",");
	format!("DELETE FROM \"{}\" WHERE key IN ({})", table_name, placeholders)
}

pub(super) fn build_range_current_sql(
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
