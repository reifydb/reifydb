// SPDX-License-Identifier: Apache-2.0
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
			value BLOB\
		) WITHOUT ROWID;\
		CREATE INDEX IF NOT EXISTS \"{0}__version\" ON \"{0}\" (version);",
		table_name
	)
}

pub(super) fn build_get_current_sql(table_name: &str) -> String {
	format!("SELECT version, value FROM \"{}\" WHERE key = ?1", table_name)
}

pub(super) fn build_get_many_current_sql(table_name: &str, key_count: usize) -> String {
	let placeholders = build_placeholders(key_count);
	format!("SELECT key, version, value FROM \"{}\" WHERE key IN ({})", table_name, placeholders)
}

fn build_placeholders(key_count: usize) -> String {
	let mut placeholders = String::with_capacity(key_count.saturating_mul(2));
	for i in 0..key_count {
		if i > 0 {
			placeholders.push(',');
		}
		placeholders.push('?');
	}
	placeholders
}

pub(super) fn build_upsert_current_sql(table_name: &str) -> String {
	format!(
		"INSERT INTO \"{0}\" (key, version, value) VALUES (?1, ?2, ?3) \
		 ON CONFLICT(key) DO UPDATE SET \
		     version = excluded.version, \
		     value = excluded.value \
		 WHERE excluded.version >= \"{0}\".version",
		table_name
	)
}

pub(super) fn build_delete_below_version_sql(table_name: &str, has_prefix: bool) -> String {
	if has_prefix {
		format!("DELETE FROM \"{0}\" WHERE version <= ?1 AND key >= ?2 AND key < ?3 RETURNING key", table_name)
	} else {
		format!("DELETE FROM \"{0}\" WHERE version <= ?1 RETURNING key", table_name)
	}
}

pub(super) fn prefix_upper_bound(prefix: &[u8]) -> Vec<u8> {
	let mut upper = prefix.to_vec();
	while let Some(last) = upper.last_mut() {
		if *last < 0xFF {
			*last += 1;
			return upper;
		}
		upper.pop();
	}
	upper
}

pub(super) fn build_delete_keys_sql(table_name: &str, key_count: usize) -> String {
	let placeholders = build_placeholders(key_count);
	format!("DELETE FROM \"{}\" WHERE key IN ({})", table_name, placeholders)
}

pub(super) fn build_delete_key_through_sql(table_name: &str) -> String {
	format!("DELETE FROM \"{}\" WHERE key = ?1 AND version <= ?2", table_name)
}

pub(super) fn build_range_consistent_sql(table_name: &str, start: Bound<()>, end: Bound<()>) -> String {
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
	sql.push_str(" AND version <= ? ORDER BY key ASC");
	sql
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
