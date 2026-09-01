// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_core::common::CommitVersion;
use reifydb_sqlite::batch::values_placeholders;

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
			updated_at INTEGER\
		) WITHOUT ROWID;\
		CREATE INDEX IF NOT EXISTS \"{0}__expiry\" ON \"{0}\" (updated_at) \
			WHERE value IS NOT NULL AND updated_at IS NOT NULL;",
		table_name
	)
}

pub(super) fn build_expired_keys_sql(table_name: &str, has_cursor: bool, limit: usize) -> String {
	let mut sql = format!(
		"SELECT key, updated_at FROM \"{0}\" \
		 WHERE value IS NOT NULL AND updated_at IS NOT NULL AND updated_at <= ?1",
		table_name
	);
	if has_cursor {
		sql.push_str(" AND (updated_at > ?2 OR (updated_at = ?2 AND key > ?3))");
	}
	sql.push_str(&format!(" ORDER BY updated_at, key LIMIT {}", limit));
	sql
}

pub(super) fn build_get_current_sql(table_name: &str) -> String {
	format!("SELECT version, value FROM \"{}\" WHERE key = ?1", table_name)
}

pub(super) fn build_current_keys_sql(table_name: &str, has_cursor: bool) -> String {
	if has_cursor {
		format!("SELECT key FROM \"{}\" WHERE key > ?1 ORDER BY key LIMIT ?2", table_name)
	} else {
		format!("SELECT key FROM \"{}\" ORDER BY key LIMIT ?1", table_name)
	}
}

pub(super) fn build_current_exists_sql(table_name: &str) -> String {
	format!("SELECT EXISTS(SELECT 1 FROM \"{}\")", table_name)
}

pub(super) fn build_max_version_sql(table_name: &str) -> String {
	format!("SELECT MAX(version) FROM \"{}\"", table_name)
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
		"INSERT INTO \"{0}\" (key, version, value, updated_at) VALUES (?1, ?2, ?3, ?4) \
		 ON CONFLICT(key) DO UPDATE SET \
		     version = excluded.version, \
		     value = excluded.value, \
		     updated_at = excluded.updated_at \
		 WHERE excluded.version >= \"{0}\".version",
		table_name
	)
}

pub(super) fn build_chunked_upsert_sql(table_name: &str, chunk: usize) -> String {
	format!(
		"INSERT INTO \"{0}\" (key, version, value, updated_at) VALUES {1} \
		 ON CONFLICT(key) DO UPDATE SET \
		     version = excluded.version, \
		     value = excluded.value, \
		     updated_at = excluded.updated_at \
		 WHERE excluded.version >= \"{0}\".version \
		 RETURNING key",
		table_name,
		values_placeholders(chunk, 4)
	)
}

pub(super) fn build_delete_current_sql(table_name: &str, key_count: usize, returning: bool) -> String {
	let placeholders = build_placeholders(key_count);
	let suffix = if returning {
		" RETURNING key"
	} else {
		""
	};
	format!("DELETE FROM \"{}\" WHERE key IN ({}) AND version <= ?{}", table_name, placeholders, suffix)
}

pub(super) fn build_delete_below_version_sql(
	table_name: &str,
	has_prefix: bool,
	has_cursor: bool,
	limit: usize,
) -> String {
	let mut inner = format!("SELECT key FROM \"{0}\" WHERE version <= ?1", table_name);
	if has_prefix {
		inner.push_str(" AND key >= ?2 AND key < ?3");
	}
	if has_cursor {
		let param = if has_prefix {
			4
		} else {
			2
		};
		inner.push_str(&format!(" AND key > ?{}", param));
	}
	inner.push_str(&format!(" ORDER BY key LIMIT {}", limit));
	format!("DELETE FROM \"{0}\" WHERE key IN ({1}) RETURNING key", table_name, inner)
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
pub(super) fn build_create_current_sql_row(table_name: &str) -> String {
	format!(
		"CREATE TABLE IF NOT EXISTS \"{0}\" (\
			key INTEGER PRIMARY KEY,\
			version BLOB NOT NULL,\
			value BLOB,\
			updated_at INTEGER\
		);\
		CREATE INDEX IF NOT EXISTS \"{0}__expiry\" ON \"{0}\" (updated_at) \
			WHERE value IS NOT NULL AND updated_at IS NOT NULL;",
		table_name
	)
}

pub(super) fn build_create_current_sql_partitioned(table_name: &str) -> String {
	format!(
		"CREATE TABLE IF NOT EXISTS \"{0}\" (\
			partition_hi INTEGER NOT NULL,\
			partition_lo INTEGER NOT NULL,\
			row INTEGER NOT NULL,\
			version BLOB NOT NULL,\
			value BLOB,\
			updated_at INTEGER,\
			PRIMARY KEY (partition_hi, partition_lo, row)\
		) WITHOUT ROWID;\
		CREATE INDEX IF NOT EXISTS \"{0}__expiry\" ON \"{0}\" (updated_at) \
			WHERE value IS NOT NULL AND updated_at IS NOT NULL;",
		table_name
	)
}

pub(super) fn build_get_current_sql_row(table_name: &str) -> String {
	format!("SELECT version, value FROM \"{}\" WHERE key = ?1", table_name)
}

pub(super) fn build_get_current_sql_partitioned(table_name: &str) -> String {
	format!(
		"SELECT version, value FROM \"{}\" WHERE partition_hi = ?1 AND partition_lo = ?2 AND row = ?3",
		table_name
	)
}

pub(super) fn build_upsert_current_sql_row(table_name: &str) -> String {
	format!(
		"INSERT INTO \"{0}\" (key, version, value, updated_at) VALUES (?1, ?2, ?3, ?4) \
		 ON CONFLICT(key) DO UPDATE SET \
		     version = excluded.version, \
		     value = excluded.value, \
		     updated_at = excluded.updated_at \
		 WHERE excluded.version >= \"{0}\".version",
		table_name
	)
}

pub(super) fn build_upsert_current_sql_partitioned(table_name: &str) -> String {
	format!(
		"INSERT INTO \"{0}\" (partition_hi, partition_lo, row, version, value, updated_at) \
		 VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
		 ON CONFLICT(partition_hi, partition_lo, row) DO UPDATE SET \
		     version = excluded.version, \
		     value = excluded.value, \
		     updated_at = excluded.updated_at \
		 WHERE excluded.version >= \"{0}\".version",
		table_name
	)
}

pub(super) fn build_chunked_upsert_sql_row(table_name: &str, chunk: usize) -> String {
	format!(
		"INSERT INTO \"{0}\" (key, version, value, updated_at) VALUES {1} \
		 ON CONFLICT(key) DO UPDATE SET \
		     version = excluded.version, \
		     value = excluded.value, \
		     updated_at = excluded.updated_at \
		 WHERE excluded.version >= \"{0}\".version \
		 RETURNING key",
		table_name,
		values_placeholders(chunk, 4)
	)
}

pub(super) fn build_chunked_upsert_sql_partitioned(table_name: &str, chunk: usize) -> String {
	format!(
		"INSERT INTO \"{0}\" (partition_hi, partition_lo, row, version, value, updated_at) VALUES {1} \
		 ON CONFLICT(partition_hi, partition_lo, row) DO UPDATE SET \
		     version = excluded.version, \
		     value = excluded.value, \
		     updated_at = excluded.updated_at \
		 WHERE excluded.version >= \"{0}\".version \
		 RETURNING partition_hi, partition_lo, row",
		table_name,
		values_placeholders(chunk, 6)
	)
}

pub(super) fn build_delete_current_sql_row(table_name: &str, key_count: usize, returning: bool) -> String {
	let placeholders = build_placeholders(key_count);
	let suffix = if returning {
		" RETURNING key"
	} else {
		""
	};
	format!("DELETE FROM \"{}\" WHERE key IN ({}) AND version <= ?{}", table_name, placeholders, suffix)
}

fn build_triple_placeholders(key_count: usize) -> String {
	let mut out = String::new();
	for i in 0..key_count {
		if i > 0 {
			out.push_str(" OR ");
		}
		out.push_str("(partition_hi = ? AND partition_lo = ? AND row = ?)");
	}
	out
}

pub(super) fn build_delete_current_sql_partitioned(table_name: &str, key_count: usize, returning: bool) -> String {
	let clause = build_triple_placeholders(key_count);
	let suffix = if returning {
		" RETURNING partition_hi, partition_lo, row"
	} else {
		""
	};
	format!("DELETE FROM \"{}\" WHERE ({}) AND version <= ?{}", table_name, clause, suffix)
}

pub(super) fn build_delete_keys_sql_row(table_name: &str, key_count: usize) -> String {
	let placeholders = build_placeholders(key_count);
	format!("DELETE FROM \"{}\" WHERE key IN ({})", table_name, placeholders)
}

pub(super) fn build_delete_keys_sql_partitioned(table_name: &str, key_count: usize) -> String {
	let clause = build_triple_placeholders(key_count);
	format!("DELETE FROM \"{}\" WHERE {}", table_name, clause)
}

pub(super) fn build_current_keys_sql_row(table_name: &str, has_cursor: bool) -> String {
	if has_cursor {
		format!("SELECT key FROM \"{}\" WHERE key > ?1 ORDER BY key LIMIT ?2", table_name)
	} else {
		format!("SELECT key FROM \"{}\" ORDER BY key LIMIT ?1", table_name)
	}
}

pub(super) fn build_current_keys_sql_partitioned(table_name: &str, has_cursor: bool) -> String {
	if has_cursor {
		format!(
			"SELECT partition_hi, partition_lo, row FROM \"{0}\" \
			 WHERE (partition_hi, partition_lo, row) > (?1, ?2, ?3) \
			 ORDER BY partition_hi, partition_lo, row LIMIT ?4",
			table_name
		)
	} else {
		format!(
			"SELECT partition_hi, partition_lo, row FROM \"{0}\" \
			 ORDER BY partition_hi, partition_lo, row LIMIT ?1",
			table_name
		)
	}
}

pub(super) fn build_get_many_current_sql_row(table_name: &str, key_count: usize) -> String {
	let placeholders = build_placeholders(key_count);
	format!("SELECT key, version, value FROM \"{}\" WHERE key IN ({})", table_name, placeholders)
}

pub(super) fn build_get_many_current_sql_partitioned(table_name: &str, key_count: usize) -> String {
	let clause = build_triple_placeholders(key_count);
	format!("SELECT partition_hi, partition_lo, row, version, value FROM \"{}\" WHERE {}", table_name, clause)
}

pub(super) fn build_expired_keys_sql_row(table_name: &str, has_cursor: bool, limit: usize) -> String {
	let mut sql = format!(
		"SELECT key, updated_at FROM \"{0}\" \
		 WHERE value IS NOT NULL AND updated_at IS NOT NULL AND updated_at <= ?1",
		table_name
	);
	if has_cursor {
		sql.push_str(" AND (updated_at > ?2 OR (updated_at = ?2 AND key > ?3))");
	}
	sql.push_str(&format!(" ORDER BY updated_at, key LIMIT {}", limit));
	sql
}

pub(super) fn build_expired_keys_sql_partitioned(table_name: &str, has_cursor: bool, limit: usize) -> String {
	let mut sql = format!(
		"SELECT partition_hi, partition_lo, row, updated_at FROM \"{0}\" \
		 WHERE value IS NOT NULL AND updated_at IS NOT NULL AND updated_at <= ?1",
		table_name
	);
	if has_cursor {
		sql.push_str(
			" AND (updated_at > ?2 OR (updated_at = ?2 AND (partition_hi, partition_lo, row) > (?3, ?4, ?5)))",
		);
	}
	sql.push_str(&format!(" ORDER BY updated_at, partition_hi, partition_lo, row LIMIT {}", limit));
	sql
}

pub(super) fn build_delete_below_version_sql_row(
	table_name: &str,
	lower: Bound<()>,
	upper: Bound<()>,
	has_cursor: bool,
	limit: usize,
) -> String {
	let mut inner = format!("SELECT key FROM \"{0}\" WHERE version <= ?1", table_name);
	let mut next = 2;
	match lower {
		Bound::Included(()) => {
			inner.push_str(&format!(" AND key >= ?{}", next));
			next += 1;
		}
		Bound::Excluded(()) => {
			inner.push_str(&format!(" AND key > ?{}", next));
			next += 1;
		}
		Bound::Unbounded => {}
	}
	match upper {
		Bound::Included(()) => {
			inner.push_str(&format!(" AND key <= ?{}", next));
			next += 1;
		}
		Bound::Excluded(()) => {
			inner.push_str(&format!(" AND key < ?{}", next));
			next += 1;
		}
		Bound::Unbounded => {}
	}
	if has_cursor {
		inner.push_str(&format!(" AND key > ?{}", next));
	}
	inner.push_str(&format!(" ORDER BY key LIMIT {}", limit));
	format!("DELETE FROM \"{0}\" WHERE key IN ({1}) RETURNING key", table_name, inner)
}

pub(super) fn build_delete_below_version_sql_partitioned(
	table_name: &str,
	lower: Bound<()>,
	upper: Bound<()>,
	has_cursor: bool,
	limit: usize,
) -> String {
	let mut inner = format!("SELECT partition_hi, partition_lo, row FROM \"{0}\" WHERE version <= ?1", table_name);
	let mut next = 2;
	match lower {
		Bound::Included(()) => {
			inner.push_str(&format!(
				" AND (partition_hi, partition_lo, row) >= (?{}, ?{}, ?{})",
				next,
				next + 1,
				next + 2
			));
			next += 3;
		}
		Bound::Excluded(()) => {
			inner.push_str(&format!(
				" AND (partition_hi, partition_lo, row) > (?{}, ?{}, ?{})",
				next,
				next + 1,
				next + 2
			));
			next += 3;
		}
		Bound::Unbounded => {}
	}
	match upper {
		Bound::Included(()) => {
			inner.push_str(&format!(
				" AND (partition_hi, partition_lo, row) <= (?{}, ?{}, ?{})",
				next,
				next + 1,
				next + 2
			));
			next += 3;
		}
		Bound::Excluded(()) => {
			inner.push_str(&format!(
				" AND (partition_hi, partition_lo, row) < (?{}, ?{}, ?{})",
				next,
				next + 1,
				next + 2
			));
			next += 3;
		}
		Bound::Unbounded => {}
	}
	if has_cursor {
		inner.push_str(&format!(
			" AND (partition_hi, partition_lo, row) > (?{}, ?{}, ?{})",
			next,
			next + 1,
			next + 2
		));
	}
	inner.push_str(&format!(" ORDER BY partition_hi, partition_lo, row LIMIT {}", limit));
	format!(
		"DELETE FROM \"{0}\" WHERE (partition_hi, partition_lo, row) IN ({1}) \
		 RETURNING partition_hi, partition_lo, row",
		table_name, inner
	)
}

pub(super) fn build_delete_below_version_sql_partitioned_exact(
	table_name: &str,
	lower_row: Bound<()>,
	upper_row: Bound<()>,
	has_cursor: bool,
	limit: usize,
) -> String {
	let mut inner = format!(
		"SELECT partition_hi, partition_lo, row FROM \"{0}\" \
		 WHERE version <= ?1 AND partition_hi = ?2 AND partition_lo = ?3",
		table_name
	);
	let mut next = 4;
	match lower_row {
		Bound::Included(()) => {
			inner.push_str(&format!(" AND row >= ?{}", next));
			next += 1;
		}
		Bound::Excluded(()) => {
			inner.push_str(&format!(" AND row > ?{}", next));
			next += 1;
		}
		Bound::Unbounded => {}
	}
	match upper_row {
		Bound::Included(()) => {
			inner.push_str(&format!(" AND row <= ?{}", next));
			next += 1;
		}
		Bound::Excluded(()) => {
			inner.push_str(&format!(" AND row < ?{}", next));
			next += 1;
		}
		Bound::Unbounded => {}
	}
	if has_cursor {
		inner.push_str(&format!(" AND row > ?{}", next));
	}
	inner.push_str(&format!(" ORDER BY row LIMIT {}", limit));
	format!(
		"DELETE FROM \"{0}\" WHERE (partition_hi, partition_lo, row) IN ({1}) \
		 RETURNING partition_hi, partition_lo, row",
		table_name, inner
	)
}

pub(super) fn build_range_current_sql_row(
	table_name: &str,
	lower: Bound<()>,
	upper: Bound<()>,
	has_last_key: bool,
	descending: bool,
) -> String {
	let mut sql = format!("SELECT key, version, value FROM \"{}\" WHERE 1=1", table_name);
	match lower {
		Bound::Included(()) => sql.push_str(" AND key >= ?"),
		Bound::Excluded(()) => sql.push_str(" AND key > ?"),
		Bound::Unbounded => {}
	}
	match upper {
		Bound::Included(()) => sql.push_str(" AND key <= ?"),
		Bound::Excluded(()) => sql.push_str(" AND key < ?"),
		Bound::Unbounded => {}
	}
	if has_last_key {
		sql.push_str(if descending {
			" AND key > ?"
		} else {
			" AND key < ?"
		});
	}
	sql.push_str(" AND value IS NOT NULL AND version <= ?");
	if descending {
		sql.push_str(" ORDER BY key ASC LIMIT ?");
	} else {
		sql.push_str(" ORDER BY key DESC LIMIT ?");
	}
	sql
}

pub(super) fn build_range_current_sql_partitioned(
	table_name: &str,
	lower: Bound<()>,
	upper: Bound<()>,
	has_last_key: bool,
	descending: bool,
) -> String {
	let mut sql =
		format!("SELECT partition_hi, partition_lo, row, version, value FROM \"{}\" WHERE 1=1", table_name);
	match lower {
		Bound::Included(()) => sql.push_str(" AND (partition_hi, partition_lo, row) >= (?, ?, ?)"),
		Bound::Excluded(()) => sql.push_str(" AND (partition_hi, partition_lo, row) > (?, ?, ?)"),
		Bound::Unbounded => {}
	}
	match upper {
		Bound::Included(()) => sql.push_str(" AND (partition_hi, partition_lo, row) <= (?, ?, ?)"),
		Bound::Excluded(()) => sql.push_str(" AND (partition_hi, partition_lo, row) < (?, ?, ?)"),
		Bound::Unbounded => {}
	}
	if has_last_key {
		sql.push_str(if descending {
			" AND (partition_hi, partition_lo, row) > (?, ?, ?)"
		} else {
			" AND (partition_hi, partition_lo, row) < (?, ?, ?)"
		});
	}
	sql.push_str(" AND value IS NOT NULL AND version <= ?");
	if descending {
		sql.push_str(" ORDER BY partition_hi ASC, partition_lo ASC, row ASC LIMIT ?");
	} else {
		sql.push_str(" ORDER BY partition_hi DESC, partition_lo DESC, row DESC LIMIT ?");
	}
	sql
}

pub(super) fn build_range_current_sql_partitioned_exact(
	table_name: &str,
	lower_row: Bound<()>,
	upper_row: Bound<()>,
	has_last_key: bool,
	descending: bool,
) -> String {
	let mut sql = format!(
		"SELECT partition_hi, partition_lo, row, version, value FROM \"{}\" \
		 WHERE partition_hi = ? AND partition_lo = ?",
		table_name
	);
	match lower_row {
		Bound::Included(()) => sql.push_str(" AND row >= ?"),
		Bound::Excluded(()) => sql.push_str(" AND row > ?"),
		Bound::Unbounded => {}
	}
	match upper_row {
		Bound::Included(()) => sql.push_str(" AND row <= ?"),
		Bound::Excluded(()) => sql.push_str(" AND row < ?"),
		Bound::Unbounded => {}
	}
	if has_last_key {
		sql.push_str(if descending {
			" AND row > ?"
		} else {
			" AND row < ?"
		});
	}
	sql.push_str(" AND value IS NOT NULL AND version <= ?");
	if descending {
		sql.push_str(" ORDER BY row ASC LIMIT ?");
	} else {
		sql.push_str(" ORDER BY row DESC LIMIT ?");
	}
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
	sql.push_str(" AND value IS NOT NULL AND version <= ?");
	if descending {
		sql.push_str(" ORDER BY key DESC LIMIT ?");
	} else {
		sql.push_str(" ORDER BY key ASC LIMIT ?");
	}
	sql
}
