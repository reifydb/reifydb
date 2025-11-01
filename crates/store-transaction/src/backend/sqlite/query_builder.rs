// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::{borrow::Cow, ops::Bound};

use rusqlite::ToSql;

pub enum SortOrder {
	Asc,
	Desc,
}

/// Build a single windowed range query for MVCC:
/// - Respects MVCC snapshot via `version <= ?snapshot`
/// - Returns (sql, param_count_before_snapshot) where param_count_before_snapshot ∈ {0,1,2}
/// - Bind order: [start_key?], [end_key?], snapshot_version, limit
pub fn build_range_query(
	table: &str,               // e.g. "multi" or your per-tenant table
	order: SortOrder,          // Asc or Desc
	start_bound: Bound<&[u8]>, // your EncodedKey as &[u8]
	end_bound: Bound<&[u8]>,   // your EncodedKey as &[u8]
) -> (String, u8) {
	let mut where_parts: Vec<Cow<'static, str>> = Vec::with_capacity(4);
	let mut key_params_before_snapshot: u8 = 0;

	match start_bound {
		Bound::Unbounded => {}
		Bound::Included(_) => {
			where_parts.push(Cow::Borrowed("key >= ?"));
			key_params_before_snapshot += 1;
		}
		Bound::Excluded(_) => {
			where_parts.push(Cow::Borrowed("key > ?"));
			key_params_before_snapshot += 1;
		}
	}

	match end_bound {
		Bound::Unbounded => {}
		Bound::Included(_) => {
			where_parts.push(Cow::Borrowed("key <= ?"));
			key_params_before_snapshot += 1;
		}
		Bound::Excluded(_) => {
			where_parts.push(Cow::Borrowed("key < ?"));
			key_params_before_snapshot += 1;
		}
	}

	// Always enforce snapshot and non-tombstone
	where_parts.push(Cow::Borrowed("version <= ?"));
	where_parts.push(Cow::Borrowed("is_tombstone = 0"));

	let where_sql = where_parts.join(" AND ");

	// ORDER BY for final output
	let key_order = match order {
		SortOrder::Asc => "ASC",
		SortOrder::Desc => "DESC",
	};

	// Bind order: [start?] [end?] [snapshot_outer] [snapshot_inner] [limit]
	let sql = format!(
		"SELECT DISTINCT m1.key, m1.value, m1.version
        FROM {table} m1
        WHERE ({where_sql})
          AND m1.version = (
            SELECT MAX(m2.version)
            FROM {table} m2
            WHERE m2.key = m1.key
              AND m2.version <= ?
              AND m2.is_tombstone = 0
          )
        ORDER BY m1.key {key_order}
        LIMIT ?",
		table = table,
		where_sql = where_sql,
		key_order = key_order,
	);

	(sql, key_params_before_snapshot)
}

/// Binds parameters in the correct order: [start?], [end?], snapshot_outer, snapshot_inner, limit
/// Returns a vector of ToSql trait objects
/// Note: snapshot is bound twice - once for outer WHERE, once for correlated subquery MAX(version)
pub fn bind_range_params(
	start_bound: Bound<&[u8]>,
	end_bound: Bound<&[u8]>,
	snapshot_version: i64,
	limit: i64,
) -> Vec<Box<dyn ToSql>> {
	let mut v: Vec<Box<dyn ToSql>> = Vec::with_capacity(5);

	match start_bound {
		Bound::Unbounded => {}
		Bound::Included(s) | Bound::Excluded(s) => v.push(Box::new(s.to_vec())),
	}
	match end_bound {
		Bound::Unbounded => {}
		Bound::Included(e) | Bound::Excluded(e) => v.push(Box::new(e.to_vec())),
	}

	v.push(Box::new(snapshot_version)); // outer WHERE clause
	v.push(Box::new(snapshot_version)); // inner MAX(version) subquery
	v.push(Box::new(limit));
	v
}
