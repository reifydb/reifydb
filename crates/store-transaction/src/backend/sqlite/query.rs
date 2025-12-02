// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! SQL query builders for SQLite backend.

use std::ops::Bound;

/// Build a range query with the given bounds and limit.
pub(super) fn build_range_query(
	table_name: &str,
	start: Bound<&[u8]>,
	end: Bound<&[u8]>,
	reverse: bool,
	limit: usize,
) -> (String, Vec<Vec<u8>>) {
	let mut conditions = Vec::new();
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
		"SELECT key, value FROM \"{}\"{}  ORDER BY key {} LIMIT {}",
		table_name, where_clause, order, limit
	);

	(query, params)
}
