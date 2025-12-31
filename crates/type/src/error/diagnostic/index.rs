// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::Diagnostic;
use crate::Fragment;

pub fn primary_key_violation(fragment: Fragment, table_name: String, key_columns: Vec<String>) -> Diagnostic {
	let columns_str = if key_columns.is_empty() {
		"(unknown columns)".to_string()
	} else {
		format!("({})", key_columns.join(", "))
	};

	Diagnostic {
		code: "INDEX_001".to_string(),
		statement: None,
		message: format!(
			"Primary key violation: duplicate key in table '{}' for columns {}",
			table_name, columns_str
		),
		column: None,
		fragment,
		label: Some("primary key violation".to_string()),
		help: Some(format!(
			"A row with the same primary key {} already exists in table '{}'. Primary keys must be unique. Consider using a different value or updating the existing row instead.",
			columns_str, table_name
		)),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn unique_index_violation(
	fragment: Fragment,
	table_name: String,
	index_name: String,
	key_columns: Vec<String>,
) -> Diagnostic {
	let columns_str = if key_columns.is_empty() {
		"(unknown columns)".to_string()
	} else {
		format!("({})", key_columns.join(", "))
	};

	Diagnostic {
		code: "INDEX_002".to_string(),
		statement: None,
		message: format!(
			"Unique index violation: duplicate key in index '{}' on table '{}' for columns {}",
			index_name, table_name, columns_str
		),
		column: None,
		fragment,
		label: Some("unique index violation".to_string()),
		help: Some(format!(
			"A row with the same value for columns {} already exists in table '{}'. The index '{}' requires unique values. Consider using a different value or removing the uniqueness constraint.",
			columns_str, table_name, index_name
		)),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
