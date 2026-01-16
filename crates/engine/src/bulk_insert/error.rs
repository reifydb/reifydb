// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Error types for bulk insert operations.

use reifydb_type::{error::diagnostic::Diagnostic, fragment::Fragment};

/// Bulk insert specific error codes and messages.
pub struct BulkInsertError;

impl BulkInsertError {
	/// Error when a column name is not found in the target schema.
	pub fn column_not_found(fragment: Fragment, source: &str, column: &str) -> reifydb_type::error::Error {
		reifydb_type::error::Error(Diagnostic {
			code: "BI_001".to_string(),
			statement: None,
			message: format!("column `{}` not found in `{}`", column, source),
			fragment,
			label: Some("unknown column".to_string()),
			help: Some("check that the column name matches the schema".to_string()),
			column: None,
			notes: vec![],
			cause: None,
			operator_chain: None,
		})
	}

	/// Error when there's a type mismatch between the value and column.
	pub fn type_mismatch(
		fragment: Fragment,
		column: &str,
		expected: &str,
		actual: &str,
	) -> reifydb_type::error::Error {
		reifydb_type::error::Error(Diagnostic {
			code: "BI_002".to_string(),
			statement: None,
			message: format!(
				"type mismatch for column `{}`: expected `{}`, got `{}`",
				column, expected, actual
			),
			fragment,
			label: Some("incompatible type".to_string()),
			help: Some("ensure the value type matches the column definition".to_string()),
			column: None,
			notes: vec![],
			cause: None,
			operator_chain: None,
		})
	}

	/// Error when more values are provided than columns exist.
	pub fn too_many_values(fragment: Fragment, expected: usize, actual: usize) -> reifydb_type::error::Error {
		reifydb_type::error::Error(Diagnostic {
			code: "BI_003".to_string(),
			statement: None,
			message: format!("too many values: expected {} columns, got {}", expected, actual),
			fragment,
			label: Some("value count mismatch".to_string()),
			help: Some("ensure the number of values matches the column count".to_string()),
			column: None,
			notes: vec![],
			cause: None,
			operator_chain: None,
		})
	}

	/// Error when the namespace is not found.
	pub fn namespace_not_found(fragment: Fragment, namespace: &str) -> reifydb_type::error::Error {
		reifydb_type::error::Error(Diagnostic {
			code: "BI_004".to_string(),
			statement: None,
			message: format!("namespace `{}` not found", namespace),
			fragment,
			label: Some("unknown namespace".to_string()),
			help: Some("create the namespace first or check the name".to_string()),
			column: None,
			notes: vec![],
			cause: None,
			operator_chain: None,
		})
	}

	/// Error when the table is not found.
	pub fn table_not_found(fragment: Fragment, namespace: &str, table: &str) -> reifydb_type::error::Error {
		reifydb_type::error::Error(Diagnostic {
			code: "BI_005".to_string(),
			statement: None,
			message: format!("table `{}.{}` not found", namespace, table),
			fragment,
			label: Some("unknown table".to_string()),
			help: Some("create the table first or check the name".to_string()),
			column: None,
			notes: vec![],
			cause: None,
			operator_chain: None,
		})
	}

	/// Error when the ring buffer is not found.
	pub fn ringbuffer_not_found(
		fragment: Fragment,
		namespace: &str,
		ringbuffer: &str,
	) -> reifydb_type::error::Error {
		reifydb_type::error::Error(Diagnostic {
			code: "BI_006".to_string(),
			statement: None,
			message: format!("ring buffer `{}.{}` not found", namespace, ringbuffer),
			fragment,
			label: Some("unknown ring buffer".to_string()),
			help: Some("create the ring buffer first or check the name".to_string()),
			column: None,
			notes: vec![],
			cause: None,
			operator_chain: None,
		})
	}
}
