// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	error, fmt,
	fmt::{Display, Formatter},
};

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::internal_error;
use reifydb_type::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
};

#[derive(Debug, Clone, PartialEq)]
pub enum OperationKind {
	Sort,
	Map,
	Filter,
	Extend,
	Patch,
	Distinct,
	Apply,
	Take,
	Aggregate,
	AggregateBy,
}

impl Display for OperationKind {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			OperationKind::Sort => f.write_str("SORT"),
			OperationKind::Map => f.write_str("MAP"),
			OperationKind::Filter => f.write_str("FILTER"),
			OperationKind::Extend => f.write_str("EXTEND"),
			OperationKind::Patch => f.write_str("PATCH"),
			OperationKind::Distinct => f.write_str("DISTINCT"),
			OperationKind::Apply => f.write_str("APPLY"),
			OperationKind::Take => f.write_str("TAKE"),
			OperationKind::Aggregate => f.write_str("AGGREGATE"),
			OperationKind::AggregateBy => f.write_str("AGGREGATE BY"),
		}
	}
}

#[derive(Debug, thiserror::Error)]
pub enum RqlError {
	#[error("TAKE operator requires non-negative value")]
	TakeNegativeValue {
		fragment: Fragment,
	},

	#[error("AGGREGATE operator requires at least one aggregation expression")]
	MissingAggregateMapBlock {
		fragment: Fragment,
	},

	#[error("Multiple {kind} expressions require curly braces")]
	OperatorMultipleWithoutBraces {
		kind: OperationKind,
		fragment: Fragment,
	},

	#[error("{kind} requires curly braces")]
	OperatorMissingBraces {
		kind: OperationKind,
		fragment: Fragment,
	},

	#[error("Sliding windows must specify a slide parameter")]
	WindowMissingSlideParameter {
		fragment: Fragment,
	},

	#[error("Slide interval must be smaller than window interval")]
	WindowSlideTooLarge {
		fragment: Fragment,
		slide_value: String,
		window_value: String,
	},

	#[error("Incompatible slide type with window type")]
	WindowIncompatibleSlideType {
		fragment: Fragment,
		window_type: String,
		slide_type: String,
	},

	#[error("Tumbling windows should not specify a slide parameter")]
	WindowTumblingWithSlide {
		fragment: Fragment,
	},

	#[error("Incompatible window type and size combination")]
	WindowIncompatibleTypeSize {
		fragment: Fragment,
		window_type: String,
		size_type: String,
	},

	#[error("Window type and size must be specified")]
	WindowMissingTypeOrSize {
		fragment: Fragment,
	},

	#[error("UPDATE requires an assignments block")]
	UpdateMissingAssignmentsBlock {
		fragment: Fragment,
	},

	#[error("UPDATE assignments block cannot be empty")]
	UpdateEmptyAssignmentsBlock {
		fragment: Fragment,
	},

	#[error("UPDATE requires a FILTER clause")]
	UpdateMissingFilterClause {
		fragment: Fragment,
	},

	#[error("DELETE requires a target table")]
	DeleteMissingTarget {
		fragment: Fragment,
	},

	#[error("DELETE requires a FILTER clause")]
	DeleteMissingFilterClause {
		fragment: Fragment,
	},

	#[error("INSERT requires a target table")]
	InsertMissingTarget {
		fragment: Fragment,
	},

	#[error("INSERT requires a data source")]
	InsertMissingSource {
		fragment: Fragment,
	},

	#[error("Cannot mix keyed and positional rows in the same INSERT")]
	InsertMixedRowTypes {
		fragment: Fragment,
	},

	#[error("Positional INSERT expects {expected} values, got {actual}")]
	InsertPositionalWrongLength {
		fragment: Fragment,
		expected: usize,
		actual: usize,
		column_names: Vec<String>,
	},

	#[error("column not found")]
	ColumnNotFound {
		fragment: Fragment,
	},

	#[error("Cannot extend with duplicate column name '{column_name}'")]
	ExtendDuplicateColumn {
		column_name: String,
	},

	#[error("Source qualification '{name}' is not supported in RQL expressions")]
	UnsupportedSourceQualification {
		fragment: Fragment,
		name: String,
	},

	#[error("Join column alias error: {message}")]
	JoinColumnAliasError {
		fragment: Fragment,
		message: String,
	},

	#[error("BREAK can only be used inside a loop")]
	BreakOutsideLoop,

	#[error("CONTINUE can only be used inside a loop")]
	ContinueOutsideLoop,

	#[error("Internal error in function {name}: {details}")]
	InternalFunctionError {
		name: String,
		fragment: Fragment,
		details: String,
	},
}

impl IntoDiagnostic for RqlError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			RqlError::TakeNegativeValue { fragment } => {
				let value = fragment.text().to_string();
				Diagnostic {
					code: "TAKE_001".to_string(),
					statement: None,
					message: format!("TAKE operator requires non-negative value, got {}", value),
					column: None,
					fragment,
					label: Some("negative value not allowed".to_string()),
					help: Some("Use a positive number to limit results, or 0 to return no rows".to_string()),
					notes: vec![
						"TAKE operator limits the number of rows returned from a query".to_string(),
						"Negative values are not meaningful in this context".to_string(),
						"Valid examples: TAKE 10, TAKE 0, TAKE 100".to_string(),
					],
					cause: None,
					operator_chain: None,
				}
			}

			RqlError::MissingAggregateMapBlock { fragment } => Diagnostic {
				code: "AGGREGATE_001".to_string(),
				statement: None,
				message: "AGGREGATE operator requires at least one aggregation expression".to_string(),
				column: None,
				fragment,
				label: Some("missing aggregation expressions".to_string()),
				help: Some("Specify aggregation functions before the BY clause, e.g., 'AGGREGATE count(id) BY category' or 'AGGREGATE { sum(amount), avg(price) } BY category'".to_string()),
				notes: vec![
					"The AGGREGATE operator requires aggregation functions like count(), sum(), avg(), min(), or max()".to_string(),
					"Use curly braces for multiple aggregations: AGGREGATE { expr1, expr2 } BY ...".to_string(),
					"For global aggregations without grouping, use: AGGREGATE count(*) BY {}".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::OperatorMultipleWithoutBraces { kind, fragment } => {
				let (code, message, label, help, notes) = match kind {
					OperationKind::Aggregate => (
						"AGGREGATE_002",
						"Multiple aggregation expressions require curly braces",
						"missing curly braces around expressions",
						"Wrap multiple aggregation expressions in curly braces, e.g., 'AGGREGATE { count(id), sum(amount), avg(price) } BY category'",
						vec![
							"When specifying multiple aggregation functions, use curly braces: AGGREGATE { expr1, expr2, ... } BY ...".to_string(),
							"Single aggregation expressions can be written without braces: AGGREGATE count(id) BY category".to_string(),
							"Curly braces make the query more readable and unambiguous".to_string(),
						],
					),
					OperationKind::AggregateBy => (
						"AGGREGATE_003",
						"Multiple grouping columns require curly braces",
						"missing curly braces around columns",
						"Wrap multiple grouping columns in curly braces, e.g., 'AGGREGATE count(id) BY { category, region, year }'",
						vec![
							"When grouping by multiple columns, use curly braces: AGGREGATE ... BY { col1, col2, ... }".to_string(),
							"Single grouping columns can be written without braces: AGGREGATE ... BY category".to_string(),
							"For global aggregations without grouping, use empty braces: AGGREGATE ... BY {}".to_string(),
						],
					),
					OperationKind::Distinct => (
						"DISTINCT_001",
						"Multiple DISTINCT columns require curly braces",
						"missing curly braces around columns",
						"Wrap multiple DISTINCT columns in curly braces, e.g., 'DISTINCT { category, value } FROM events'",
						vec![
							"When using DISTINCT with multiple columns, use curly braces: DISTINCT { col1, col2, ... }".to_string(),
							"Single columns can be written without braces: DISTINCT category FROM events".to_string(),
							"No arguments means distinct on all columns: DISTINCT FROM events".to_string(),
							"Curly braces make the query more readable and unambiguous".to_string(),
						],
					),
					OperationKind::Map => (
						"MAP_001",
						"Multiple MAP expressions require curly braces",
						"missing curly braces around expressions",
						"Wrap multiple MAP expressions in curly braces, e.g., 'MAP { name, age, email } FROM users'",
						vec![
							"When mapping multiple columns or expressions, use curly braces: MAP { expr1, expr2, ... }".to_string(),
							"Single expressions can be written without braces: MAP name FROM users".to_string(),
							"Curly braces make the query more readable and unambiguous".to_string(),
						],
					),
					OperationKind::Extend => (
						"EXTEND_001",
						"Multiple EXTEND expressions require curly braces",
						"missing curly braces around expressions",
						"Wrap multiple EXTEND expressions in curly braces, e.g., 'EXTEND { total: price * quantity, tax: price * 0.1 }'",
						vec![
							"When extending with multiple columns or expressions, use curly braces: EXTEND { expr1, expr2, ... }".to_string(),
							"Single expressions can be written without braces: EXTEND total: price * quantity".to_string(),
							"Curly braces make the query more readable and unambiguous".to_string(),
						],
					),
					OperationKind::Apply => (
						"APPLY_001",
						"Multiple APPLY arguments require curly braces",
						"missing curly braces around arguments",
						"Wrap multiple APPLY arguments in curly braces, e.g., 'APPLY operator { arg1, arg2 }'",
						vec![
							"When applying operators with multiple arguments, use curly braces: APPLY operator { arg1, arg2, ... }".to_string(),
							"Single arguments can be written without braces: APPLY running_sum amount".to_string(),
							"No arguments should use empty braces: APPLY counter {}".to_string(),
						],
					),
					_ => (
						"OP_001",
						"Multiple expressions require curly braces",
						"missing curly braces",
						"Wrap expressions in curly braces",
						vec![],
					),
				};
				Diagnostic {
					code: code.to_string(),
					statement: None,
					message: message.to_string(),
					column: None,
					fragment,
					label: Some(label.to_string()),
					help: Some(help.to_string()),
					notes,
					cause: None,
					operator_chain: None,
				}
			}

			RqlError::OperatorMissingBraces { kind, fragment } => {
				let (code, message, help, notes) = match kind {
					OperationKind::Sort => (
						"SORT_001",
						"SORT requires curly braces around columns",
						"Wrap SORT columns in curly braces, e.g., 'SORT {name}' or 'SORT {name:ASC, age:DESC}'",
						vec![
							"SORT always requires curly braces: SORT {column}".to_string(),
							"Multiple columns: SORT {col1:ASC, col2:DESC}".to_string(),
						],
					),
					OperationKind::Map => (
						"MAP_002",
						"MAP requires curly braces around expressions",
						"Wrap MAP expressions in curly braces, e.g., 'MAP {name}' or 'MAP {name, age}'",
						vec![
							"MAP always requires curly braces: MAP {expression}".to_string(),
							"Multiple expressions: MAP {expr1, expr2}".to_string(),
						],
					),
					OperationKind::Filter => (
						"FILTER_001",
						"FILTER requires curly braces around the condition",
						"Wrap FILTER condition in curly braces, e.g., 'FILTER {price > 100}'",
						vec![
							"FILTER always requires curly braces: FILTER {condition}".to_string(),
							"Example: FILTER {active == true AND price > 100}".to_string(),
						],
					),
					OperationKind::Extend => (
						"EXTEND_002",
						"EXTEND requires curly braces around expressions",
						"Wrap EXTEND expressions in curly braces, e.g., 'EXTEND {total: price * qty}'",
						vec![
							"EXTEND always requires curly braces: EXTEND {expression}".to_string(),
							"Multiple expressions: EXTEND {expr1, expr2}".to_string(),
						],
					),
					OperationKind::Patch => (
						"PATCH_001",
						"PATCH requires curly braces around expressions",
						"Wrap PATCH expressions in curly braces, e.g., 'PATCH {status: \"active\"}'",
						vec![
							"PATCH always requires curly braces: PATCH {expression}".to_string(),
							"Multiple expressions: PATCH {expr1, expr2}".to_string(),
						],
					),
					OperationKind::Distinct => (
						"DISTINCT_002",
						"DISTINCT requires curly braces around columns",
						"Wrap DISTINCT columns in curly braces, e.g., 'DISTINCT {}' or 'DISTINCT {name}'",
						vec![
							"DISTINCT always requires curly braces: DISTINCT {columns}".to_string(),
							"For all columns distinct: DISTINCT {}".to_string(),
							"Multiple columns: DISTINCT {col1, col2}".to_string(),
						],
					),
					OperationKind::Apply => (
						"APPLY_002",
						"APPLY requires curly braces around arguments",
						"Wrap APPLY arguments in curly braces, e.g., 'APPLY running_sum {value}'",
						vec![
							"APPLY always requires curly braces: APPLY operator {arguments}".to_string(),
							"No arguments: APPLY counter {}".to_string(),
							"Multiple arguments: APPLY op {arg1, arg2}".to_string(),
						],
					),
					OperationKind::Take => (
						"TAKE_002",
						"TAKE requires curly braces around the limit",
						"Wrap TAKE limit in curly braces, e.g., 'TAKE {10}' or 'TAKE {}'",
						vec![
							"TAKE always requires curly braces: TAKE {limit}".to_string(),
							"Unlimited: TAKE {}".to_string(),
							"Limited: TAKE {100}".to_string(),
						],
					),
					OperationKind::Aggregate => (
						"AGGREGATE_004",
						"AGGREGATE requires curly braces around aggregation expressions",
						"Wrap aggregation expressions in curly braces, e.g., 'AGGREGATE {count(id)} BY {name}'",
						vec![
							"AGGREGATE always requires curly braces: AGGREGATE {expressions} BY {columns}".to_string(),
							"Example: AGGREGATE {sum(amount), avg(price)} BY {category}".to_string(),
						],
					),
					OperationKind::AggregateBy => (
						"AGGREGATE_005",
						"AGGREGATE BY requires curly braces around grouping columns",
						"Wrap BY columns in curly braces, e.g., 'AGGREGATE {count(id)} BY {name}'",
						vec![
							"AGGREGATE BY always requires curly braces: BY {columns}".to_string(),
							"Global aggregation (no grouping): BY {}".to_string(),
							"Multiple columns: BY {col1, col2}".to_string(),
						],
					),
				};
				Diagnostic {
					code: code.to_string(),
					statement: None,
					message: message.to_string(),
					column: None,
					fragment,
					label: Some("missing curly braces".to_string()),
					help: Some(help.to_string()),
					notes,
					cause: None,
					operator_chain: None,
				}
			}

			RqlError::WindowMissingSlideParameter { fragment } => Diagnostic {
				code: "WINDOW_002".to_string(),
				statement: None,
				message: "Sliding windows must specify a slide parameter".to_string(),
				column: None,
				fragment,
				label: Some("missing slide parameter".to_string()),
				help: Some("Add a slide parameter to the WINDOW configuration, e.g., 'WINDOW WITH { interval: \"5m\", slide: \"1m\" }'".to_string()),
				notes: vec![
					"Sliding windows create overlapping windows by advancing in smaller steps".to_string(),
					"The slide parameter determines how far each window advances".to_string(),
					"Example: WINDOW WITH { interval: \"10m\", slide: \"2m\" } creates 10-minute windows that advance every 2 minutes".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowSlideTooLarge { fragment, slide_value, window_value } => Diagnostic {
				code: "WINDOW_003".to_string(),
				statement: None,
				message: format!(
					"Slide interval ({}) must be smaller than window interval ({}) for overlapping sliding windows",
					slide_value, window_value
				),
				column: None,
				fragment,
				label: Some("slide too large".to_string()),
				help: Some(
					"Reduce the slide value to be smaller than the window size for overlapping windows".to_string(),
				),
				notes: vec![
					"Sliding windows create overlapping segments when slide < window size".to_string(),
					"If slide >= window size, consider using tumbling windows instead".to_string(),
					"Example: For 10-minute windows, use slide values like \"2m\", \"5m\", or \"1m\"".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowIncompatibleSlideType { fragment, window_type, slide_type } => Diagnostic {
				code: "WINDOW_004".to_string(),
				statement: None,
				message: format!("Incompatible slide type {} with window type {}", slide_type, window_type),
				column: None,
				fragment,
				label: Some("mismatched types".to_string()),
				help: Some(
					"Use duration-based slide for time windows, or count-based slide for count windows".to_string(),
				),
				notes: vec![
					"Time-based windows (interval) require duration-based slide parameters (e.g., \"1m\", \"30s\")".to_string(),
					"Count-based windows (count) require numeric slide parameters (e.g., 10, 50)".to_string(),
					"Example time window: WINDOW WITH { interval: \"5m\", slide: \"1m\" }".to_string(),
					"Example count window: WINDOW WITH { count: 100, slide: 20 }".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowTumblingWithSlide { fragment } => Diagnostic {
				code: "WINDOW_005".to_string(),
				statement: None,
				message: "Tumbling windows should not specify a slide parameter".to_string(),
				column: None,
				fragment,
				label: Some("unexpected slide parameter".to_string()),
				help: Some(
					"Remove the slide parameter for tumbling windows, or use sliding windows if overlap is needed"
						.to_string(),
				),
				notes: vec![
					"Tumbling windows are non-overlapping and advance by their full size".to_string(),
					"For tumbling windows, use only: WINDOW WITH { interval: \"5m\" } or WINDOW WITH { count: 100 }".to_string(),
					"For overlapping windows, use sliding windows with both size and slide parameters".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowIncompatibleTypeSize { fragment, window_type, size_type } => Diagnostic {
				code: "WINDOW_006".to_string(),
				statement: None,
				message: format!(
					"Incompatible window type {} and size type {} for window",
					window_type, size_type
				),
				column: None,
				fragment,
				label: Some("mismatched window configuration".to_string()),
				help: Some("Use 'interval' with time-based windows or 'count' with count-based windows".to_string()),
				notes: vec![
					"Time-based windows use 'interval' parameter with duration values (e.g., \"5m\", \"1h\")".to_string(),
					"Count-based windows use 'count' parameter with numeric values (e.g., 100, 500)".to_string(),
					"Example time window: WINDOW WITH { interval: \"10m\" }".to_string(),
					"Example count window: WINDOW WITH { count: 1000 }".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::WindowMissingTypeOrSize { fragment } => Diagnostic {
				code: "WINDOW_007".to_string(),
				statement: None,
				message: "Window type and size must be specified for window".to_string(),
				column: None,
				fragment,
				label: Some("incomplete window configuration".to_string()),
				help: Some(
					"Specify either 'interval' for time-based windows or 'count' for count-based windows"
						.to_string(),
				),
				notes: vec![
					"Windows require a size specification to determine their boundaries".to_string(),
					"Use 'interval' with duration for time-based windows: WINDOW WITH { interval: \"5m\" }".to_string(),
					"Use 'count' with number for count-based windows: WINDOW WITH { count: 100 }".to_string(),
					"Additional parameters like 'slide' can be added for sliding windows".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::UpdateMissingAssignmentsBlock { fragment } => Diagnostic {
				code: "UPDATE_001".to_string(),
				statement: None,
				message: "UPDATE requires an assignments block".to_string(),
				column: None,
				fragment,
				label: Some("missing assignments block".to_string()),
				help: Some(
					"Specify the fields to update in curly braces, e.g., 'UPDATE users { name: 'alice' } FILTER id == 1'"
						.to_string(),
				),
				notes: vec![
					"The assignments block specifies which columns to update and their new values".to_string(),
					"Use colon syntax for assignments: { column: value, ... }".to_string(),
					"Example: UPDATE users { name: 'alice', age: 30 } FILTER id == 1".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::UpdateEmptyAssignmentsBlock { fragment } => Diagnostic {
				code: "UPDATE_002".to_string(),
				statement: None,
				message: "UPDATE assignments block cannot be empty".to_string(),
				column: None,
				fragment,
				label: Some("empty assignments block".to_string()),
				help: Some(
					"Specify at least one field to update, e.g., 'UPDATE users { name: 'alice' } FILTER id == 1'"
						.to_string(),
				),
				notes: vec![
					"At least one column must be specified for update".to_string(),
					"Example: UPDATE users { name: 'alice', age: 30 } FILTER id == 1".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::UpdateMissingFilterClause { fragment } => Diagnostic {
				code: "UPDATE_003".to_string(),
				statement: None,
				message: "UPDATE requires a FILTER clause".to_string(),
				column: None,
				fragment,
				label: Some("missing FILTER clause".to_string()),
				help: Some(
					"Add a FILTER clause to specify which rows to update, e.g., 'UPDATE users { name: 'alice' } FILTER id == 1'"
						.to_string(),
				),
				notes: vec![
					"The FILTER clause is required to prevent accidental mass updates".to_string(),
					"It specifies the condition for selecting rows to update".to_string(),
					"Example: UPDATE users { name: 'alice' } FILTER id == 1".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::DeleteMissingTarget { fragment } => Diagnostic {
				code: "DELETE_001".to_string(),
				statement: None,
				message: "DELETE requires a target table".to_string(),
				column: None,
				fragment,
				label: Some("missing target table".to_string()),
				help: Some("Specify the table to delete from, e.g., 'DELETE users FILTER id == 1'".to_string()),
				notes: vec![
					"The target table specifies which table to delete rows from".to_string(),
					"Example: DELETE users FILTER id == 1".to_string(),
					"Example with namespace: DELETE test::users FILTER id == 1".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::DeleteMissingFilterClause { fragment } => Diagnostic {
				code: "DELETE_002".to_string(),
				statement: None,
				message: "DELETE requires a FILTER clause".to_string(),
				column: None,
				fragment,
				label: Some("missing FILTER clause".to_string()),
				help: Some(
					"Add a FILTER clause to specify which rows to delete, e.g., 'DELETE users FILTER id == 1'"
						.to_string(),
				),
				notes: vec![
					"The FILTER clause is required to prevent accidental mass deletes".to_string(),
					"It specifies the condition for selecting rows to delete".to_string(),
					"Example: DELETE users FILTER id == 1".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::InsertMissingTarget { fragment } => Diagnostic {
				code: "INSERT_001".to_string(),
				statement: None,
				message: "INSERT requires a target table".to_string(),
				column: None,
				fragment,
				label: Some("missing target table".to_string()),
				help: Some(
					"Specify the table to insert into, e.g., 'INSERT users [{ id: 1, name: \"Alice\" }]'"
						.to_string(),
				),
				notes: vec![
					"The target table specifies where to insert the data".to_string(),
					"Example: INSERT users [{ id: 1, name: \"Alice\" }]".to_string(),
					"Example with namespace: INSERT test::users [{ id: 1, name: \"Alice\" }]".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::InsertMissingSource { fragment } => Diagnostic {
				code: "INSERT_002".to_string(),
				statement: None,
				message: "INSERT requires a data source".to_string(),
				column: None,
				fragment,
				label: Some("missing data source".to_string()),
				help: Some(
					"Specify the data to insert, e.g., 'INSERT users [{ id: 1, name: \"Alice\" }]'".to_string(),
				),
				notes: vec![
					"Use inline array directly: INSERT users [{ id: 1 }, { id: 2 }]".to_string(),
					"Use variable directly: INSERT users $data".to_string(),
					"Use FROM for table sources: INSERT target_table FROM source_table".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::InsertMixedRowTypes { fragment } => Diagnostic {
				code: "INSERT_003".to_string(),
				statement: None,
				message: "Cannot mix keyed {key: value} and positional (value, ...) rows in the same INSERT"
					.to_string(),
				column: None,
				fragment,
				label: Some("mixed row types".to_string()),
				help: Some("Use either all keyed rows [{id: 1, name: \"Alice\"}] or all positional rows [(1, \"Alice\")] in a single INSERT".to_string()),
				notes: vec![
					"Keyed rows use curly braces: { column: value, ... }".to_string(),
					"Positional rows use parentheses: (value1, value2, ...)".to_string(),
					"All rows in a single INSERT must use the same style".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::InsertPositionalWrongLength { fragment, expected, actual, column_names } => Diagnostic {
				code: "INSERT_004".to_string(),
				statement: None,
				message: format!(
					"Positional INSERT expects {} values (one per column), got {}",
					expected, actual
				),
				column: None,
				fragment,
				label: Some(format!("expected {} values", expected)),
				help: Some(format!(
					"Provide a value for each column in order: ({})",
					column_names.join(", ")
				)),
				notes: vec![
					format!("Table has {} columns: {}", expected, column_names.join(", ")),
					"Positional inserts must provide a value for every column".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::ColumnNotFound { fragment } => Diagnostic {
				code: "QUERY_001".to_string(),
				statement: None,
				message: "column not found".to_string(),
				fragment,
				label: Some("this column does not exist in the current context".to_string()),
				help: Some("check for typos or ensure the column is defined in the input".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			RqlError::ExtendDuplicateColumn { column_name } => Diagnostic {
				code: "EXTEND_002".to_string(),
				statement: None,
				message: format!("Cannot extend with duplicate column name '{}'", column_name),
				fragment: Fragment::None,
				label: Some("column already exists in the current frame".to_string()),
				help: Some("Use a different column name or remove the existing column first".to_string()),
				column: None,
				notes: vec![
					"EXTEND operation cannot add columns that already exist in the frame".to_string(),
					"Each column name must be unique within the result frame".to_string(),
					"Consider using MAP if you want to replace existing columns".to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::UnsupportedSourceQualification { fragment, name } => Diagnostic {
				code: "QUERY_002".to_string(),
				statement: None,
				message: format!("Source qualification '{}' is not supported in RQL expressions", name),
				fragment,
				label: Some(
					"source qualification is only allowed for join aliases in ON clauses".to_string(),
				),
				help: Some(
					"Remove the qualification or use it only with a join alias in the ON clause".to_string(),
				),
				column: None,
				notes: vec![
					"RQL uses a dataframe-centered approach where each operation produces a new dataframe"
						.to_string(),
					"Source qualifications are not needed as columns are unambiguous in the dataframe".to_string(),
					"Only join aliases can be used for qualification within ON clauses to disambiguate columns"
						.to_string(),
				],
				cause: None,
				operator_chain: None,
			},

			RqlError::JoinColumnAliasError { fragment, message } => Diagnostic {
				code: "QUERY_003".to_string(),
				statement: None,
				message: format!("Join column alias error: {}", message),
				fragment,
				label: Some("invalid column qualification in using clause".to_string()),
				help: Some(
					"In each pair, exactly one expression should reference the join alias".to_string(),
				),
				column: None,
				notes: vec![
					"Example: using (id, orders.user_id) where 'orders' is the join alias".to_string(),
					"Unqualified columns refer to the current dataframe".to_string(),
				],
				cause: None,
				operator_chain: None,
			},
			RqlError::BreakOutsideLoop => Diagnostic {
				code: "RUNTIME_004".to_string(),
				statement: None,
				message: "BREAK can only be used inside a loop".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Use BREAK inside a LOOP, WHILE, or FOR block".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			RqlError::ContinueOutsideLoop => Diagnostic {
				code: "RUNTIME_005".to_string(),
				statement: None,
				message: "CONTINUE can only be used inside a loop".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Use CONTINUE inside a LOOP, WHILE, or FOR block".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			RqlError::InternalFunctionError { name, fragment, details } => Diagnostic {
				code: "FUNCTION_008".to_string(),
				statement: None,
				message: format!("Internal error in function {}: {}", name, details),
				column: None,
				fragment,
				label: Some("internal error".to_string()),
				help: Some("This is an internal error - please report this issue".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}

impl From<RqlError> for Error {
	fn from(err: RqlError) -> Self {
		Error(err.into_diagnostic())
	}
}

/// Errors related to identifier resolution
#[derive(Debug, Clone)]
pub enum IdentifierError {
	SourceNotFound(PrimitiveNotFoundError),
	ColumnNotFound {
		column: String,
	},
	AmbiguousColumn(AmbiguousColumnError),
	UnknownAlias(UnknownAliasError),
	FunctionNotFound(FunctionNotFoundError),
	SequenceNotFound {
		namespace: String,
		name: String,
	},
	IndexNotFound {
		namespace: String,
		table: String,
		name: String,
	},
}

impl fmt::Display for IdentifierError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			IdentifierError::SourceNotFound(e) => {
				write!(f, "{}", e)
			}
			IdentifierError::ColumnNotFound {
				column,
			} => {
				write!(f, "Column '{}' not found", column)
			}
			IdentifierError::AmbiguousColumn(e) => {
				write!(f, "{}", e)
			}
			IdentifierError::UnknownAlias(e) => write!(f, "{}", e),
			IdentifierError::FunctionNotFound(e) => {
				write!(f, "{}", e)
			}
			IdentifierError::SequenceNotFound {
				namespace,
				name,
			} => {
				write!(f, "Sequence '{}::{}' not found", namespace, name)
			}
			IdentifierError::IndexNotFound {
				namespace,
				table,
				name,
			} => {
				write!(f, "Index '{}' on table '{}::{}' not found", name, namespace, table)
			}
		}
	}
}

impl error::Error for IdentifierError {}

impl From<IdentifierError> for Error {
	fn from(err: IdentifierError) -> Self {
		match err {
			IdentifierError::SourceNotFound(ref e) => CatalogError::NotFound {
				kind: CatalogObjectKind::Table,
				namespace: e.namespace.clone(),
				name: e.name.clone(),
				fragment: e.fragment.clone(),
			}
			.into(),
			_ => {
				internal_error!("{}", err)
			}
		}
	}
}

/// Namespace not found error
#[derive(Debug, Clone)]
pub struct SchemaNotFoundError {
	pub namespace: String,
}

impl fmt::Display for SchemaNotFoundError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Namespace '{}' does not exist", self.namespace)
	}
}

/// Source (table/view) not found error
#[derive(Debug, Clone)]
pub struct PrimitiveNotFoundError {
	pub namespace: String,
	pub name: String,
	pub fragment: Fragment,
}

impl fmt::Display for PrimitiveNotFoundError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		if self.namespace == "public" || self.namespace.is_empty() {
			write!(f, "Table or view '{}' does not exist", self.name)
		} else {
			write!(f, "Table or view '{}::{}' does not exist", self.namespace, self.name)
		}
	}
}

impl PrimitiveNotFoundError {
	/// Create error with additional context about what type was expected
	pub fn with_expected_type(namespace: String, name: String, _expected: &str, fragment: Fragment) -> Self {
		// Could extend this to include expected type in the error
		Self {
			namespace,
			name,
			fragment,
		}
	}
}

/// Ambiguous column reference error
#[derive(Debug, Clone)]
pub struct AmbiguousColumnError {
	pub column: String,
	pub sources: Vec<String>,
}

impl fmt::Display for AmbiguousColumnError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Column '{}' is ambiguous, found in sources: {}", self.column, self.sources.join(", "))
	}
}

/// Unknown alias error
#[derive(Debug, Clone)]
pub struct UnknownAliasError {
	pub alias: String,
}

impl fmt::Display for UnknownAliasError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Alias '{}' is not defined in the current scope", self.alias)
	}
}

/// Function not found error
#[derive(Debug, Clone)]
pub struct FunctionNotFoundError {
	pub namespaces: Vec<String>,
	pub name: String,
}

impl fmt::Display for FunctionNotFoundError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		if self.namespaces.is_empty() {
			write!(f, "Function '{}' does not exist", self.name)
		} else {
			let qualified = format!("{}::{}", self.namespaces.join("::"), self.name);
			write!(f, "Function '{}' does not exist", qualified)
		}
	}
}

/// Check if a fragment represents an injected default namespace
pub fn is_default_namespace(fragment: &Fragment) -> bool {
	matches!(fragment, Fragment::Internal { .. })
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_schema_not_found_display() {
		let err = SchemaNotFoundError {
			namespace: "myschema".to_string(),
		};
		assert_eq!(err.to_string(), "Namespace 'myschema' does not exist");
	}

	#[test]
	fn test_primitive_not_found_display() {
		let err = PrimitiveNotFoundError {
			namespace: "public".to_string(),
			name: "users".to_string(),
			fragment: Fragment::None,
		};
		assert_eq!(err.to_string(), "Table or view 'users' does not exist");

		let err = PrimitiveNotFoundError {
			namespace: "myschema".to_string(),
			name: "users".to_string(),
			fragment: Fragment::None,
		};
		assert_eq!(err.to_string(), "Table or view 'myschema::users' does not exist");
	}

	#[test]
	fn test_ambiguous_column_display() {
		let err = AmbiguousColumnError {
			column: "id".to_string(),
			sources: vec!["users".to_string(), "profiles".to_string()],
		};
		assert_eq!(err.to_string(), "Column 'id' is ambiguous, found in sources: users, profiles");
	}

	#[test]
	fn test_unknown_alias_display() {
		let err = UnknownAliasError {
			alias: "u".to_string(),
		};
		assert_eq!(err.to_string(), "Alias 'u' is not defined in the current scope");
	}

	#[test]
	fn test_function_not_found_display() {
		let err = FunctionNotFoundError {
			namespaces: vec![],
			name: "my_func".to_string(),
		};
		assert_eq!(err.to_string(), "Function 'my_func' does not exist");

		let err = FunctionNotFoundError {
			namespaces: vec!["pg_catalog".to_string(), "string".to_string()],
			name: "substr".to_string(),
		};
		assert_eq!(err.to_string(), "Function 'pg_catalog::string::substr' does not exist");
	}
}
