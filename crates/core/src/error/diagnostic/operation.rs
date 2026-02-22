// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{error::Diagnostic, fragment::Fragment};

pub fn take_negative_value(fragment: Fragment) -> Diagnostic {
	let value = fragment.text();
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

/// Missing aggregate map block error
pub fn missing_aggregate_map_block(fragment: Fragment) -> Diagnostic {
	Diagnostic {
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
	}
}

/// Multiple aggregate map expressions without braces error
pub fn aggregate_multiple_map_without_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_002".to_string(),
		statement: None,
		message: "Multiple aggregation expressions require curly braces".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces around expressions".to_string()),
		help: Some("Wrap multiple aggregation expressions in curly braces, e.g., 'AGGREGATE { count(id), sum(amount), avg(price) } BY category'".to_string()),
		notes: vec![
			"When specifying multiple aggregation functions, use curly braces: AGGREGATE { expr1, expr2, ... } BY ...".to_string(),
			"Single aggregation expressions can be written without braces: AGGREGATE count(id) BY category".to_string(),
			"Curly braces make the query more readable and unambiguous".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// Multiple aggregate by expressions without braces error
pub fn aggregate_multiple_by_without_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_003".to_string(),
		statement: None,
		message: "Multiple grouping columns require curly braces".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces around columns".to_string()),
		help: Some("Wrap multiple grouping columns in curly braces, e.g., 'AGGREGATE count(id) BY { category, region, year }'".to_string()),
		notes: vec![
			"When grouping by multiple columns, use curly braces: AGGREGATE ... BY { col1, col2, ... }".to_string(),
			"Single grouping columns can be written without braces: AGGREGATE ... BY category".to_string(),
			"For global aggregations without grouping, use empty braces: AGGREGATE ... BY {}".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// Multiple DISTINCT columns without braces error
pub fn distinct_multiple_columns_without_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "DISTINCT_001".to_string(),
		statement: None,
		message: "Multiple DISTINCT columns require curly braces".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces around columns".to_string()),
		help: Some("Wrap multiple DISTINCT columns in curly braces, e.g., 'DISTINCT { category, value } FROM events'".to_string()),
		notes: vec![
			"When using DISTINCT with multiple columns, use curly braces: DISTINCT { col1, col2, ... }".to_string(),
			"Single columns can be written without braces: DISTINCT category FROM events".to_string(),
			"No arguments means distinct on all columns: DISTINCT FROM events".to_string(),
			"Curly braces make the query more readable and unambiguous".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// Multiple MAP expressions without braces error
pub fn map_multiple_expressions_without_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "MAP_001".to_string(),
		statement: None,
		message: "Multiple MAP expressions require curly braces".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces around expressions".to_string()),
		help: Some(
			"Wrap multiple MAP expressions in curly braces, e.g., 'MAP { name, age, email } FROM users'"
				.to_string(),
		),
		notes: vec![
			"When mapping multiple columns or expressions, use curly braces: MAP { expr1, expr2, ... }"
				.to_string(),
			"Single expressions can be written without braces: MAP name FROM users".to_string(),
			"Curly braces make the query more readable and unambiguous".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// Multiple EXTEND expressions without braces error
pub fn extend_multiple_expressions_without_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "EXTEND_001".to_string(),
		statement: None,
		message: "Multiple EXTEND expressions require curly braces".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces around expressions".to_string()),
		help: Some("Wrap multiple EXTEND expressions in curly braces, e.g., 'EXTEND { total: price * quantity, tax: price * 0.1 }'".to_string()),
		notes: vec![
			"When extending with multiple columns or expressions, use curly braces: EXTEND { expr1, expr2, ... }".to_string(),
			"Single expressions can be written without braces: EXTEND total: price * quantity".to_string(),
			"Curly braces make the query more readable and unambiguous".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// Multiple APPLY arguments without braces error
pub fn apply_multiple_arguments_without_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "APPLY_001".to_string(),
		statement: None,
		message: "Multiple APPLY arguments require curly braces".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces around arguments".to_string()),
		help: Some("Wrap multiple APPLY arguments in curly braces, e.g., 'APPLY operator { arg1, arg2 }'"
			.to_string()),
		notes: vec![
			"When applying operators with multiple arguments, use curly braces: APPLY operator { arg1, arg2, ... }".to_string(),
			"Single arguments can be written without braces: APPLY running_sum amount".to_string(),
			"No arguments should use empty braces: APPLY counter {}".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// Missing slide parameter for sliding window error
pub fn window_missing_slide_parameter(fragment: Fragment) -> Diagnostic {
	Diagnostic {
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
	}
}

/// Slide interval too large for window error
pub fn window_slide_too_large(fragment: Fragment, slide_value: String, window_value: String) -> Diagnostic {
	Diagnostic {
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
			"Reduce the slide value to be smaller than the window size for overlapping windows".to_string()
		),
		notes: vec![
			"Sliding windows create overlapping segments when slide < window size".to_string(),
			"If slide >= window size, consider using tumbling windows instead".to_string(),
			"Example: For 10-minute windows, use slide values like \"2m\", \"5m\", or \"1m\"".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// Incompatible slide type with window type error
pub fn window_incompatible_slide_type(fragment: Fragment, window_type: String, slide_type: String) -> Diagnostic {
	Diagnostic {
		code: "WINDOW_004".to_string(),
		statement: None,
		message: format!("Incompatible slide type {} with window type {}", slide_type, window_type),
		column: None,
		fragment,
		label: Some("mismatched types".to_string()),
		help: Some(
			"Use duration-based slide for time windows, or count-based slide for count windows".to_string()
		),
		notes: vec![
			"Time-based windows (interval) require duration-based slide parameters (e.g., \"1m\", \"30s\")"
				.to_string(),
			"Count-based windows (count) require numeric slide parameters (e.g., 10, 50)".to_string(),
			"Example time window: WINDOW WITH { interval: \"5m\", slide: \"1m\" }".to_string(),
			"Example count window: WINDOW WITH { count: 100, slide: 20 }".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// Tumbling window with slide parameter error
pub fn window_tumbling_with_slide(fragment: Fragment) -> Diagnostic {
	Diagnostic {
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
	}
}

/// Incompatible window type and size combination error
pub fn window_incompatible_type_size(fragment: Fragment, window_type: String, size_type: String) -> Diagnostic {
	Diagnostic {
		code: "WINDOW_006".to_string(),
		statement: None,
		message: format!("Incompatible window type {} and size type {} for window", window_type, size_type),
		column: None,
		fragment,
		label: Some("mismatched window configuration".to_string()),
		help: Some("Use 'interval' with time-based windows or 'count' with count-based windows".to_string()),
		notes: vec![
			"Time-based windows use 'interval' parameter with duration values (e.g., \"5m\", \"1h\")"
				.to_string(),
			"Count-based windows use 'count' parameter with numeric values (e.g., 100, 500)".to_string(),
			"Example time window: WINDOW WITH { interval: \"10m\" }".to_string(),
			"Example count window: WINDOW WITH { count: 1000 }".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// Missing window type or size error
pub fn window_missing_type_or_size(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "WINDOW_007".to_string(),
		statement: None,
		message: "Window type and size must be specified for window".to_string(),
		column: None,
		fragment,
		label: Some("incomplete window configuration".to_string()),
		help: Some("Specify either 'interval' for time-based windows or 'count' for count-based windows"
			.to_string()),
		notes: vec![
			"Windows require a size specification to determine their boundaries".to_string(),
			"Use 'interval' with duration for time-based windows: WINDOW WITH { interval: \"5m\" }"
				.to_string(),
			"Use 'count' with number for count-based windows: WINDOW WITH { count: 100 }".to_string(),
			"Additional parameters like 'slide' can be added for sliding windows".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// UPDATE missing assignments block error
pub fn update_missing_assignments_block(fragment: Fragment) -> Diagnostic {
	Diagnostic {
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
	}
}

/// UPDATE empty assignments block error
pub fn update_empty_assignments_block(fragment: Fragment) -> Diagnostic {
	Diagnostic {
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
	}
}

/// UPDATE missing FILTER clause error
pub fn update_missing_filter_clause(fragment: Fragment) -> Diagnostic {
	Diagnostic {
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
	}
}

/// DELETE missing target error
pub fn delete_missing_target(fragment: Fragment) -> Diagnostic {
	Diagnostic {
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
			"Example with namespace: DELETE test.users FILTER id == 1".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// DELETE missing FILTER clause error
pub fn delete_missing_filter_clause(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "DELETE_002".to_string(),
		statement: None,
		message: "DELETE requires a FILTER clause".to_string(),
		column: None,
		fragment,
		label: Some("missing FILTER clause".to_string()),
		help: Some("Add a FILTER clause to specify which rows to delete, e.g., 'DELETE users FILTER id == 1'"
			.to_string()),
		notes: vec![
			"The FILTER clause is required to prevent accidental mass deletes".to_string(),
			"It specifies the condition for selecting rows to delete".to_string(),
			"Example: DELETE users FILTER id == 1".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// INSERT missing target error
pub fn insert_missing_target(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "INSERT_001".to_string(),
		statement: None,
		message: "INSERT requires a target table".to_string(),
		column: None,
		fragment,
		label: Some("missing target table".to_string()),
		help: Some("Specify the table to insert into, e.g., 'INSERT users [{ id: 1, name: \"Alice\" }]'"
			.to_string()),
		notes: vec![
			"The target table specifies where to insert the data".to_string(),
			"Example: INSERT users [{ id: 1, name: \"Alice\" }]".to_string(),
			"Example with namespace: INSERT test.users [{ id: 1, name: \"Alice\" }]".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// INSERT missing data source error
pub fn insert_missing_source(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "INSERT_002".to_string(),
		statement: None,
		message: "INSERT requires a data source".to_string(),
		column: None,
		fragment,
		label: Some("missing data source".to_string()),
		help: Some("Specify the data to insert, e.g., 'INSERT users [{ id: 1, name: \"Alice\" }]'".to_string()),
		notes: vec![
			"Use inline array directly: INSERT users [{ id: 1 }, { id: 2 }]".to_string(),
			"Use variable directly: INSERT users $data".to_string(),
			"Use FROM for table sources: INSERT target_table FROM source_table".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

pub fn insert_mixed_row_types(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "INSERT_003".to_string(),
		statement: None,
		message: "Cannot mix keyed {key: value} and positional (value, ...) rows in the same INSERT".to_string(),
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
	}
}

pub fn insert_positional_wrong_length(
	fragment: Fragment,
	expected: usize,
	actual: usize,
	column_names: &[String],
) -> Diagnostic {
	Diagnostic {
		code: "INSERT_004".to_string(),
		statement: None,
		message: format!("Positional INSERT expects {} values (one per column), got {}", expected, actual),
		column: None,
		fragment,
		label: Some(format!("expected {} values", expected)),
		help: Some(format!("Provide a value for each column in order: ({})", column_names.join(", "))),
		notes: vec![
			format!("Table has {} columns: {}", expected, column_names.join(", ")),
			"Positional inserts must provide a value for every column".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// SORT missing braces error
pub fn sort_missing_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "SORT_001".to_string(),
		statement: None,
		message: "SORT requires curly braces around columns".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces".to_string()),
		help: Some("Wrap SORT columns in curly braces, e.g., 'SORT {name}' or 'SORT {name:ASC, age:DESC}'"
			.to_string()),
		notes: vec![
			"SORT always requires curly braces: SORT {column}".to_string(),
			"Multiple columns: SORT {col1:ASC, col2:DESC}".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// MAP missing braces error
pub fn map_missing_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "MAP_002".to_string(),
		statement: None,
		message: "MAP requires curly braces around expressions".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces".to_string()),
		help: Some("Wrap MAP expressions in curly braces, e.g., 'MAP {name}' or 'MAP {name, age}'".to_string()),
		notes: vec![
			"MAP always requires curly braces: MAP {expression}".to_string(),
			"Multiple expressions: MAP {expr1, expr2}".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// FILTER missing braces error
pub fn filter_missing_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "FILTER_001".to_string(),
		statement: None,
		message: "FILTER requires curly braces around the condition".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces".to_string()),
		help: Some("Wrap FILTER condition in curly braces, e.g., 'FILTER {price > 100}'".to_string()),
		notes: vec![
			"FILTER always requires curly braces: FILTER {condition}".to_string(),
			"Example: FILTER {active == true AND price > 100}".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// EXTEND missing braces error
pub fn extend_missing_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "EXTEND_002".to_string(),
		statement: None,
		message: "EXTEND requires curly braces around expressions".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces".to_string()),
		help: Some("Wrap EXTEND expressions in curly braces, e.g., 'EXTEND {total: price * qty}'".to_string()),
		notes: vec![
			"EXTEND always requires curly braces: EXTEND {expression}".to_string(),
			"Multiple expressions: EXTEND {expr1, expr2}".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// PATCH missing braces error
pub fn patch_missing_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "PATCH_001".to_string(),
		statement: None,
		message: "PATCH requires curly braces around expressions".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces".to_string()),
		help: Some("Wrap PATCH expressions in curly braces, e.g., 'PATCH {status: \"active\"}'".to_string()),
		notes: vec![
			"PATCH always requires curly braces: PATCH {expression}".to_string(),
			"Multiple expressions: PATCH {expr1, expr2}".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// DISTINCT missing braces error
pub fn distinct_missing_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "DISTINCT_002".to_string(),
		statement: None,
		message: "DISTINCT requires curly braces around columns".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces".to_string()),
		help: Some(
			"Wrap DISTINCT columns in curly braces, e.g., 'DISTINCT {}' or 'DISTINCT {name}'".to_string()
		),
		notes: vec![
			"DISTINCT always requires curly braces: DISTINCT {columns}".to_string(),
			"For all columns distinct: DISTINCT {}".to_string(),
			"Multiple columns: DISTINCT {col1, col2}".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// APPLY missing braces error
pub fn apply_missing_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "APPLY_002".to_string(),
		statement: None,
		message: "APPLY requires curly braces around arguments".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces".to_string()),
		help: Some("Wrap APPLY arguments in curly braces, e.g., 'APPLY running_sum {value}'".to_string()),
		notes: vec![
			"APPLY always requires curly braces: APPLY operator {arguments}".to_string(),
			"No arguments: APPLY counter {}".to_string(),
			"Multiple arguments: APPLY op {arg1, arg2}".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// TAKE missing braces error
pub fn take_missing_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "TAKE_002".to_string(),
		statement: None,
		message: "TAKE requires curly braces around the limit".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces".to_string()),
		help: Some("Wrap TAKE limit in curly braces, e.g., 'TAKE {10}' or 'TAKE {}'".to_string()),
		notes: vec![
			"TAKE always requires curly braces: TAKE {limit}".to_string(),
			"Unlimited: TAKE {}".to_string(),
			"Limited: TAKE {100}".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// AGGREGATE missing braces error for projections
pub fn aggregate_missing_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_004".to_string(),
		statement: None,
		message: "AGGREGATE requires curly braces around aggregation expressions".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces".to_string()),
		help: Some("Wrap aggregation expressions in curly braces, e.g., 'AGGREGATE {count(id)} BY {name}'"
			.to_string()),
		notes: vec![
			"AGGREGATE always requires curly braces: AGGREGATE {expressions} BY {columns}".to_string(),
			"Example: AGGREGATE {sum(amount), avg(price)} BY {category}".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

/// AGGREGATE BY missing braces error
pub fn aggregate_by_missing_braces(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_005".to_string(),
		statement: None,
		message: "AGGREGATE BY requires curly braces around grouping columns".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces".to_string()),
		help: Some("Wrap BY columns in curly braces, e.g., 'AGGREGATE {count(id)} BY {name}'".to_string()),
		notes: vec![
			"AGGREGATE BY always requires curly braces: BY {columns}".to_string(),
			"Global aggregation (no grouping): BY {}".to_string(),
			"Multiple columns: BY {col1, col2}".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}
