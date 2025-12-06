// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::{error::diagnostic::Diagnostic, fragment::IntoFragment};

pub fn take_negative_value<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
	}
}

/// Missing aggregate map block error
pub fn missing_aggregate_map_block<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
		cause: None}
}

/// Multiple aggregate map expressions without braces error
pub fn aggregate_multiple_map_without_braces<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
		cause: None}
}

/// Multiple aggregate by expressions without braces error
pub fn aggregate_multiple_by_without_braces<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
		cause: None}
}

/// Multiple SELECT expressions without braces error
pub fn select_multiple_expressions_without_braces<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
		code: "SELECT_001".to_string(),
		statement: None,
		message: "Multiple SELECT expressions require curly braces".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces around expressions".to_string()),
		help: Some("Wrap multiple SELECT expressions in curly braces, e.g., 'SELECT { name, age, email } FROM users'".to_string()),
		notes: vec![
			"When selecting multiple columns or expressions, use curly braces: SELECT { expr1, expr2, ... }".to_string(),
			"Single expressions can be written without braces: SELECT name FROM users".to_string(),
			"Curly braces make the query more readable and unambiguous".to_string(),
		],
		cause: None}
}

/// Multiple DISTINCT columns without braces error
pub fn distinct_multiple_columns_without_braces<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
		cause: None}
}

/// Multiple MAP expressions without braces error
pub fn map_multiple_expressions_without_braces<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
	}
}

/// Multiple EXTEND expressions without braces error
pub fn extend_multiple_expressions_without_braces<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
		cause: None}
}

/// Multiple APPLY arguments without braces error
pub fn apply_multiple_arguments_without_braces<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
	}
}

/// Missing slide parameter for sliding window error
pub fn window_missing_slide_parameter<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
	}
}

/// Slide interval too large for window error
pub fn window_slide_too_large<'a>(
	fragment: impl IntoFragment<'a>,
	slide_value: String,
	window_value: String,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
	}
}

/// Incompatible slide type with window type error
pub fn window_incompatible_slide_type<'a>(
	fragment: impl IntoFragment<'a>,
	window_type: String,
	slide_type: String,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
	}
}

/// Tumbling window with slide parameter error
pub fn window_tumbling_with_slide<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
	}
}

/// Incompatible window type and size combination error
pub fn window_incompatible_type_size<'a>(
	fragment: impl IntoFragment<'a>,
	window_type: String,
	size_type: String,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
	}
}

/// Missing window type or size error
pub fn window_missing_type_or_size<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
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
	}
}
