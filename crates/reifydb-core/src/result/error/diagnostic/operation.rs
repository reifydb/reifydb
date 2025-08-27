// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	interface::fragment::IntoFragment,
	result::error::diagnostic::Diagnostic,
};

pub fn take_negative_value<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	let value = fragment.value();
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
pub fn missing_aggregate_map_block<'a>(
	fragment: impl IntoFragment<'a>,
) -> Diagnostic {
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
		cause: None,
	}
}

/// Multiple aggregate map expressions without braces error
pub fn aggregate_multiple_map_without_braces<'a>(
	fragment: impl IntoFragment<'a>,
) -> Diagnostic {
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
		cause: None,
	}
}

/// Multiple aggregate by expressions without braces error
pub fn aggregate_multiple_by_without_braces<'a>(
	fragment: impl IntoFragment<'a>,
) -> Diagnostic {
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
		cause: None,
	}
}

/// Multiple SELECT expressions without braces error
pub fn select_multiple_expressions_without_braces<'a>(
	fragment: impl IntoFragment<'a>,
) -> Diagnostic {
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
		cause: None,
	}
}

/// Multiple MAP expressions without braces error
pub fn map_multiple_expressions_without_braces<'a>(
	fragment: impl IntoFragment<'a>,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
		code: "MAP_001".to_string(),
		statement: None,
		message: "Multiple MAP expressions require curly braces".to_string(),
		column: None,
		fragment,
		label: Some("missing curly braces around expressions".to_string()),
		help: Some("Wrap multiple MAP expressions in curly braces, e.g., 'MAP { name, age, email } FROM users'".to_string()),
		notes: vec![
			"When mapping multiple columns or expressions, use curly braces: MAP { expr1, expr2, ... }".to_string(),
			"Single expressions can be written without braces: MAP name FROM users".to_string(),
			"Curly braces make the query more readable and unambiguous".to_string(),
		],
		cause: None,
	}
}

/// Multiple EXTEND expressions without braces error
pub fn extend_multiple_expressions_without_braces<'a>(
	fragment: impl IntoFragment<'a>,
) -> Diagnostic {
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
		cause: None,
	}
}
