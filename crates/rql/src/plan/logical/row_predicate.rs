// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Row predicate extraction for optimized row-number-based access.
//!
//! This module detects patterns in filter expressions that can be optimized
//! to O(1) or O(k) row lookups instead of full table scans:
//! - `rownum == N` → Point lookup
//! - `rownum in [a, b, c]` → List lookup
//! - `rownum between X and Y` → Range scan

use std::sync::Arc;

use reifydb_type::ROW_NUMBER_COLUMN_NAME;

use crate::expression::{
	BetweenExpression, ColumnExpression, ConstantExpression, EqExpression, Expression, InExpression,
	TupleExpression,
};

/// Represents a row-number-based predicate that can be used for optimized access.
#[derive(Debug, Clone, PartialEq)]
pub enum RowPredicate {
	/// Single row lookup: `rownum == N`
	Point(u64),
	/// Multiple discrete row lookups: `rownum in [a, b, c]`
	List(Vec<u64>),
	/// Range scan: `rownum between X and Y` (inclusive)
	Range {
		start: u64,
		end: u64,
	},
}

/// Attempts to extract a row predicate from a filter expression.
///
/// Returns `Some(RowPredicate)` if the expression represents a row-number-based
/// filter that can be optimized. Returns `None` if:
/// - The expression doesn't involve row numbers
/// - The row number comparison involves runtime variables
/// - The expression is too complex to optimize
pub fn extract_row_predicate(expr: &Expression) -> Option<RowPredicate> {
	match expr {
		// rownum == N
		Expression::Equal(eq) => extract_from_equal(eq),
		// rownum in [a, b, c]
		Expression::In(in_expr) => extract_from_in(in_expr),
		// rownum between X and Y
		Expression::Between(between) => extract_from_between(between),
		_ => None,
	}
}

/// Extracts a point lookup from an equality expression.
fn extract_from_equal(eq: &EqExpression) -> Option<RowPredicate> {
	// Check both orderings: rownum == N and N == rownum
	if let Some(value) = try_extract_rownum_eq(&eq.left, &eq.right) {
		return Some(RowPredicate::Point(value));
	}
	if let Some(value) = try_extract_rownum_eq(&eq.right, &eq.left) {
		return Some(RowPredicate::Point(value));
	}
	None
}

/// Tries to extract a row number value from `column == constant` pattern.
fn try_extract_rownum_eq(maybe_rownum: &Expression, maybe_value: &Expression) -> Option<u64> {
	if !is_rownum_column(maybe_rownum) {
		return None;
	}
	extract_constant_u64(maybe_value)
}

/// Extracts a list lookup from an IN expression.
fn extract_from_in(in_expr: &InExpression) -> Option<RowPredicate> {
	// Check if the value side is rownum
	if !is_rownum_column(&in_expr.value) {
		return None;
	}

	// The list should be a tuple/list of constants
	match in_expr.list.as_ref() {
		Expression::Tuple(tuple) => extract_list_from_tuple(tuple),
		_ => None,
	}
}

/// Extracts row numbers from a tuple expression.
fn extract_list_from_tuple(tuple: &TupleExpression) -> Option<RowPredicate> {
	let mut values = Vec::with_capacity(tuple.expressions.len());
	for expr in &tuple.expressions {
		match extract_constant_u64(expr) {
			Some(v) => values.push(v),
			None => return None, // Non-constant in list, can't optimize
		}
	}
	if values.is_empty() {
		return None;
	}
	Some(RowPredicate::List(values))
}

/// Extracts a range scan from a BETWEEN expression.
fn extract_from_between(between: &BetweenExpression) -> Option<RowPredicate> {
	// Check if the value is rownum
	if !is_rownum_column(&between.value) {
		return None;
	}

	// Both bounds must be constants
	let start = extract_constant_u64(&between.lower)?;
	let end = extract_constant_u64(&between.upper)?;

	// Ensure valid range
	if start > end {
		return None;
	}

	Some(RowPredicate::Range {
		start,
		end,
	})
}

/// Checks if an expression is a column reference to the row number column.
fn is_rownum_column(expr: &Expression) -> bool {
	match expr {
		Expression::Column(ColumnExpression(col_id)) => col_id.name.text() == ROW_NUMBER_COLUMN_NAME,
		Expression::AccessSource(access) => access.column.name.text() == ROW_NUMBER_COLUMN_NAME,
		_ => false,
	}
}

/// Extracts a u64 value from a constant expression.
fn extract_constant_u64(expr: &Expression) -> Option<u64> {
	match expr {
		Expression::Constant(ConstantExpression::Number {
			fragment,
		}) => {
			// Parse the number from the fragment text
			let text = fragment.text();
			text.parse::<u64>().ok()
		}
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnSource};
	use reifydb_type::Fragment;

	use super::*;

	fn make_rownum_column() -> Expression {
		let column = ColumnIdentifier {
			source: ColumnSource::Source {
				namespace: Fragment::Internal {
					text: Arc::new("_context".to_string()),
				},
				source: Fragment::Internal {
					text: Arc::new("_context".to_string()),
				},
			},
			name: Fragment::Internal {
				text: Arc::new(ROW_NUMBER_COLUMN_NAME.to_string()),
			},
		};
		Expression::Column(ColumnExpression(column))
	}

	fn make_constant(n: u64) -> Expression {
		Expression::Constant(ConstantExpression::Number {
			fragment: Fragment::Internal {
				text: Arc::from(n.to_string()),
			},
		})
	}

	#[test]
	fn test_point_lookup() {
		let eq = EqExpression {
			left: Box::new(make_rownum_column()),
			right: Box::new(make_constant(42)),
			fragment: Fragment::internal("=="),
		};
		let expr = Expression::Equal(eq);

		let predicate = extract_row_predicate(&expr);
		assert_eq!(predicate, Some(RowPredicate::Point(42)));
	}

	#[test]
	fn test_point_lookup_reversed() {
		let eq = EqExpression {
			left: Box::new(make_constant(42)),
			right: Box::new(make_rownum_column()),
			fragment: Fragment::internal("=="),
		};
		let expr = Expression::Equal(eq);

		let predicate = extract_row_predicate(&expr);
		assert_eq!(predicate, Some(RowPredicate::Point(42)));
	}

	#[test]
	fn test_list_lookup() {
		let tuple = TupleExpression {
			expressions: vec![make_constant(1), make_constant(5), make_constant(10)],
			fragment: Fragment::internal("[]"),
		};
		let in_expr = InExpression {
			value: Box::new(make_rownum_column()),
			list: Box::new(Expression::Tuple(tuple)),
			negated: false,
			fragment: Fragment::internal("in"),
		};
		let expr = Expression::In(in_expr);

		let predicate = extract_row_predicate(&expr);
		assert_eq!(predicate, Some(RowPredicate::List(vec![1, 5, 10])));
	}

	#[test]
	fn test_range_scan() {
		let between = BetweenExpression {
			value: Box::new(make_rownum_column()),
			lower: Box::new(make_constant(10)),
			upper: Box::new(make_constant(100)),
			fragment: Fragment::internal("between"),
		};
		let expr = Expression::Between(between);

		let predicate = extract_row_predicate(&expr);
		assert_eq!(
			predicate,
			Some(RowPredicate::Range {
				start: 10,
				end: 100
			})
		);
	}

	#[test]
	fn test_no_rownum_returns_none() {
		let other_column = ColumnIdentifier {
			source: ColumnSource::Source {
				namespace: Fragment::Internal {
					text: Arc::new("default".to_string()),
				},
				source: Fragment::Internal {
					text: Arc::new("users".to_string()),
				},
			},
			name: Fragment::Internal {
				text: Arc::new("id".to_string()),
			},
		};
		let eq = EqExpression {
			left: Box::new(Expression::Column(ColumnExpression(other_column))),
			right: Box::new(make_constant(42)),
			fragment: Fragment::internal("=="),
		};
		let expr = Expression::Equal(eq);

		let predicate = extract_row_predicate(&expr);
		assert_eq!(predicate, None);
	}

	#[test]
	fn test_invalid_range_returns_none() {
		// start > end should return None
		let between = BetweenExpression {
			value: Box::new(make_rownum_column()),
			lower: Box::new(make_constant(100)),
			upper: Box::new(make_constant(10)),
			fragment: Fragment::internal("between"),
		};
		let expr = Expression::Between(between);

		let predicate = extract_row_predicate(&expr);
		assert_eq!(predicate, None);
	}
}
