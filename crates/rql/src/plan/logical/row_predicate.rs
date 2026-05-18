// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::row_number::ROW_NUMBER_COLUMN_NAME;

use crate::expression::{
	BetweenExpression, ColumnExpression, ConstantExpression, EqExpression, Expression, InExpression,
	TupleExpression,
};

#[derive(Debug, Clone, PartialEq)]
pub enum RowPredicate {
	Point(u64),

	List(Vec<u64>),

	Range {
		start: u64,
		end: u64,
	},
}

pub fn extract_row_predicate(expr: &Expression) -> Option<RowPredicate> {
	match expr {
		Expression::Equal(eq) => extract_from_equal(eq),

		Expression::In(in_expr) => extract_from_in(in_expr),

		Expression::Between(between) => extract_from_between(between),
		_ => None,
	}
}

fn extract_from_equal(eq: &EqExpression) -> Option<RowPredicate> {
	if let Some(value) = try_extract_rownum_eq(&eq.left, &eq.right) {
		return Some(RowPredicate::Point(value));
	}
	if let Some(value) = try_extract_rownum_eq(&eq.right, &eq.left) {
		return Some(RowPredicate::Point(value));
	}
	None
}

fn try_extract_rownum_eq(maybe_rownum: &Expression, maybe_value: &Expression) -> Option<u64> {
	if !is_rownum_column(maybe_rownum) {
		return None;
	}
	extract_constant_u64(maybe_value)
}

fn extract_from_in(in_expr: &InExpression) -> Option<RowPredicate> {
	if !is_rownum_column(&in_expr.value) {
		return None;
	}

	match in_expr.list.as_ref() {
		Expression::Tuple(tuple) => extract_list_from_tuple(tuple),
		Expression::List(list) => extract_list_from_expressions(&list.expressions),
		_ => None,
	}
}

fn extract_list_from_tuple(tuple: &TupleExpression) -> Option<RowPredicate> {
	extract_list_from_expressions(&tuple.expressions)
}

fn extract_list_from_expressions(expressions: &[Expression]) -> Option<RowPredicate> {
	let mut values = Vec::with_capacity(expressions.len());
	for expr in expressions {
		match extract_constant_u64(expr) {
			Some(v) => values.push(v),
			None => return None,
		}
	}
	if values.is_empty() {
		return None;
	}
	Some(RowPredicate::List(values))
}

fn extract_from_between(between: &BetweenExpression) -> Option<RowPredicate> {
	if !is_rownum_column(&between.value) {
		return None;
	}

	let start = extract_constant_u64(&between.lower)?;
	let end = extract_constant_u64(&between.upper)?;

	if start > end {
		return None;
	}

	Some(RowPredicate::Range {
		start,
		end,
	})
}

fn is_rownum_column(expr: &Expression) -> bool {
	match expr {
		Expression::Column(ColumnExpression(col_id)) => col_id.name.text() == ROW_NUMBER_COLUMN_NAME,
		Expression::AccessSource(access) => access.column.name.text() == ROW_NUMBER_COLUMN_NAME,
		_ => false,
	}
}

fn extract_constant_u64(expr: &Expression) -> Option<u64> {
	match expr {
		Expression::Constant(ConstantExpression::Number {
			fragment,
		}) => {
			let text = fragment.text();
			text.parse::<u64>().ok()
		}
		_ => None,
	}
}

#[cfg(test)]
pub mod tests {
	use std::sync::Arc;

	use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnShape};
	use reifydb_type::fragment::Fragment;

	use super::*;
	use crate::expression::ListExpression;

	fn make_rownum_column() -> Expression {
		let column = ColumnIdentifier {
			shape: ColumnShape::Qualified {
				namespace: Fragment::Internal {
					text: Arc::from("_context"),
				},
				name: Fragment::Internal {
					text: Arc::from("_context"),
				},
			},
			name: Fragment::Internal {
				text: Arc::from(ROW_NUMBER_COLUMN_NAME),
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
		let list = ListExpression {
			expressions: vec![make_constant(1), make_constant(5), make_constant(10)],
			fragment: Fragment::internal("[]"),
		};
		let in_expr = InExpression {
			value: Box::new(make_rownum_column()),
			list: Box::new(Expression::List(list)),
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
			shape: ColumnShape::Qualified {
				namespace: Fragment::Internal {
					text: Arc::from("default"),
				},
				name: Fragment::Internal {
					text: Arc::from("users"),
				},
			},
			name: Fragment::Internal {
				text: Arc::from("id"),
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
