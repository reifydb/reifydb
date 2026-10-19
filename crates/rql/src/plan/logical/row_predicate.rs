// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::expression::{
	BetweenExpression, ColumnExpression, ConstantExpression, EqExpression, Expression, InExpression,
	ParameterExpression, TupleExpression, VariableExpression,
};
use reifydb_value::value::{number::parse::parse_primitive_uint, row_number::ROW_NUMBER_COLUMN_NAME};

use crate::nodes::{RowKey, RowKeyPredicate};

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
	if in_expr.negated || !is_rownum_column(&in_expr.value) {
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
		}) => parse_primitive_uint::<u64>(fragment.clone()).ok(),
		_ => None,
	}
}

pub fn extract_row_key_predicate(expr: &Expression) -> Option<RowKeyPredicate> {
	let predicate = match expr {
		Expression::Equal(eq) => key_from_equal(eq).map(RowKeyPredicate::Point),
		Expression::In(in_expr) => keys_from_in(in_expr),
		Expression::Between(between) => keys_from_between(between),
		_ => None,
	}?;
	has_parameter(&predicate).then_some(predicate)
}

fn key_from_equal(eq: &EqExpression) -> Option<RowKey> {
	if is_rownum_column(&eq.left) {
		return extract_row_key(&eq.right);
	}
	if is_rownum_column(&eq.right) {
		return extract_row_key(&eq.left);
	}
	None
}

fn keys_from_in(in_expr: &InExpression) -> Option<RowKeyPredicate> {
	if in_expr.negated || !is_rownum_column(&in_expr.value) {
		return None;
	}
	let expressions = match in_expr.list.as_ref() {
		Expression::Tuple(tuple) => &tuple.expressions,
		Expression::List(list) => &list.expressions,
		_ => return None,
	};
	if expressions.is_empty() {
		return None;
	}
	let keys = expressions.iter().map(extract_row_key).collect::<Option<Vec<_>>>()?;
	Some(RowKeyPredicate::List(keys))
}

fn keys_from_between(between: &BetweenExpression) -> Option<RowKeyPredicate> {
	if !is_rownum_column(&between.value) {
		return None;
	}
	Some(RowKeyPredicate::Range {
		start: extract_row_key(&between.lower)?,
		end: extract_row_key(&between.upper)?,
	})
}

fn extract_row_key(expr: &Expression) -> Option<RowKey> {
	match expr {
		Expression::Variable(variable) => Some(RowKey::Variable(variable.clone())),
		Expression::Parameter(
			ParameterExpression::Positional {
				fragment,
			}
			| ParameterExpression::Named {
				fragment,
			},
		) => Some(RowKey::Variable(VariableExpression {
			fragment: fragment.clone(),
		})),
		_ => extract_constant_u64(expr).map(RowKey::Constant),
	}
}

fn has_parameter(predicate: &RowKeyPredicate) -> bool {
	match predicate {
		RowKeyPredicate::Point(key) => matches!(key, RowKey::Variable(_)),
		RowKeyPredicate::List(keys) => keys.iter().any(|key| matches!(key, RowKey::Variable(_))),
		RowKeyPredicate::Range {
			start,
			end,
		} => matches!(start, RowKey::Variable(_)) || matches!(end, RowKey::Variable(_)),
	}
}

#[cfg(test)]
pub mod tests {
	use std::sync::Arc;

	use reifydb_core::{
		expression::ListExpression,
		interface::identifier::{ColumnIdentifier, ColumnObject},
	};
	use reifydb_value::fragment::Fragment;

	use super::*;

	fn make_rownum_column() -> Expression {
		let column = ColumnIdentifier {
			object: ColumnObject::Qualified {
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
			object: ColumnObject::Qualified {
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
		// An inverted range has no rows, so it must decline rather than produce an empty-but-valid predicate.
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

	fn make_variable(text: &str) -> Expression {
		Expression::Variable(make_key(text))
	}

	fn make_key(text: &str) -> VariableExpression {
		VariableExpression {
			fragment: Fragment::internal(text),
		}
	}

	fn make_in(list: Vec<Expression>, negated: bool) -> Expression {
		Expression::In(InExpression {
			value: Box::new(make_rownum_column()),
			list: Box::new(Expression::List(ListExpression {
				expressions: list,
				fragment: Fragment::internal("[]"),
			})),
			negated,
			fragment: Fragment::internal("in"),
		})
	}

	#[test]
	fn a_negated_in_is_never_a_row_lookup() {
		// A lookup of the listed rows would return exactly the rows `not in` must exclude.
		let constants = make_in(vec![make_constant(1), make_constant(2)], true);
		let parameters = make_in(vec![make_variable("$1"), make_constant(2)], true);

		assert_eq!(extract_row_predicate(&constants), None);
		assert_eq!(extract_row_key_predicate(&parameters), None);
	}

	#[test]
	fn a_parameter_key_becomes_a_row_key_predicate() {
		// The plan is cached by text, so a variable or parameter key must stay unresolved in the predicate.
		let eq = Expression::Equal(EqExpression {
			left: Box::new(make_variable("$1")),
			right: Box::new(make_rownum_column()),
			fragment: Fragment::internal("=="),
		});
		let list = make_in(vec![make_variable("$1"), make_constant(5)], false);
		let range = Expression::Between(BetweenExpression {
			value: Box::new(make_rownum_column()),
			lower: Box::new(make_constant(100)),
			upper: Box::new(Expression::Parameter(ParameterExpression::Positional {
				fragment: Fragment::internal("$2"),
			})),
			fragment: Fragment::internal("between"),
		});

		assert_eq!(
			extract_row_key_predicate(&eq),
			Some(RowKeyPredicate::Point(RowKey::Variable(make_key("$1"))))
		);
		assert_eq!(
			extract_row_key_predicate(&list),
			Some(RowKeyPredicate::List(vec![RowKey::Variable(make_key("$1")), RowKey::Constant(5)]))
		);
		assert_eq!(
			extract_row_key_predicate(&range),
			Some(RowKeyPredicate::Range {
				start: RowKey::Constant(100),
				end: RowKey::Variable(make_key("$2")),
			})
		);
	}

	#[test]
	fn constant_keys_are_not_a_row_key_predicate() {
		// Constant-only filters must keep the constant lookup nodes or today's scan, never the parameter node.
		let list = make_in(vec![make_constant(1), make_constant(2)], false);
		let inverted = Expression::Between(BetweenExpression {
			value: Box::new(make_rownum_column()),
			lower: Box::new(make_constant(100)),
			upper: Box::new(make_constant(10)),
			fragment: Fragment::internal("between"),
		});

		assert_eq!(extract_row_key_predicate(&list), None);
		assert_eq!(extract_row_key_predicate(&inverted), None);
	}
}
