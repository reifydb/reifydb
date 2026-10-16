// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::from_ref;

use datafusion_expr::{BinaryExpr, Expr, Operator};
use reifydb_core::{
	expression::{Expression, InExpression},
	value::column::factory,
};
use reifydb_value::error::BinaryOp;

use crate::{
	expression::context::EvalContext,
	lower::{Lowered, Node, bool_field, bool_operand, compare, literal, lower_node},
};

pub(super) fn items(list: &Expression) -> &[Expression] {
	match list {
		Expression::Tuple(tuple) => &tuple.expressions,
		Expression::List(list) => &list.expressions,
		other => from_ref(other),
	}
}

pub(super) fn lower_in<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
	e: &'e InExpression,
) -> Lowered<'e, Node> {
	let name = e.fragment.text();
	let items = items(&e.list);
	if items.is_empty() {
		lower_node(ctx, operator, &e.value)?;
		let (field, array) = factory::bool(name, [e.negated]);
		return Ok(literal::row_zero(operator, &field, &array)?);
	}
	let mut nullable = false;
	let mut folded: Option<Expr> = None;
	for item in items {
		let equal = compare::lower_compare(
			ctx,
			operator,
			expression,
			(&e.value, item),
			e.fragment.clone(),
			(BinaryOp::Equal, Operator::Eq),
		)?;
		nullable |= equal.field.is_nullable();
		let equal = bool_operand(equal)?;
		folded = Some(match folded {
			None => equal,
			Some(left) => Expr::BinaryExpr(BinaryExpr::new(Box::new(left), Operator::Or, Box::new(equal))),
		});
	}
	let mut expr = folded.expect("a non-empty list folds to one expression");
	if e.negated {
		expr = Expr::Not(Box::new(expr));
	}
	Ok(Node {
		expr,
		field: bool_field(name, nullable),
	})
}
