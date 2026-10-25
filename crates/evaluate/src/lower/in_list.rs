// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::from_ref;

use datafusion_expr::{BinaryExpr, Expr, Operator, expr::InList};
use reifydb_core::{
	expression::{Expression, InExpression, ParameterExpression},
	value::{
		batch::{is_scalar, scalar_value},
		column::factory,
	},
};
use reifydb_value::{error::BinaryOp, value::Value};

use crate::{
	Result,
	expression::{compare::field_compare_target, context::EvalContext},
	lower::{
		Lowered, Node, bool_field, bool_operand, compare, inner_type, is_untyped_none, literal,
		literal::none_bool, lower_node,
	},
	stack::Variable,
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
) -> Lowered<Node> {
	if let Some(values) = list_values(ctx, &e.list)? {
		return lower_in_values(ctx, operator, e, values);
	}
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

fn list_values(ctx: &EvalContext, list: &Expression) -> Result<Option<Vec<Value>>> {
	let name = match list {
		Expression::Variable(variable) => variable.name(),
		Expression::Parameter(ParameterExpression::Named {
			fragment,
		}) => fragment.text().strip_prefix('$').unwrap_or(fragment.text()),
		_ => return Ok(None),
	};
	let value = match ctx.symbols.get(name) {
		Some(Variable::Columns {
			batch,
		}) if is_scalar(batch) => scalar_value(batch)?,
		Some(_) => return Ok(None),
		None => match ctx.params.get_named(name) {
			Some(value) => value.clone(),
			None => return Ok(None),
		},
	};
	Ok(match value {
		Value::List(items) => Some(items),
		Value::Any(inner) => match *inner {
			Value::List(items) => Some(items),
			_ => None,
		},
		_ => None,
	})
}

fn lower_in_values(ctx: &EvalContext, operator: &'static str, e: &InExpression, values: Vec<Value>) -> Lowered<Node> {
	let name = e.fragment.text();
	let value = lower_node(ctx, operator, &e.value)?;
	if values.is_empty() {
		let (field, array) = factory::bool(name, [e.negated]);
		return Ok(literal::row_zero(operator, &field, &array)?);
	}
	if is_untyped_none(&value.field)? {
		return Ok(Node {
			expr: none_bool(),
			field: bool_field(name, true),
		});
	}
	let value_type = inner_type(&value.field)?;
	let mut nullable = value.field.is_nullable();
	let mut has_none = false;
	let mut target = None;
	let mut nodes = Vec::with_capacity(values.len());
	for item in values {
		let (field, array) = factory::from_one(name, item);
		let node = literal::row_zero(operator, &field, &array)?;
		if is_untyped_none(&node.field)? {
			has_none = true;
			continue;
		}
		let item_type = inner_type(&node.field)?;
		let item_target = field_compare_target((&value.field, &value_type), (&node.field, &item_type), true)?
			.ok_or_else(|| {
			compare::not_applicable(
				BinaryOp::Equal,
				value_type.clone(),
				item_type.clone(),
				e.fragment.clone(),
			)
		})?;
		if target.get_or_insert_with(|| item_target.clone()) != &item_target {
			return Err(compare::not_applicable(
				BinaryOp::Equal,
				value_type,
				item_type,
				e.fragment.clone(),
			)
			.into());
		}
		nullable |= node.field.is_nullable();
		nodes.push((node, item_type));
	}
	let mut expr = match target {
		Some(target) => {
			let list = nodes
				.into_iter()
				.map(|(node, item_type)| compare::cast_side(node, &item_type, &target, &e.fragment))
				.collect();
			let value = compare::cast_side(value, &value_type, &target, &e.fragment);
			Expr::InList(InList::new(Box::new(value), list, false))
		}
		None => none_bool(),
	};
	if has_none {
		nullable = true;
		expr = Expr::BinaryExpr(BinaryExpr::new(Box::new(expr), Operator::Or, Box::new(none_bool())));
	}
	if e.negated {
		expr = Expr::Not(Box::new(expr));
	}
	Ok(Node {
		expr,
		field: bool_field(name, nullable),
	})
}
