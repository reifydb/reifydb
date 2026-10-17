// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use datafusion_common::{
	Result,
	tree_node::{Transformed, TreeNode},
};
use reifydb_core::{
	expression::{
		AccessObjectExpression, ColumnExpression, ConstantExpression, EqExpression, Expression,
		GreaterThanExpression, IfExpression, PrefixExpression, PrefixOperator, SubExpression,
		name::column_refs,
	},
	interface::identifier::{ColumnIdentifier, ColumnObject},
};
use reifydb_value::fragment::Fragment;

fn frag(text: &str) -> Fragment {
	Fragment::Internal {
		text: Arc::from(text),
	}
}

fn table() -> ColumnObject {
	ColumnObject::Qualified {
		namespace: frag("ns"),
		name: frag("t"),
	}
}

fn col(name: &str) -> Expression {
	Expression::Column(ColumnExpression(ColumnIdentifier {
		object: table(),
		name: frag(name),
	}))
}

fn num(val: &str) -> Expression {
	Expression::Constant(ConstantExpression::Number {
		fragment: frag(val),
	})
}

fn sub(l: Expression, r: Expression) -> Expression {
	Expression::Sub(SubExpression {
		left: Box::new(l),
		right: Box::new(r),
		fragment: frag("-"),
	})
}

fn gt(l: Expression, r: Expression) -> Expression {
	Expression::GreaterThan(GreaterThanExpression {
		left: Box::new(l),
		right: Box::new(r),
		fragment: frag(">"),
	})
}

fn neg(e: Expression) -> Expression {
	Expression::Prefix(PrefixExpression {
		operator: PrefixOperator::Minus(frag("-")),
		expression: Box::new(e),
		fragment: frag("-"),
	})
}

fn if_else(condition: Expression, then_expr: Expression, else_expr: Expression) -> Expression {
	Expression::If(IfExpression {
		condition: Box::new(condition),
		then_expr: Box::new(then_expr),
		else_ifs: vec![],
		else_expr: Some(Box::new(else_expr)),
		fragment: frag("if"),
	})
}

fn rename(from: &'static str, to: &'static str) -> impl FnMut(Expression) -> Result<Transformed<Expression>> {
	move |e| match &e {
		Expression::Column(c) if c.0.name.text() == from => Ok(Transformed::yes(col(to))),
		_ => Ok(Transformed::no(e)),
	}
}

#[test]
fn transform_up_rewrites_a_column_in_every_branch() {
	// A rewrite that skips one child kind would leave a stale column behind, so every branch must be reached.
	let before = if_else(gt(sub(col("a"), num("1")), num("0")), col("a"), neg(col("a")));
	let after = before.transform_up(rename("a", "b")).unwrap();
	assert!(after.transformed);
	assert_eq!(after.data, if_else(gt(sub(col("b"), num("1")), num("0")), col("b"), neg(col("b"))));
}

#[test]
fn transform_up_keeps_operand_order() {
	// Swapping operands of a non-commutative operator would silently change answers.
	let after = sub(col("a"), col("c")).transform_up(rename("a", "b")).unwrap();
	assert_eq!(after.data, sub(col("b"), col("c")));
}

#[test]
fn column_refs_keeps_the_qualifier() {
	// p.mint and mint name different columns after a join, so the qualifier must survive.
	let qualified = Expression::AccessSource(AccessObjectExpression {
		column: ColumnIdentifier {
			object: ColumnObject::Alias(frag("p")),
			name: frag("mint"),
		},
	});
	let expr = Expression::Equal(EqExpression {
		left: Box::new(qualified),
		right: Box::new(col("mint")),
		fragment: frag("=="),
	});
	let refs = column_refs(&expr);
	assert_eq!(refs.len(), 2);
	assert_eq!(refs[0].object, ColumnObject::Alias(frag("p")));
	assert_eq!(refs[1].object, table());
	assert!(refs.iter().all(|r| r.name.text() == "mint"));
}

#[test]
fn transform_down_stops_at_the_first_rewrite() {
	// A rule that asks to stop must leave every later node untouched.
	let after = sub(col("a"), col("a"))
		.transform_down(|e| match &e {
			Expression::Column(_) => Ok(Transformed::complete(col("b"))),
			_ => Ok(Transformed::no(e)),
		})
		.unwrap();
	assert_eq!(after.data, sub(col("b"), col("a")));
}
