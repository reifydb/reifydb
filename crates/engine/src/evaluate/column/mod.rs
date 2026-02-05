// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::Column;
use reifydb_function::registry::Functions;
use reifydb_rql::expression::Expression;
use reifydb_runtime::clock::Clock;

use crate::evaluate::ColumnEvaluationContext;

pub mod access;
pub mod alias;
pub mod arith;
pub mod call;
pub mod cast;
pub mod column;
pub mod compare;
pub(crate) mod constant;
pub mod extend_expr;
pub mod if_expr;
pub mod logic;
pub mod map_expr;
pub mod parameter;
pub mod prefix;
pub mod tuple;
pub mod variable;

#[derive(Clone)]
pub struct StandardColumnEvaluator {
	clock: Clock,
	functions: Functions,
}

impl StandardColumnEvaluator {
	pub fn new(functions: Functions, clock: Clock) -> Self {
		Self {
			clock,
			functions,
		}
	}
}

impl StandardColumnEvaluator {
	pub fn evaluate(&self, ctx: &ColumnEvaluationContext, expr: &Expression) -> crate::Result<Column> {
		match expr {
			Expression::AccessSource(expr) => self.access(ctx, expr),
			Expression::Alias(expr) => self.alias(ctx, expr),
			Expression::Add(expr) => self.add(ctx, expr),
			Expression::Div(expr) => self.div(ctx, expr),
			Expression::Call(expr) => self.call(ctx, expr),
			Expression::Cast(expr) => self.cast(ctx, expr),
			Expression::Column(expr) => self.column(ctx, expr),
			Expression::Constant(expr) => self.constant(ctx, expr),
			Expression::GreaterThan(expr) => self.greater_than(ctx, expr),
			Expression::GreaterThanEqual(expr) => self.greater_than_equal(ctx, expr),
			Expression::LessThan(expr) => self.less_than(ctx, expr),
			Expression::LessThanEqual(expr) => self.less_than_equal(ctx, expr),
			Expression::Equal(expr) => self.equal(ctx, expr),
			Expression::NotEqual(expr) => self.not_equal(ctx, expr),
			Expression::Between(expr) => self.between(ctx, expr),
			Expression::In(expr) => self.in_expr(ctx, expr),
			Expression::And(expr) => self.and(ctx, expr),
			Expression::Or(expr) => self.or(ctx, expr),
			Expression::Xor(expr) => self.xor(ctx, expr),
			Expression::Rem(expr) => self.rem(ctx, expr),
			Expression::Mul(expr) => self.mul(ctx, expr),
			Expression::Prefix(expr) => self.prefix(ctx, expr),
			Expression::Sub(expr) => self.sub(ctx, expr),
			Expression::Tuple(expr) => self.tuple(ctx, expr),
			Expression::Parameter(expr) => self.parameter(ctx, expr),
			Expression::Variable(expr) => self.variable(ctx, expr),
			Expression::If(expr) => self.if_expr(ctx, expr),
			Expression::Map(expr) => self.map_expr(ctx, expr),
			Expression::Extend(expr) => self.extend_expr(ctx, expr),
			expr => unimplemented!("{expr:?}"),
		}
	}

	pub fn evaluate_multi(&self, ctx: &ColumnEvaluationContext, expr: &Expression) -> crate::Result<Vec<Column>> {
		match expr {
			Expression::Map(map_expr) => self.map_expr_multi(ctx, map_expr),
			Expression::Extend(extend_expr) => self.extend_expr_multi(ctx, extend_expr),
			Expression::If(if_expr) => self.if_expr_multi(ctx, if_expr),
			_ => Ok(vec![self.evaluate(ctx, expr)?]),
		}
	}
}

pub fn evaluate(
	ctx: &ColumnEvaluationContext,
	expr: &Expression,
	functions: &Functions,
	clock: &Clock,
) -> crate::Result<Column> {
	let evaluator = StandardColumnEvaluator::new(functions.clone(), clock.clone());

	// Ensures that result column data type matches the expected target
	// column type
	if let Some(ty) = ctx.target.as_ref().map(|c| c.column_type()) {
		let mut column = evaluator.evaluate(ctx, expr)?;
		let data = cast::cast_column_data(ctx, &column.data(), ty, &expr.lazy_fragment())?;
		column = Column {
			name: column.name,
			data,
		};
		Ok(column)
	} else {
		evaluator.evaluate(ctx, expr)
	}
}
