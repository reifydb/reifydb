// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file
use reifydb_type::Fragment;

use crate::expression::{
	AddExpression, CastExpression, ConstantExpression, DivExpression, Expression, MulExpression,
	ParameterExpression, RemExpression, SubExpression,
};

impl<'a> Expression<'a> {
	pub fn lazy_fragment(&self) -> impl Fn() -> Fragment<'a> + '_ {
		move || match self {
			Expression::AccessSource(expr) => expr.full_fragment_owned(),
			Expression::Alias(expr) => expr.expression.full_fragment_owned(),
			Expression::Cast(CastExpression {
				expression: expr,
				..
			}) => expr.full_fragment_owned(),
			Expression::Constant(expr) => match expr {
				ConstantExpression::Undefined {
					fragment,
				}
				| ConstantExpression::Bool {
					fragment,
				}
				| ConstantExpression::Number {
					fragment,
				}
				| ConstantExpression::Temporal {
					fragment,
				}
				| ConstantExpression::Text {
					fragment,
				} => fragment.clone(),
			},
			Expression::Column(expr) => expr.full_fragment_owned(),

			Expression::Add(expr) => expr.full_fragment_owned(),
			Expression::Sub(expr) => expr.full_fragment_owned(),
			Expression::GreaterThan(expr) => expr.fragment.clone(),
			Expression::GreaterThanEqual(expr) => expr.fragment.clone(),
			Expression::LessThan(expr) => expr.fragment.clone(),
			Expression::LessThanEqual(expr) => expr.fragment.clone(),
			Expression::Equal(expr) => expr.fragment.clone(),
			Expression::NotEqual(expr) => expr.fragment.clone(),
			Expression::Between(expr) => expr.full_fragment_owned(),
			Expression::And(expr) => expr.fragment.clone(),
			Expression::Or(expr) => expr.fragment.clone(),
			Expression::Xor(expr) => expr.fragment.clone(),

			Expression::Mul(expr) => expr.full_fragment_owned(),
			Expression::Div(expr) => expr.full_fragment_owned(),
			Expression::Rem(expr) => expr.full_fragment_owned(),

			Expression::Tuple(expr) => {
				let fragments =
					expr.expressions.iter().map(|e| e.full_fragment_owned()).collect::<Vec<_>>();
				Fragment::merge_all(fragments)
			}
			Expression::Type(expr) => expr.fragment.clone(),

			Expression::Prefix(expr) => expr.full_fragment_owned(),

			Expression::Call(expr) => expr.full_fragment_owned(),
			Expression::Parameter(param) => match param {
				ParameterExpression::Positional {
					fragment,
					..
				} => fragment.clone(),
				ParameterExpression::Named {
					fragment,
				} => fragment.clone(),
			},
			Expression::Variable(var) => var.fragment.clone(),
			Expression::If(if_expr) => if_expr.full_fragment_owned(),
			Expression::Map(map_expr) => map_expr.fragment.clone(),
			Expression::Extend(extend_expr) => extend_expr.fragment.clone(),
		}
	}
}

impl<'a> AddExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

impl<'a> ConstantExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		match self {
			ConstantExpression::Undefined {
				fragment,
			} => fragment.clone(),
			ConstantExpression::Bool {
				fragment,
			} => fragment.clone(),
			ConstantExpression::Number {
				fragment,
			} => fragment.clone(),
			ConstantExpression::Temporal {
				fragment,
			} => fragment.clone(),
			ConstantExpression::Text {
				fragment,
			} => fragment.clone(),
		}
	}
}

impl<'a> SubExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

impl<'a> MulExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

impl<'a> DivExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

impl<'a> RemExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

impl<'a> Expression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		self.lazy_fragment()()
	}
}
