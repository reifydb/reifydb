// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.
use crate::{
	Fragment, OwnedFragment,
	interface::{
		evaluate::expression::{
			AddExpression, CastExpression, ConstantExpression,
			DivExpression, Expression, MulExpression,
			RemExpression, SubExpression,
		},
		expression::ParameterExpression,
	},
};

impl<'a> Expression<'a> {
	pub fn lazy_fragment(&self) -> impl Fn() -> Fragment<'a> + '_ {
		move || match self {
			Expression::AccessSource(expr) => expr.fragment(),
			Expression::Alias(expr) => expr.expression.fragment(),
			Expression::Cast(CastExpression {
				expression: expr,
				..
			}) => expr.fragment(),
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
			Expression::Column(expr) => expr.0.clone(),

			Expression::Add(expr) => expr.fragment(),
			Expression::Sub(expr) => expr.fragment(),
			Expression::GreaterThan(expr) => expr.fragment.clone(),
			Expression::GreaterThanEqual(expr) => {
				expr.fragment.clone()
			}
			Expression::LessThan(expr) => expr.fragment.clone(),
			Expression::LessThanEqual(expr) => {
				expr.fragment.clone()
			}
			Expression::Equal(expr) => expr.fragment.clone(),
			Expression::NotEqual(expr) => expr.fragment.clone(),
			Expression::Between(expr) => expr.fragment(),
			Expression::And(expr) => expr.fragment.clone(),
			Expression::Or(expr) => expr.fragment.clone(),
			Expression::Xor(expr) => expr.fragment.clone(),

			Expression::Mul(expr) => expr.fragment(),
			Expression::Div(expr) => expr.fragment(),
			Expression::Rem(expr) => expr.fragment(),

			Expression::Tuple(expr) => {
				let fragments = expr
					.expressions
					.iter()
					.map(|e| e.fragment().into_owned())
					.collect::<Vec<_>>();
				Fragment::Owned(OwnedFragment::merge_all(
					fragments,
				))
			}
			Expression::Type(expr) => expr.fragment.clone(),

			Expression::Prefix(expr) => expr.fragment(),

			Expression::Call(expr) => expr.fragment(),
			Expression::Parameter(param) => match param {
				ParameterExpression::Positional {
					fragment,
					..
				} => fragment.clone(),
				ParameterExpression::Named {
					fragment,
				} => fragment.clone(),
			},
		}
	}
}

impl<'a> AddExpression<'a> {
	pub fn fragment(&self) -> Fragment<'a> {
		Fragment::Owned(OwnedFragment::merge_all([
			self.left.fragment().into_owned(),
			self.fragment.clone().into_owned(),
			self.right.fragment().into_owned(),
		]))
	}
}

impl<'a> ConstantExpression<'a> {
	pub fn fragment(&self) -> Fragment<'a> {
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
	pub fn fragment(&self) -> Fragment<'a> {
		Fragment::Owned(OwnedFragment::merge_all([
			self.left.fragment().into_owned(),
			self.fragment.clone().into_owned(),
			self.right.fragment().into_owned(),
		]))
	}
}

impl<'a> MulExpression<'a> {
	pub fn fragment(&self) -> Fragment<'a> {
		Fragment::Owned(OwnedFragment::merge_all([
			self.left.fragment().into_owned(),
			self.fragment.clone().into_owned(),
			self.right.fragment().into_owned(),
		]))
	}
}

impl<'a> DivExpression<'a> {
	pub fn fragment(&self) -> Fragment<'a> {
		Fragment::Owned(OwnedFragment::merge_all([
			self.left.fragment().into_owned(),
			self.fragment.clone().into_owned(),
			self.right.fragment().into_owned(),
		]))
	}
}

impl<'a> RemExpression<'a> {
	pub fn fragment(&self) -> Fragment<'a> {
		Fragment::Owned(OwnedFragment::merge_all([
			self.left.fragment().into_owned(),
			self.fragment.clone().into_owned(),
			self.right.fragment().into_owned(),
		]))
	}
}

impl<'a> Expression<'a> {
	pub fn fragment(&self) -> Fragment<'a> {
		self.lazy_fragment()()
	}
}
