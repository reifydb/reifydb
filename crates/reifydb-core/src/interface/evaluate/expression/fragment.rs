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

impl Expression {
	pub fn lazy_fragment<'a>(&'a self) -> impl Fn() -> Fragment<'a> + 'a {
		move || match self {
			Expression::AccessSource(expr) => {
				Fragment::Owned(expr.fragment())
			}
			Expression::Alias(expr) => {
				Fragment::Owned(expr.expression.fragment())
			}
			Expression::Cast(CastExpression {
				expression: expr,
				..
			}) => Fragment::Owned(expr.fragment()),
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
				} => Fragment::Owned(fragment.clone()),
			},
			Expression::Column(expr) => {
				Fragment::Owned(expr.0.clone())
			}

			Expression::Add(expr) => {
				Fragment::Owned(expr.fragment())
			}
			Expression::Sub(expr) => {
				Fragment::Owned(expr.fragment())
			}
			Expression::GreaterThan(expr) => {
				Fragment::Owned(expr.fragment.clone())
			}
			Expression::GreaterThanEqual(expr) => {
				Fragment::Owned(expr.fragment.clone())
			}
			Expression::LessThan(expr) => {
				Fragment::Owned(expr.fragment.clone())
			}
			Expression::LessThanEqual(expr) => {
				Fragment::Owned(expr.fragment.clone())
			}
			Expression::Equal(expr) => {
				Fragment::Owned(expr.fragment.clone())
			}
			Expression::NotEqual(expr) => {
				Fragment::Owned(expr.fragment.clone())
			}
			Expression::Between(expr) => {
				Fragment::Owned(expr.fragment())
			}
			Expression::And(expr) => {
				Fragment::Owned(expr.fragment.clone())
			}
			Expression::Or(expr) => {
				Fragment::Owned(expr.fragment.clone())
			}
			Expression::Xor(expr) => {
				Fragment::Owned(expr.fragment.clone())
			}

			Expression::Mul(expr) => {
				Fragment::Owned(expr.fragment())
			}
			Expression::Div(expr) => {
				Fragment::Owned(expr.fragment())
			}
			Expression::Rem(expr) => {
				Fragment::Owned(expr.fragment())
			}

			Expression::Tuple(expr) => {
				let fragments = expr
					.expressions
					.iter()
					.map(|e| e.fragment())
					.collect::<Vec<_>>();
				Fragment::Owned(OwnedFragment::merge_all(
					fragments,
				))
			}
			Expression::Type(expr) => {
				Fragment::Owned(expr.fragment.clone())
			}

			Expression::Prefix(expr) => {
				Fragment::Owned(expr.fragment())
			}

			Expression::Call(expr) => {
				Fragment::Owned(expr.fragment())
			}
			Expression::Parameter(param) => match param {
				ParameterExpression::Positional {
					fragment,
					..
				} => Fragment::Owned(fragment.clone()),
				ParameterExpression::Named {
					fragment,
				} => Fragment::Owned(fragment.clone()),
			},
		}
	}
}

impl AddExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

impl ConstantExpression {
	pub fn fragment(&self) -> OwnedFragment {
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

impl SubExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

impl MulExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

impl DivExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

impl RemExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

impl Expression {
	pub fn fragment(&self) -> OwnedFragment {
		self.lazy_fragment()().into_owned()
	}
}
