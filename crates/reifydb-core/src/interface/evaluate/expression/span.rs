// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.
use crate::{
	OwnedSpan,
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
	pub fn lazy_span(&self) -> impl Fn() -> OwnedSpan + '_ {
		move || {
			match self {
				Expression::AccessSource(expr) => expr.span(),
				Expression::Alias(expr) => {
					expr.expression.span()
				}
				Expression::Cast(CastExpression {
					expression: expr,
					..
				}) => expr.span(),
				Expression::Constant(expr) => match expr {
					ConstantExpression::Undefined {
						span,
					}
					| ConstantExpression::Bool {
						span,
					}
					| ConstantExpression::Number {
						span,
					}
					| ConstantExpression::Temporal {
						span,
					}
					| ConstantExpression::Text {
						span,
					} => span.clone(),
				},
				Expression::Column(expr) => expr.0.clone(),

				Expression::Add(expr) => expr.span(),
				Expression::Sub(expr) => expr.span(),
				Expression::GreaterThan(expr) => {
					expr.span.clone()
				}
				Expression::GreaterThanEqual(expr) => {
					expr.span.clone()
				}
				Expression::LessThan(expr) => expr.span.clone(),
				Expression::LessThanEqual(expr) => {
					expr.span.clone()
				}
				Expression::Equal(expr) => expr.span.clone(),
				Expression::NotEqual(expr) => expr.span.clone(),
				Expression::Between(expr) => expr.span(),
				Expression::And(expr) => expr.span.clone(),
				Expression::Or(expr) => expr.span.clone(),
				Expression::Xor(expr) => expr.span.clone(),

				Expression::Mul(expr) => expr.span(),
				Expression::Div(expr) => expr.span(),
				Expression::Rem(expr) => expr.span(),

				Expression::Tuple(_expr) => {
					// let spans =
					// expr.elements.iter().map(|e|
					// e.span()).collect::<Vec<_>>();
					// Span::merge_all(spans).unwrap()
					unimplemented!()
				}
				Expression::Type(expr) => expr.span.clone(),

				Expression::Prefix(expr) => expr.span(),

				Expression::Call(expr) => expr.span(),
				Expression::Parameter(param) => match param {
					ParameterExpression::Positional {
						span,
						..
					} => span.clone(),
					ParameterExpression::Named {
						span,
					} => span.clone(),
				},
			}
		}
	}
}

impl AddExpression {
	pub fn span(&self) -> OwnedSpan {
		OwnedSpan::merge_all([
			self.left.span(),
			self.span.clone(),
			self.right.span(),
		])
	}
}

impl ConstantExpression {
	pub fn span(&self) -> OwnedSpan {
		match self {
			ConstantExpression::Undefined {
				span,
			} => span.clone(),
			ConstantExpression::Bool {
				span,
			} => span.clone(),
			ConstantExpression::Number {
				span,
			} => span.clone(),
			ConstantExpression::Temporal {
				span,
			} => span.clone(),
			ConstantExpression::Text {
				span,
			} => span.clone(),
		}
	}
}

impl SubExpression {
	pub fn span(&self) -> OwnedSpan {
		OwnedSpan::merge_all([
			self.left.span(),
			self.span.clone(),
			self.right.span(),
		])
	}
}

impl MulExpression {
	pub fn span(&self) -> OwnedSpan {
		OwnedSpan::merge_all([
			self.left.span(),
			self.span.clone(),
			self.right.span(),
		])
	}
}

impl DivExpression {
	pub fn span(&self) -> OwnedSpan {
		OwnedSpan::merge_all([
			self.left.span(),
			self.span.clone(),
			self.right.span(),
		])
	}
}

impl RemExpression {
	pub fn span(&self) -> OwnedSpan {
		OwnedSpan::merge_all([
			self.left.span(),
			self.span.clone(),
			self.right.span(),
		])
	}
}

impl Expression {
	pub fn span(&self) -> OwnedSpan {
		self.lazy_span()()
	}
}
