// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB
use reifydb_type::{
	fragment::Fragment,
	value::{
		Value,
		boolean::parse::parse_bool,
		number::parse::{parse_float, parse_primitive_int, parse_primitive_uint},
	},
};

use crate::expression::{
	AddExpression, CastExpression, ConstantExpression, DivExpression, Expression, MulExpression,
	ParameterExpression, RemExpression, SubExpression,
};

impl Expression {
	pub fn lazy_fragment(&self) -> impl Fn() -> Fragment {
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
			Expression::GreaterThan(expr) => expr.full_fragment_owned(),
			Expression::GreaterThanEqual(expr) => expr.full_fragment_owned(),
			Expression::LessThan(expr) => expr.full_fragment_owned(),
			Expression::LessThanEqual(expr) => expr.full_fragment_owned(),
			Expression::Equal(expr) => expr.full_fragment_owned(),
			Expression::NotEqual(expr) => expr.full_fragment_owned(),
			Expression::Between(expr) => expr.full_fragment_owned(),
			Expression::And(expr) => expr.full_fragment_owned(),
			Expression::Or(expr) => expr.full_fragment_owned(),
			Expression::Xor(expr) => expr.full_fragment_owned(),

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
			Expression::In(in_expr) => in_expr.fragment.clone(),
		}
	}
}

impl AddExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

impl ConstantExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
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

	pub fn to_value(&self) -> Value {
		match self {
			Self::Undefined {
				..
			} => Value::Undefined,
			Self::Bool {
				fragment,
			} => parse_bool(fragment.clone()).map(Value::Boolean).unwrap_or(Value::Undefined),
			Self::Number {
				fragment,
			} => Self::parse_number(fragment),
			Self::Text {
				fragment,
			} => Value::Utf8(fragment.text().to_string()),
			Self::Temporal {
				fragment,
			} => Value::Utf8(fragment.text().to_string()),
		}
	}

	fn parse_number(fragment: &Fragment) -> Value {
		let text = fragment.text();
		if text.contains('.') || text.contains('e') || text.contains('E') {
			return parse_float::<f64>(fragment.clone()).map(Value::float8).unwrap_or(Value::Undefined);
		}
		parse_primitive_int::<i8>(fragment.clone())
			.map(Value::Int1)
			.or_else(|_| parse_primitive_int::<i16>(fragment.clone()).map(Value::Int2))
			.or_else(|_| parse_primitive_int::<i32>(fragment.clone()).map(Value::Int4))
			.or_else(|_| parse_primitive_int::<i64>(fragment.clone()).map(Value::Int8))
			.or_else(|_| parse_primitive_int::<i128>(fragment.clone()).map(Value::Int16))
			.or_else(|_| parse_primitive_uint::<u128>(fragment.clone()).map(Value::Uint16))
			.unwrap_or(Value::Undefined)
	}
}

impl SubExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

impl MulExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

impl DivExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

impl RemExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

impl Expression {
	pub fn full_fragment_owned(&self) -> Fragment {
		self.lazy_fragment()()
	}
}
