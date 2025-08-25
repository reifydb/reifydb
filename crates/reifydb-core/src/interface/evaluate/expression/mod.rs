// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod fragment;

use std::{
	fmt,
	fmt::{Display, Formatter},
};

use serde::{Deserialize, Serialize};

use crate::{OwnedFragment, Type};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasExpression {
	pub alias: IdentExpression,
	pub expression: Box<Expression>,
	pub fragment: OwnedFragment,
}

impl Display for AliasExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		Display::fmt(&self.alias, f)
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
	AccessSource(AccessSourceExpression),

	Alias(AliasExpression),

	Cast(CastExpression),

	Constant(ConstantExpression),

	Column(ColumnExpression),

	Add(AddExpression),

	Div(DivExpression),

	Call(Caltokenizepression),

	Rem(RemExpression),

	Mul(Mutokenizepression),

	Sub(SubExpression),

	Tuple(TupleExpression),

	Prefix(PrefixExpression),

	GreaterThan(GreaterThanExpression),

	GreaterThanEqual(GreaterThanEquatokenizepression),

	LessThan(LessThanExpression),

	LessThanEqual(LessThanEquatokenizepression),

	Equal(Equatokenizepression),

	NotEqual(NotEquatokenizepression),

	Between(BetweenExpression),

	And(AndExpression),

	Or(OrExpression),

	Xor(XorExpression),

	Type(TypeExpression),

	Parameter(ParameterExpression),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessSourceExpression {
	pub source: OwnedFragment,
	pub column: OwnedFragment,
}

impl AccessSourceExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.source.clone(),
			self.column.clone(),
		])
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstantExpression {
	Undefined {
		fragment: OwnedFragment,
	},
	Bool {
		fragment: OwnedFragment,
	},
	// any number
	Number {
		fragment: OwnedFragment,
	},
	// any textual representation can be String, Text, ...
	Text {
		fragment: OwnedFragment,
	},
	// any temporal representation can be Date, Time, DateTime, ...
	Temporal {
		fragment: OwnedFragment,
	},
}

impl Display for ConstantExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			ConstantExpression::Undefined {
				..
			} => write!(f, "undefined"),
			ConstantExpression::Bool {
				fragment,
			} => write!(f, "{}", fragment.fragment()),
			ConstantExpression::Number {
				fragment,
			} => write!(f, "{}", fragment.fragment()),
			ConstantExpression::Text {
				fragment,
			} => write!(f, "\"{}\"", fragment.fragment()),
			ConstantExpression::Temporal {
				fragment,
			} => write!(f, "{}", fragment.fragment()),
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastExpression {
	pub fragment: OwnedFragment,
	pub expression: Box<Expression>,
	pub to: TypeExpression,
}

impl CastExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.fragment.clone(),
			self.expression.fragment(),
			self.to.fragment(),
		])
	}

	pub fn lazy_fragment(&self) -> impl Fn() -> OwnedFragment + '_ {
		move || self.fragment()
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeExpression {
	pub fragment: OwnedFragment,
	pub ty: Type,
}

impl TypeExpression {
	pub fn fragment(&self) -> OwnedFragment {
		self.fragment.clone()
	}

	pub fn lazy_fragment(&self) -> impl Fn() -> OwnedFragment + '_ {
		move || self.fragment()
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DivExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mutokenizepression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GreaterThanExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

impl GreaterThanExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GreaterThanEquatokenizepression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

impl GreaterThanEquatokenizepression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LessThanExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

impl LessThanExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LessThanEquatokenizepression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

impl LessThanEquatokenizepression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Equatokenizepression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

impl Equatokenizepression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotEquatokenizepression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

impl NotEquatokenizepression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BetweenExpression {
	pub value: Box<Expression>,
	pub lower: Box<Expression>,
	pub upper: Box<Expression>,
	pub fragment: OwnedFragment,
}

impl BetweenExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.value.fragment(),
			self.fragment.clone(),
			self.lower.fragment(),
			self.upper.fragment(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AndExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

impl AndExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

impl OrExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XorExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: OwnedFragment,
}

impl XorExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.left.fragment(),
			self.fragment.clone(),
			self.right.fragment(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnExpression(pub OwnedFragment);

impl ColumnExpression {
	pub fn fragment(&self) -> OwnedFragment {
		self.0.clone()
	}
}

impl Display for Expression {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			Expression::AccessSource(AccessSourceExpression {
				source: target,
				column: property,
			}) => {
				write!(
					f,
					"{}.{}",
					target.fragment(),
					property.fragment()
				)
			}
			Expression::Alias(AliasExpression {
				alias,
				expression,
				..
			}) => {
				write!(f, "{} as {}", expression, alias)
			}
			Expression::Cast(CastExpression {
				expression: expr,
				..
			}) => write!(f, "{}", expr),
			Expression::Constant(fragment) => {
				write!(f, "Constant({})", fragment)
			}
			Expression::Column(ColumnExpression(fragment)) => {
				write!(f, "Column({})", fragment.fragment())
			}
			Expression::Add(AddExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} + {})", left, right)
			}
			Expression::Div(DivExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} / {})", left, right)
			}
			Expression::Call(call) => write!(f, "{}", call),
			Expression::Rem(RemExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} % {})", left, right)
			}
			Expression::Mul(Mutokenizepression {
				left,
				right,
				..
			}) => {
				write!(f, "({} * {})", left, right)
			}
			Expression::Sub(SubExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} - {})", left, right)
			}
			Expression::Tuple(tuple) => write!(f, "({})", tuple),
			Expression::Prefix(prefix) => write!(f, "{}", prefix),
			Expression::GreaterThan(GreaterThanExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} > {})", left, right)
			}
			Expression::GreaterThanEqual(
				GreaterThanEquatokenizepression {
					left,
					right,
					..
				},
			) => {
				write!(f, "({} >= {})", left, right)
			}
			Expression::LessThan(LessThanExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} < {})", left, right)
			}
			Expression::LessThanEqual(
				LessThanEquatokenizepression {
					left,
					right,
					..
				},
			) => {
				write!(f, "({} <= {})", left, right)
			}
			Expression::Equal(Equatokenizepression {
				left,
				right,
				..
			}) => {
				write!(f, "({} == {})", left, right)
			}
			Expression::NotEqual(NotEquatokenizepression {
				left,
				right,
				..
			}) => {
				write!(f, "({} != {})", left, right)
			}
			Expression::Between(BetweenExpression {
				value,
				lower,
				upper,
				..
			}) => {
				write!(
					f,
					"({} BETWEEN {} AND {})",
					value, lower, upper
				)
			}
			Expression::And(AndExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} and {})", left, right)
			}
			Expression::Or(OrExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} or {})", left, right)
			}
			Expression::Xor(XorExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} xor {})", left, right)
			}
			Expression::Type(TypeExpression {
				fragment,
				..
			}) => write!(f, "{}", fragment.fragment()),
			Expression::Parameter(param) => match param {
				ParameterExpression::Positional {
					fragment,
					..
				} => write!(f, "{}", fragment.fragment()),
				ParameterExpression::Named {
					fragment,
				} => write!(f, "{}", fragment.fragment()),
			},
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Caltokenizepression {
	pub func: IdentExpression,
	pub args: Vec<Expression>,
	pub fragment: OwnedFragment,
}

impl Caltokenizepression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::Statement {
			column: self.func.0.column(),
			line: self.func.0.line(),
			text: format!(
				"{}({})",
				self.func.0.fragment(),
				self.args
					.iter()
					.map(|arg| arg
						.fragment()
						.fragment()
						.to_string())
					.collect::<Vec<_>>()
					.join(",")
			),
		}
	}
}

impl Display for Caltokenizepression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let args = self
			.args
			.iter()
			.map(|arg| format!("{}", arg))
			.collect::<Vec<_>>()
			.join(", ");
		write!(f, "{}({})", self.func, args)
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentExpression(pub OwnedFragment);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterExpression {
	Positional {
		fragment: OwnedFragment,
	},
	Named {
		fragment: OwnedFragment,
	},
}

impl ParameterExpression {
	pub fn position(&self) -> Option<u32> {
		match self {
			ParameterExpression::Positional {
				fragment,
			} => fragment.fragment()[1..].parse().ok(),
			ParameterExpression::Named {
				..
			} => None,
		}
	}

	pub fn name(&self) -> Option<&str> {
		match self {
			ParameterExpression::Named {
				fragment,
			} => Some(&fragment.fragment()[1..]),
			ParameterExpression::Positional {
				..
			} => None,
		}
	}
}

impl IdentExpression {
	pub fn name(&self) -> &str {
		self.0.fragment()
	}
}

impl Display for IdentExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0.fragment())
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PrefixOperator {
	Minus(OwnedFragment),
	Plus(OwnedFragment),
	Not(OwnedFragment),
}

impl PrefixOperator {
	pub fn fragment(&self) -> OwnedFragment {
		match self {
			PrefixOperator::Minus(fragment) => fragment.clone(),
			PrefixOperator::Plus(fragment) => fragment.clone(),
			PrefixOperator::Not(fragment) => fragment.clone(),
		}
	}
}

impl Display for PrefixOperator {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			PrefixOperator::Minus(_) => write!(f, "-"),
			PrefixOperator::Plus(_) => write!(f, "+"),
			PrefixOperator::Not(_) => write!(f, "not"),
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefixExpression {
	pub operator: PrefixOperator,
	pub expression: Box<Expression>,
	pub fragment: OwnedFragment,
}

impl PrefixExpression {
	pub fn fragment(&self) -> OwnedFragment {
		OwnedFragment::merge_all([
			self.operator.fragment(),
			self.expression.fragment(),
		])
	}
}

impl Display for PrefixExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "({}{})", self.operator, self.expression)
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TupleExpression {
	pub expressions: Vec<Expression>,
	pub fragment: OwnedFragment,
}

impl Display for TupleExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let items = self
			.expressions
			.iter()
			.map(|e| format!("{}", e))
			.collect::<Vec<_>>()
			.join(", ");
		write!(f, "({})", items)
	}
}
