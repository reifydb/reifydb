// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod fragment;

use std::{
	fmt,
	fmt::{Display, Formatter},
};

use reifydb_type::{Fragment, OwnedFragment, Type};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasExpression<'a> {
	pub alias: IdentExpression<'a>,
	pub expression: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> Display for AliasExpression<'a> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		Display::fmt(&self.alias, f)
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression<'a> {
	AccessSource(AccessSourceExpression<'a>),

	Alias(AliasExpression<'a>),

	Cast(CastExpression<'a>),

	Constant(ConstantExpression<'a>),

	Column(ColumnExpression<'a>),

	Add(AddExpression<'a>),

	Div(DivExpression<'a>),

	Call(CallExpression<'a>),

	Rem(RemExpression<'a>),

	Mul(MulExpression<'a>),

	Sub(SubExpression<'a>),

	Tuple(TupleExpression<'a>),

	Prefix(PrefixExpression<'a>),

	GreaterThan(GreaterThanExpression<'a>),

	GreaterThanEqual(GreaterThanEqExpression<'a>),

	LessThan(LessThanExpression<'a>),

	LessThanEqual(LessThanEqExpression<'a>),

	Equal(EqExpression<'a>),

	NotEqual(NotEqExpression<'a>),

	Between(BetweenExpression<'a>),

	And(AndExpression<'a>),

	Or(OrExpression<'a>),

	Xor(XorExpression<'a>),

	Type(TypeExpression<'a>),

	Parameter(ParameterExpression<'a>),
}

use crate::interface::identifier::ColumnIdentifier;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessSourceExpression<'a> {
	pub column: ColumnIdentifier<'a>,
}

impl<'a> AccessSourceExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		// For backward compatibility, merge source and column fragments
		match &self.column.source {
			crate::interface::identifier::ColumnSource::Source {
				source,
				..
			} => Fragment::merge_all([source.clone(), self.column.name.clone()]),
			crate::interface::identifier::ColumnSource::Alias(alias) => {
				Fragment::merge_all([alias.clone(), self.column.name.clone()])
			}
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstantExpression<'a> {
	Undefined {
		fragment: Fragment<'a>,
	},
	Bool {
		fragment: Fragment<'a>,
	},
	// any number
	Number {
		fragment: Fragment<'a>,
	},
	// any textual representation can be String, Text, ...
	Text {
		fragment: Fragment<'a>,
	},
	// any temporal representation can be Date, Time, DateTime, ...
	Temporal {
		fragment: Fragment<'a>,
	},
}

impl<'a> Display for ConstantExpression<'a> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			ConstantExpression::Undefined {
				..
			} => write!(f, "undefined"),
			ConstantExpression::Bool {
				fragment,
			} => write!(f, "{}", fragment.text()),
			ConstantExpression::Number {
				fragment,
			} => write!(f, "{}", fragment.text()),
			ConstantExpression::Text {
				fragment,
			} => write!(f, "\"{}\"", fragment.text()),
			ConstantExpression::Temporal {
				fragment,
			} => write!(f, "{}", fragment.text()),
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastExpression<'a> {
	pub fragment: Fragment<'a>,
	pub expression: Box<Expression<'a>>,
	pub to: TypeExpression<'a>,
}

impl<'a> CastExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.fragment.clone(),
			self.expression.full_fragment_owned(),
			self.to.full_fragment_owned(),
		])
	}

	pub fn lazy_fragment(&self) -> impl Fn() -> Fragment<'a> + '_ {
		move || self.full_fragment_owned()
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeExpression<'a> {
	pub fragment: Fragment<'a>,
	pub ty: Type,
}

impl<'a> TypeExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		self.fragment.clone()
	}

	pub fn lazy_fragment(&self) -> impl Fn() -> Fragment<'a> + '_ {
		move || self.full_fragment_owned()
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DivExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MulExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GreaterThanExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> GreaterThanExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GreaterThanEqExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> GreaterThanEqExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LessThanExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> LessThanExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LessThanEqExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> LessThanEqExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EqExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> EqExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotEqExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> NotEqExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BetweenExpression<'a> {
	pub value: Box<Expression<'a>>,
	pub lower: Box<Expression<'a>>,
	pub upper: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> BetweenExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.value.full_fragment_owned(),
			self.fragment.clone(),
			self.lower.full_fragment_owned(),
			self.upper.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AndExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> AndExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> OrExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XorExpression<'a> {
	pub left: Box<Expression<'a>>,
	pub right: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> XorExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnExpression<'a>(pub ColumnIdentifier<'a>);

impl<'a> ColumnExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		// Return just the column name for unqualified column references
		self.0.name.clone()
	}

	pub fn column(&self) -> &ColumnIdentifier<'a> {
		&self.0
	}
}

impl<'a> Display for Expression<'a> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			Expression::AccessSource(AccessSourceExpression {
				column,
			}) => match &column.source {
				crate::interface::identifier::ColumnSource::Source {
					source,
					..
				} => {
					write!(f, "{}.{}", source.text(), column.name.text())
				}
				crate::interface::identifier::ColumnSource::Alias(alias) => {
					write!(f, "{}.{}", alias.text(), column.name.text())
				}
			},
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
			Expression::Column(ColumnExpression(column)) => {
				write!(f, "Column({})", column.name.text())
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
			Expression::Mul(MulExpression {
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
			Expression::GreaterThanEqual(GreaterThanEqExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} >= {})", left, right)
			}
			Expression::LessThan(LessThanExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} < {})", left, right)
			}
			Expression::LessThanEqual(LessThanEqExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} <= {})", left, right)
			}
			Expression::Equal(EqExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} == {})", left, right)
			}
			Expression::NotEqual(NotEqExpression {
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
				write!(f, "({} BETWEEN {} AND {})", value, lower, upper)
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
			}) => write!(f, "{}", fragment.text()),
			Expression::Parameter(param) => match param {
				ParameterExpression::Positional {
					fragment,
					..
				} => write!(f, "{}", fragment.text()),
				ParameterExpression::Named {
					fragment,
				} => write!(f, "{}", fragment.text()),
			},
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallExpression<'a> {
	pub func: IdentExpression<'a>,
	pub args: Vec<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> CallExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::Owned(OwnedFragment::Statement {
			column: self.func.0.column(),
			line: self.func.0.line(),
			text: format!(
				"{}({})",
				self.func.0.text(),
				self.args
					.iter()
					.map(|arg| arg.full_fragment_owned().text().to_string())
					.collect::<Vec<_>>()
					.join(",")
			),
		})
	}
}

impl<'a> Display for CallExpression<'a> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let args = self.args.iter().map(|arg| format!("{}", arg)).collect::<Vec<_>>().join(", ");
		write!(f, "{}({})", self.func, args)
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentExpression<'a>(pub Fragment<'a>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterExpression<'a> {
	Positional {
		fragment: Fragment<'a>,
	},
	Named {
		fragment: Fragment<'a>,
	},
}

impl<'a> ParameterExpression<'a> {
	pub fn position(&self) -> Option<u32> {
		match self {
			ParameterExpression::Positional {
				fragment,
			} => fragment.text()[1..].parse().ok(),
			ParameterExpression::Named {
				..
			} => None,
		}
	}

	pub fn name(&self) -> Option<&str> {
		match self {
			ParameterExpression::Named {
				fragment,
			} => Some(&fragment.text()[1..]),
			ParameterExpression::Positional {
				..
			} => None,
		}
	}
}

impl<'a> IdentExpression<'a> {
	pub fn name(&self) -> &str {
		self.0.text()
	}
}

impl<'a> Display for IdentExpression<'a> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0.text())
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PrefixOperator<'a> {
	Minus(Fragment<'a>),
	Plus(Fragment<'a>),
	Not(Fragment<'a>),
}

impl<'a> PrefixOperator<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		match self {
			PrefixOperator::Minus(fragment) => fragment.clone(),
			PrefixOperator::Plus(fragment) => fragment.clone(),
			PrefixOperator::Not(fragment) => fragment.clone(),
		}
	}
}

impl<'a> Display for PrefixOperator<'a> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			PrefixOperator::Minus(_) => write!(f, "-"),
			PrefixOperator::Plus(_) => write!(f, "+"),
			PrefixOperator::Not(_) => write!(f, "not"),
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefixExpression<'a> {
	pub operator: PrefixOperator<'a>,
	pub expression: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> PrefixExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([self.operator.full_fragment_owned(), self.expression.full_fragment_owned()])
	}
}

impl<'a> Display for PrefixExpression<'a> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "({}{})", self.operator, self.expression)
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TupleExpression<'a> {
	pub expressions: Vec<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> Display for TupleExpression<'a> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let items = self.expressions.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ");
		write!(f, "({})", items)
	}
}
