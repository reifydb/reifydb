// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod fragment;
mod join;
pub mod json;
mod name;

pub use join::JoinConditionCompiler;
pub use name::*;

use crate::{
	ast,
	ast::{Ast, AstInfix, AstLiteral, InfixOperator, parse_str},
	convert_data_type,
};

pub fn parse_expression(rql: &str) -> crate::Result<Vec<Expression<'_>>> {
	let statements = parse_str(rql)?;
	if statements.is_empty() {
		return Ok(vec![]);
	}

	let mut result = Vec::new();
	for statement in statements {
		for ast in statement.nodes {
			result.push(ExpressionCompiler::compile(ast)?);
		}
	}

	Ok(result)
}

use std::{
	fmt,
	fmt::{Display, Formatter},
};

use reifydb_core::interface::{ColumnIdentifier, ColumnSource};
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

	In(InExpression<'a>),

	Type(TypeExpression<'a>),

	Parameter(ParameterExpression<'a>),
	Variable(VariableExpression<'a>),

	If(IfExpression<'a>),
	Map(MapExpression<'a>),
	Extend(ExtendExpression<'a>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessSourceExpression<'a> {
	pub column: ColumnIdentifier<'a>,
}

impl<'a> AccessSourceExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		// For backward compatibility, merge source and column fragments
		match &self.column.source {
			ColumnSource::Source {
				source,
				..
			} => Fragment::merge_all([source.clone(), self.column.name.clone()]),
			ColumnSource::Alias(alias) => Fragment::merge_all([alias.clone(), self.column.name.clone()]),
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
pub struct InExpression<'a> {
	pub value: Box<Expression<'a>>,
	pub list: Box<Expression<'a>>,
	pub negated: bool,
	pub fragment: Fragment<'a>,
}

impl<'a> InExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		Fragment::merge_all([
			self.value.full_fragment_owned(),
			self.fragment.clone(),
			self.list.full_fragment_owned(),
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
				ColumnSource::Source {
					source,
					..
				} => {
					write!(f, "{}.{}", source.text(), column.name.text())
				}
				ColumnSource::Alias(alias) => {
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
				write!(f, "{}", column.name.text())
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
			Expression::In(InExpression {
				value,
				list,
				negated,
				..
			}) => {
				if *negated {
					write!(f, "({} NOT IN {})", value, list)
				} else {
					write!(f, "({} IN {})", value, list)
				}
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
			Expression::Variable(var) => write!(f, "{}", var.fragment.text()),
			Expression::If(if_expr) => write!(f, "{}", if_expr),
			Expression::Map(map_expr) => write!(
				f,
				"MAP{{ {} }}",
				map_expr.expressions
					.iter()
					.map(|expr| format!("{}", expr))
					.collect::<Vec<_>>()
					.join(", ")
			),
			Expression::Extend(extend_expr) => write!(
				f,
				"EXTEND{{ {} }}",
				extend_expr
					.expressions
					.iter()
					.map(|expr| format!("{}", expr))
					.collect::<Vec<_>>()
					.join(", ")
			),
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VariableExpression<'a> {
	pub fragment: Fragment<'a>,
}

impl<'a> VariableExpression<'a> {
	pub fn name(&self) -> &str {
		// Extract variable name from token value (skip the '$')
		let text = self.fragment.text();
		if text.starts_with('$') {
			&text[1..]
		} else {
			text
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IfExpression<'a> {
	pub condition: Box<Expression<'a>>,
	pub then_expr: Box<Expression<'a>>,
	pub else_ifs: Vec<ElseIfExpression<'a>>,
	pub else_expr: Option<Box<Expression<'a>>>,
	pub fragment: Fragment<'a>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElseIfExpression<'a> {
	pub condition: Box<Expression<'a>>,
	pub then_expr: Box<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

impl<'a> IfExpression<'a> {
	pub fn full_fragment_owned(&self) -> Fragment<'a> {
		self.fragment.clone()
	}

	pub fn lazy_fragment(&self) -> impl Fn() -> Fragment<'a> + '_ {
		move || self.full_fragment_owned()
	}
}

impl<'a> Display for IfExpression<'a> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "if {} {{ {} }}", self.condition, self.then_expr)?;

		for else_if in &self.else_ifs {
			write!(f, " else if {} {{ {} }}", else_if.condition, else_if.then_expr)?;
		}

		if let Some(else_expr) = &self.else_expr {
			write!(f, " else {{ {} }}", else_expr)?;
		}

		Ok(())
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

pub struct ExpressionCompiler {}

impl ExpressionCompiler {
	pub fn compile<'a>(ast: Ast<'a>) -> crate::Result<Expression<'a>> {
		match ast {
			Ast::Literal(literal) => match literal {
				AstLiteral::Boolean(_) => Ok(Expression::Constant(ConstantExpression::Bool {
					fragment: literal.fragment(),
				})),
				AstLiteral::Number(_) => Ok(Expression::Constant(ConstantExpression::Number {
					fragment: literal.fragment(),
				})),
				AstLiteral::Temporal(_) => Ok(Expression::Constant(ConstantExpression::Temporal {
					fragment: literal.fragment(),
				})),
				AstLiteral::Text(_) => Ok(Expression::Constant(ConstantExpression::Text {
					fragment: literal.fragment(),
				})),
				AstLiteral::Undefined(_) => Ok(Expression::Constant(ConstantExpression::Undefined {
					fragment: literal.fragment(),
				})),
			},
			Ast::Identifier(identifier) => {
				// Create an unqualified column identifier
				use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnSource};
				use reifydb_type::OwnedFragment;

				let column = ColumnIdentifier {
					source: ColumnSource::Source {
						namespace: Fragment::Owned(OwnedFragment::Internal {
							text: String::from("_context"),
						}),
						source: Fragment::Owned(OwnedFragment::Internal {
							text: String::from("_context"),
						}),
					},
					name: identifier.token.fragment.clone(),
				};
				Ok(Expression::Column(ColumnExpression(column)))
			}
			Ast::CallFunction(call) => {
				// Build the full function name from namespace + function
				let full_name = if call.function.namespaces.is_empty() {
					call.function.name.text().to_string()
				} else {
					let namespace_path = call
						.function
						.namespaces
						.iter()
						.map(|ns| ns.text())
						.collect::<Vec<_>>()
						.join("::");
					format!("{}::{}", namespace_path, call.function.name.text())
				};

				// Compile arguments
				let mut arg_expressions = Vec::new();
				for arg_ast in call.arguments.nodes {
					arg_expressions.push(Self::compile(arg_ast)?);
				}

				Ok(Expression::Call(CallExpression {
					func: IdentExpression(Fragment::Owned(OwnedFragment::testing(&full_name))),
					args: arg_expressions,
					fragment: call.token.fragment,
				}))
			}
			Ast::Infix(ast) => Self::infix(ast),
			Ast::Between(between) => {
				let value = Self::compile(*between.value)?;
				let lower = Self::compile(*between.lower)?;
				let upper = Self::compile(*between.upper)?;

				Ok(Expression::Between(BetweenExpression {
					value: Box::new(value),
					lower: Box::new(lower),
					upper: Box::new(upper),
					fragment: between.token.fragment,
				}))
			}
			Ast::Tuple(tuple) => {
				let mut expressions = Vec::with_capacity(tuple.len());

				for ast in tuple.nodes {
					expressions.push(Self::compile(ast)?);
				}

				Ok(Expression::Tuple(TupleExpression {
					expressions,
					fragment: tuple.token.fragment,
				}))
			}
			Ast::Prefix(prefix) => {
				let (fragment, operator) = match prefix.operator {
					ast::AstPrefixOperator::Plus(token) => {
						(token.fragment.clone(), PrefixOperator::Plus(token.fragment))
					}
					ast::AstPrefixOperator::Negate(token) => {
						(token.fragment.clone(), PrefixOperator::Minus(token.fragment))
					}
					ast::AstPrefixOperator::Not(token) => {
						(token.fragment.clone(), PrefixOperator::Not(token.fragment))
					}
				};

				Ok(Expression::Prefix(PrefixExpression {
					operator,
					expression: Box::new(Self::compile(*prefix.node)?),
					fragment,
				}))
			}
			Ast::Cast(node) => {
				let mut tuple = node.tuple;
				let node = tuple.nodes.pop().unwrap();
				let fragment = node.as_identifier().token.fragment.clone();
				let ty = convert_data_type(&fragment)?;

				let expr = tuple.nodes.pop().unwrap();

				Ok(Expression::Cast(CastExpression {
					fragment: tuple.token.fragment,
					expression: Box::new(Self::compile(expr)?),
					to: TypeExpression {
						fragment,
						ty,
					},
				}))
			}
			Ast::Variable(var) => Ok(Expression::Variable(VariableExpression {
				fragment: var.token.fragment,
			})),
			Ast::Rownum(_rownum) => {
				// Compile rownum to a column reference for rownum
				use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnSource};
				use reifydb_type::{OwnedFragment, ROW_NUMBER_COLUMN_NAME};

				let column = ColumnIdentifier {
					source: ColumnSource::Source {
						namespace: Fragment::Owned(OwnedFragment::Internal {
							text: String::from("_context"),
						}),
						source: Fragment::Owned(OwnedFragment::Internal {
							text: String::from("_context"),
						}),
					},
					name: Fragment::Owned(OwnedFragment::Internal {
						text: String::from(ROW_NUMBER_COLUMN_NAME),
					}),
				};
				Ok(Expression::Column(ColumnExpression(column)))
			}
			Ast::If(if_ast) => {
				// Compile condition
				let condition = Box::new(Self::compile(*if_ast.condition)?);

				// Compile then expression
				let then_expr = Box::new(Self::compile(*if_ast.then_block)?);

				// Compile else_if chains
				let mut else_ifs = Vec::new();
				for else_if in if_ast.else_ifs {
					let else_if_condition = Box::new(Self::compile(*else_if.condition)?);
					let else_if_then = Box::new(Self::compile(*else_if.then_block)?);
					else_ifs.push(ElseIfExpression {
						condition: else_if_condition,
						then_expr: else_if_then,
						fragment: else_if.token.fragment,
					});
				}

				// Compile optional else expression
				let else_expr = if let Some(else_block) = if_ast.else_block {
					Some(Box::new(Self::compile(*else_block)?))
				} else {
					None
				};

				Ok(Expression::If(IfExpression {
					condition,
					then_expr,
					else_ifs,
					else_expr,
					fragment: if_ast.token.fragment,
				}))
			}
			Ast::Map(map) => {
				// Compile expressions in the map
				let mut expressions = Vec::with_capacity(map.nodes.len());
				for node in map.nodes {
					expressions.push(Self::compile(node)?);
				}

				Ok(Expression::Map(MapExpression {
					expressions,
					fragment: map.token.fragment,
				}))
			}
			Ast::Extend(extend) => {
				// Compile expressions in the extend
				let mut expressions = Vec::with_capacity(extend.nodes.len());
				for node in extend.nodes {
					expressions.push(Self::compile(node)?);
				}

				Ok(Expression::Extend(ExtendExpression {
					expressions,
					fragment: extend.token.fragment,
				}))
			}
			Ast::List(list) => {
				// Compile list expressions (used for IN [...] syntax)
				let mut expressions = Vec::with_capacity(list.nodes.len());
				for ast in list.nodes {
					expressions.push(Self::compile(ast)?);
				}
				Ok(Expression::Tuple(TupleExpression {
					expressions,
					fragment: list.token.fragment,
				}))
			}
			ast => unimplemented!("{:?}", ast),
		}
	}

	fn infix<'a>(ast: AstInfix<'a>) -> crate::Result<Expression<'a>> {
		match ast.operator {
			InfixOperator::Add(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;
				Ok(Expression::Add(AddExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::Divide(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;
				Ok(Expression::Div(DivExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::Subtract(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;
				Ok(Expression::Sub(SubExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::Rem(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;
				Ok(Expression::Rem(RemExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::Multiply(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;
				Ok(Expression::Mul(MulExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::Call(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				let Expression::Column(ColumnExpression(column)) = left else {
					panic!()
				};
				let Expression::Tuple(tuple) = right else {
					panic!()
				};

				Ok(Expression::Call(CallExpression {
					func: IdentExpression(column.name),
					args: tuple.expressions,
					fragment: token.fragment,
				}))
			}
			InfixOperator::GreaterThan(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::GreaterThan(GreaterThanExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::GreaterThanEqual(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::GreaterThanEqual(GreaterThanEqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::LessThan(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::LessThan(LessThanExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::LessThanEqual(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::LessThanEqual(LessThanEqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::Equal(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::Equal(EqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::NotEqual(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::NotEqual(NotEqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::As(token) => {
				let left = Self::compile(*ast.left)?;
				let Ast::Identifier(right) = *ast.right else {
					unimplemented!()
				};

				Ok(Expression::Alias(AliasExpression {
					alias: IdentExpression(right.token.fragment),
					expression: Box::new(left),
					fragment: token.fragment,
				}))
			}

			InfixOperator::And(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::And(AndExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}

			InfixOperator::Or(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::Or(OrExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}

			InfixOperator::Xor(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::Xor(XorExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}

			InfixOperator::In(token) => {
				let value = Self::compile(*ast.left)?;
				let list = Self::compile(*ast.right)?;

				Ok(Expression::In(InExpression {
					value: Box::new(value),
					list: Box::new(list),
					negated: false,
					fragment: token.fragment,
				}))
			}

			InfixOperator::NotIn(token) => {
				let value = Self::compile(*ast.left)?;
				let list = Self::compile(*ast.right)?;

				Ok(Expression::In(InExpression {
					value: Box::new(value),
					list: Box::new(list),
					negated: true,
					fragment: token.fragment,
				}))
			}

			InfixOperator::Assign(token) => {
				// Treat = as == for equality comparison in
				// expressions
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::Equal(EqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}

			InfixOperator::TypeAscription(token) => {
				match *ast.left {
					Ast::Identifier(alias) => {
						let right = Self::compile(*ast.right)?;

						Ok(Expression::Alias(AliasExpression {
							alias: IdentExpression(alias.token.fragment),
							expression: Box::new(right),
							fragment: token.fragment,
						}))
					}
					Ast::Literal(AstLiteral::Text(text)) => {
						// Handle string literals as alias names (common in MAP syntax)
						let right = Self::compile(*ast.right)?;

						Ok(Expression::Alias(AliasExpression {
							alias: IdentExpression(text.0.fragment),
							expression: Box::new(right),
							fragment: token.fragment,
						}))
					}
					_ => {
						use reifydb_type::{OwnedFragment, diagnostic::Diagnostic, err};
						return err!(Diagnostic {
							code: "EXPR_001".to_string(),
							statement: None,
							message: "Invalid alias expression".to_string(),
							column: None,
							fragment: OwnedFragment::None,
							label: Some("Only identifiers and string literals can be used as alias names".to_string()),
							help: Some("Use an identifier or string literal for the alias name".to_string()),
							notes: vec![],
							cause: None,
						});
					}
				}
			}
			operator => {
				unimplemented!("not implemented: {operator:?}")
			} /* InfixOperator::Arrow(_) => {}
			   * InfixOperator::AccessPackage(_) => {}
			   * InfixOperator::Subtract(_) => {}
			   * InfixOperator::Multiply(_) => {}
			   * InfixOperator::Divide(_) => {}
			   * InfixOperator::Rem(_) => {}
			   * InfixOperator::TypeAscription(_) => {} */
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapExpression<'a> {
	pub expressions: Vec<Expression<'a>>,
	pub fragment: Fragment<'a>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendExpression<'a> {
	pub expressions: Vec<Expression<'a>>,
	pub fragment: Fragment<'a>,
}
