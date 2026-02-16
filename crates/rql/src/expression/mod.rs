// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod fragment;
pub mod join;
pub mod json;
pub mod name;

use crate::{
	ast,
	ast::{
		ast::{Ast, AstInfix, AstLiteral, InfixOperator},
		parse_str,
	},
	bump::{Bump, BumpBox},
	convert_data_type,
};

pub fn parse_expression(rql: &str) -> crate::Result<Vec<Expression>> {
	let bump = Bump::new();
	let statements = parse_str(&bump, rql)?;
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
	str::FromStr,
	sync::Arc,
};

use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnPrimitive};
use reifydb_type::{fragment::Fragment, value::r#type::Type};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasExpression {
	pub alias: IdentExpression,
	pub expression: Box<Expression>,
	pub fragment: Fragment,
}

impl Display for AliasExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		Display::fmt(&self.alias, f)
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
	AccessSource(AccessPrimitiveExpression),

	Alias(AliasExpression),

	Cast(CastExpression),

	Constant(ConstantExpression),

	Column(ColumnExpression),

	Add(AddExpression),

	Div(DivExpression),

	Call(CallExpression),

	Rem(RemExpression),

	Mul(MulExpression),

	Sub(SubExpression),

	Tuple(TupleExpression),

	Prefix(PrefixExpression),

	GreaterThan(GreaterThanExpression),

	GreaterThanEqual(GreaterThanEqExpression),

	LessThan(LessThanExpression),

	LessThanEqual(LessThanEqExpression),

	Equal(EqExpression),

	NotEqual(NotEqExpression),

	Between(BetweenExpression),

	And(AndExpression),

	Or(OrExpression),

	Xor(XorExpression),

	In(InExpression),

	Type(TypeExpression),

	Parameter(ParameterExpression),
	Variable(VariableExpression),

	If(IfExpression),
	Map(MapExpression),
	Extend(ExtendExpression),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessPrimitiveExpression {
	pub column: ColumnIdentifier,
}

impl AccessPrimitiveExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		// For backward compatibility, merge primitive and column fragments
		match &self.column.primitive {
			ColumnPrimitive::Primitive {
				primitive,
				..
			} => Fragment::merge_all([primitive.clone(), self.column.name.clone()]),
			ColumnPrimitive::Alias(alias) => Fragment::merge_all([alias.clone(), self.column.name.clone()]),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstantExpression {
	None {
		fragment: Fragment,
	},
	Bool {
		fragment: Fragment,
	},
	// any number
	Number {
		fragment: Fragment,
	},
	// any textual representation can be String, Text, ...
	Text {
		fragment: Fragment,
	},
	// any temporal representation can be Date, Time, DateTime, ...
	Temporal {
		fragment: Fragment,
	},
}

impl Display for ConstantExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			ConstantExpression::None {
				..
			} => write!(f, "none"),
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
pub struct CastExpression {
	pub fragment: Fragment,
	pub expression: Box<Expression>,
	pub to: TypeExpression,
}

impl CastExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.fragment.clone(),
			self.expression.full_fragment_owned(),
			self.to.full_fragment_owned(),
		])
	}

	pub fn lazy_fragment(&self) -> impl Fn() -> Fragment {
		move || self.full_fragment_owned()
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeExpression {
	pub fragment: Fragment,
	pub ty: Type,
}

impl TypeExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		self.fragment.clone()
	}

	pub fn lazy_fragment(&self) -> impl Fn() -> Fragment {
		move || self.full_fragment_owned()
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DivExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MulExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GreaterThanExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl GreaterThanExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GreaterThanEqExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl GreaterThanEqExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LessThanExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl LessThanExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LessThanEqExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl LessThanEqExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EqExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl EqExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotEqExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl NotEqExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BetweenExpression {
	pub value: Box<Expression>,
	pub lower: Box<Expression>,
	pub upper: Box<Expression>,
	pub fragment: Fragment,
}

impl BetweenExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.value.full_fragment_owned(),
			self.fragment.clone(),
			self.lower.full_fragment_owned(),
			self.upper.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AndExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl AndExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl OrExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XorExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl XorExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InExpression {
	pub value: Box<Expression>,
	pub list: Box<Expression>,
	pub negated: bool,
	pub fragment: Fragment,
}

impl InExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.value.full_fragment_owned(),
			self.fragment.clone(),
			self.list.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnExpression(pub ColumnIdentifier);

impl ColumnExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		// Return just the column name for unqualified column references
		self.0.name.clone()
	}

	pub fn column(&self) -> &ColumnIdentifier {
		&self.0
	}
}

impl Display for Expression {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			Expression::AccessSource(AccessPrimitiveExpression {
				column,
			}) => match &column.primitive {
				ColumnPrimitive::Primitive {
					primitive,
					..
				} => {
					write!(f, "{}.{}", primitive.text(), column.name.text())
				}
				ColumnPrimitive::Alias(alias) => {
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
pub struct CallExpression {
	pub func: IdentExpression,
	pub args: Vec<Expression>,
	pub fragment: Fragment,
}

impl CallExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::Statement {
			column: self.func.0.column(),
			line: self.func.0.line(),
			text: Arc::from(format!(
				"{}({})",
				self.func.0.text(),
				self.args
					.iter()
					.map(|arg| arg.full_fragment_owned().text().to_string())
					.collect::<Vec<_>>()
					.join(",")
			)),
		}
	}
}

impl Display for CallExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let args = self.args.iter().map(|arg| format!("{}", arg)).collect::<Vec<_>>().join(", ");
		write!(f, "{}({})", self.func, args)
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentExpression(pub Fragment);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterExpression {
	Positional {
		fragment: Fragment,
	},
	Named {
		fragment: Fragment,
	},
}

impl ParameterExpression {
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
pub struct VariableExpression {
	pub fragment: Fragment,
}

impl VariableExpression {
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
pub struct IfExpression {
	pub condition: Box<Expression>,
	pub then_expr: Box<Expression>,
	pub else_ifs: Vec<ElseIfExpression>,
	pub else_expr: Option<Box<Expression>>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElseIfExpression {
	pub condition: Box<Expression>,
	pub then_expr: Box<Expression>,
	pub fragment: Fragment,
}

impl IfExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		self.fragment.clone()
	}

	pub fn lazy_fragment(&self) -> impl Fn() -> Fragment {
		move || self.full_fragment_owned()
	}
}

impl Display for IfExpression {
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

impl IdentExpression {
	pub fn name(&self) -> &str {
		self.0.text()
	}
}

impl Display for IdentExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0.text())
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PrefixOperator {
	Minus(Fragment),
	Plus(Fragment),
	Not(Fragment),
}

impl PrefixOperator {
	pub fn full_fragment_owned(&self) -> Fragment {
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
	pub fragment: Fragment,
}

impl PrefixExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([self.operator.full_fragment_owned(), self.expression.full_fragment_owned()])
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
	pub fragment: Fragment,
}

impl Display for TupleExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let items = self.expressions.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ");
		write!(f, "({})", items)
	}
}

pub struct ExpressionCompiler {}

impl ExpressionCompiler {
	pub fn compile(ast: Ast<'_>) -> crate::Result<Expression> {
		match ast {
			Ast::Literal(literal) => match literal {
				AstLiteral::Boolean(_) => Ok(Expression::Constant(ConstantExpression::Bool {
					fragment: literal.fragment().to_owned(),
				})),
				AstLiteral::Number(_) => Ok(Expression::Constant(ConstantExpression::Number {
					fragment: literal.fragment().to_owned(),
				})),
				AstLiteral::Temporal(_) => Ok(Expression::Constant(ConstantExpression::Temporal {
					fragment: literal.fragment().to_owned(),
				})),
				AstLiteral::Text(_) => Ok(Expression::Constant(ConstantExpression::Text {
					fragment: literal.fragment().to_owned(),
				})),
				AstLiteral::None(_) => Ok(Expression::Constant(ConstantExpression::None {
					fragment: literal.fragment().to_owned(),
				})),
			},
			Ast::Identifier(identifier) => {
				// Create an unqualified column identifier
				use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnPrimitive};
				use reifydb_type::fragment::Fragment;

				let column = ColumnIdentifier {
					primitive: ColumnPrimitive::Primitive {
						namespace: Fragment::Internal {
							text: Arc::from("_context"),
						},
						primitive: Fragment::Internal {
							text: Arc::from("_context"),
						},
					},
					name: identifier.token.fragment.to_owned(),
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
					let compiled = Self::compile(arg_ast)?;
					let compiled = match &compiled {
						Expression::Column(col_expr) => {
							if let Ok(ty) = Type::from_str(col_expr.0.name.text()) {
								Expression::Type(TypeExpression {
									fragment: col_expr.0.name.clone(),
									ty,
								})
							} else {
								compiled
							}
						}
						_ => compiled,
					};
					arg_expressions.push(compiled);
				}

				Ok(Expression::Call(CallExpression {
					func: IdentExpression(Fragment::testing(&full_name)),
					args: arg_expressions,
					fragment: call.token.fragment.to_owned(),
				}))
			}
			Ast::Infix(ast) => Self::infix(ast),
			Ast::Between(between) => {
				let value = Self::compile(BumpBox::into_inner(between.value))?;
				let lower = Self::compile(BumpBox::into_inner(between.lower))?;
				let upper = Self::compile(BumpBox::into_inner(between.upper))?;

				Ok(Expression::Between(BetweenExpression {
					value: Box::new(value),
					lower: Box::new(lower),
					upper: Box::new(upper),
					fragment: between.token.fragment.to_owned(),
				}))
			}
			Ast::Tuple(tuple) => {
				let mut expressions = Vec::with_capacity(tuple.len());

				for ast in tuple.nodes {
					expressions.push(Self::compile(ast)?);
				}

				Ok(Expression::Tuple(TupleExpression {
					expressions,
					fragment: tuple.token.fragment.to_owned(),
				}))
			}
			Ast::Prefix(prefix) => {
				let (fragment, operator) = match prefix.operator {
					ast::ast::AstPrefixOperator::Plus(token) => (
						token.fragment.to_owned(),
						PrefixOperator::Plus(token.fragment.to_owned()),
					),
					ast::ast::AstPrefixOperator::Negate(token) => (
						token.fragment.to_owned(),
						PrefixOperator::Minus(token.fragment.to_owned()),
					),
					ast::ast::AstPrefixOperator::Not(token) => (
						token.fragment.to_owned(),
						PrefixOperator::Not(token.fragment.to_owned()),
					),
				};

				Ok(Expression::Prefix(PrefixExpression {
					operator,
					expression: Box::new(Self::compile(BumpBox::into_inner(prefix.node))?),
					fragment,
				}))
			}
			Ast::Cast(node) => {
				let mut tuple = node.tuple;
				let node = tuple.nodes.pop().unwrap();
				let bump_fragment = node.as_identifier().token.fragment;
				let ty = convert_data_type(&bump_fragment)?;
				let fragment = bump_fragment.to_owned();

				let expr = tuple.nodes.pop().unwrap();

				Ok(Expression::Cast(CastExpression {
					fragment: tuple.token.fragment.to_owned(),
					expression: Box::new(Self::compile(expr)?),
					to: TypeExpression {
						fragment,
						ty,
					},
				}))
			}
			Ast::Variable(var) => Ok(Expression::Variable(VariableExpression {
				fragment: var.token.fragment.to_owned(),
			})),
			Ast::Rownum(_rownum) => {
				// Compile rownum to a column reference for rownum
				use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnPrimitive};
				use reifydb_type::{fragment::Fragment, value::row_number::ROW_NUMBER_COLUMN_NAME};

				let column = ColumnIdentifier {
					primitive: ColumnPrimitive::Primitive {
						namespace: Fragment::Internal {
							text: Arc::from("_context"),
						},
						primitive: Fragment::Internal {
							text: Arc::from("_context"),
						},
					},
					name: Fragment::Internal {
						text: Arc::from(ROW_NUMBER_COLUMN_NAME),
					},
				};
				Ok(Expression::Column(ColumnExpression(column)))
			}
			Ast::If(if_ast) => {
				// Compile condition
				let condition = Box::new(Self::compile(BumpBox::into_inner(if_ast.condition))?);

				// Compile then expression (take first expression from first statement in block)
				let then_expr = Box::new(Self::compile_block_as_expr(if_ast.then_block)?);

				// Compile else_if chains
				let mut else_ifs = Vec::new();
				for else_if in if_ast.else_ifs {
					let else_if_condition =
						Box::new(Self::compile(BumpBox::into_inner(else_if.condition))?);
					let else_if_then = Box::new(Self::compile_block_as_expr(else_if.then_block)?);
					else_ifs.push(ElseIfExpression {
						condition: else_if_condition,
						then_expr: else_if_then,
						fragment: else_if.token.fragment.to_owned(),
					});
				}

				// Compile optional else expression
				let else_expr = if let Some(else_block) = if_ast.else_block {
					Some(Box::new(Self::compile_block_as_expr(else_block)?))
				} else {
					None
				};

				Ok(Expression::If(IfExpression {
					condition,
					then_expr,
					else_ifs,
					else_expr,
					fragment: if_ast.token.fragment.to_owned(),
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
					fragment: map.token.fragment.to_owned(),
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
					fragment: extend.token.fragment.to_owned(),
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
					fragment: list.token.fragment.to_owned(),
				}))
			}
			ast => unimplemented!("{:?}", ast),
		}
	}

	/// Compile an AstBlock as a single expression.
	/// Takes the first expression from the first statement in the block.
	/// Used for IF/ELSE blocks in expression context.
	fn compile_block_as_expr(block: crate::ast::ast::AstBlock<'_>) -> crate::Result<Expression> {
		let fragment = block.token.fragment.to_owned();
		if let Some(first_stmt) = block.statements.into_iter().next() {
			if let Some(first_node) = first_stmt.nodes.into_iter().next() {
				return Self::compile(first_node);
			}
		}
		// Empty block → none
		Ok(Expression::Constant(ConstantExpression::None {
			fragment,
		}))
	}

	fn infix(ast: AstInfix<'_>) -> crate::Result<Expression> {
		match ast.operator {
			InfixOperator::Add(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::Add(AddExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::Divide(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::Div(DivExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::Subtract(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::Sub(SubExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::Rem(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::Rem(RemExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::Multiply(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::Mul(MulExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::Call(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;

				let Expression::Column(ColumnExpression(column)) = left else {
					panic!()
				};
				let Expression::Tuple(tuple) = right else {
					panic!()
				};

				Ok(Expression::Call(CallExpression {
					func: IdentExpression(column.name),
					args: tuple.expressions,
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::GreaterThan(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;

				Ok(Expression::GreaterThan(GreaterThanExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::GreaterThanEqual(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;

				Ok(Expression::GreaterThanEqual(GreaterThanEqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::LessThan(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;

				Ok(Expression::LessThan(LessThanExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::LessThanEqual(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;

				Ok(Expression::LessThanEqual(LessThanEqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::Equal(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;

				Ok(Expression::Equal(EqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::NotEqual(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;

				Ok(Expression::NotEqual(NotEqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::As(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let alias_fragment = match BumpBox::into_inner(ast.right) {
					Ast::Identifier(ident) => ident.token.fragment.to_owned(),
					Ast::Literal(AstLiteral::Text(text)) => {
						let raw = text.0.fragment.text();
						let unquoted = raw.trim_matches('"');
						Fragment::internal(unquoted)
					}
					_ => unimplemented!(),
				};

				Ok(Expression::Alias(AliasExpression {
					alias: IdentExpression(alias_fragment),
					expression: Box::new(left),
					fragment: token.fragment.to_owned(),
				}))
			}

			InfixOperator::And(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;

				Ok(Expression::And(AndExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}

			InfixOperator::Or(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;

				Ok(Expression::Or(OrExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}

			InfixOperator::Xor(token) => {
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let right = Self::compile(BumpBox::into_inner(ast.right))?;

				Ok(Expression::Xor(XorExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}

			InfixOperator::In(token) => {
				let value = Self::compile(BumpBox::into_inner(ast.left))?;
				let list = Self::compile(BumpBox::into_inner(ast.right))?;

				Ok(Expression::In(InExpression {
					value: Box::new(value),
					list: Box::new(list),
					negated: false,
					fragment: token.fragment.to_owned(),
				}))
			}

			InfixOperator::NotIn(token) => {
				let value = Self::compile(BumpBox::into_inner(ast.left))?;
				let list = Self::compile(BumpBox::into_inner(ast.right))?;

				Ok(Expression::In(InExpression {
					value: Box::new(value),
					list: Box::new(list),
					negated: true,
					fragment: token.fragment.to_owned(),
				}))
			}

			InfixOperator::Assign(token) => {
				// Assignment operator (=) is not valid in expression context
				// Use == for equality comparison
				use reifydb_type::error::diagnostic::ast as diag_ast;
				reifydb_type::return_error!(diag_ast::unsupported_token_error(
					token.fragment.to_owned()
				))
			}

			InfixOperator::TypeAscription(token) => {
				match BumpBox::into_inner(ast.left) {
					Ast::Identifier(alias) => {
						let right = Self::compile(BumpBox::into_inner(ast.right))?;

						Ok(Expression::Alias(AliasExpression {
							alias: IdentExpression(alias.token.fragment.to_owned()),
							expression: Box::new(right),
							fragment: token.fragment.to_owned(),
						}))
					}
					Ast::Literal(AstLiteral::Text(text)) => {
						// Handle string literals as alias names (common in MAP syntax)
						let right = Self::compile(BumpBox::into_inner(ast.right))?;

						Ok(Expression::Alias(AliasExpression {
							alias: IdentExpression(text.0.fragment.to_owned()),
							expression: Box::new(right),
							fragment: token.fragment.to_owned(),
						}))
					}
					_ => {
						use reifydb_type::{
							err, error::diagnostic::Diagnostic, fragment::Fragment,
						};
						return err!(Diagnostic {
							code: "EXPR_001".to_string(),
							statement: None,
							message: "Invalid alias expression".to_string(),
							column: None,
							fragment: Fragment::None,
							label: Some("Only identifiers and string literals can be used as alias names".to_string()),
							help: Some("Use an identifier or string literal for the alias name".to_string()),
							notes: vec![],
							cause: None,
							operator_chain: None,
						});
					}
				}
			}
			InfixOperator::AccessNamespace(_token) => {
				// Handle namespace access: `ns::func(args)` → CallExpression with namespaced name
				// Extract namespace name from left side (always an identifier)
				let left = Self::compile(BumpBox::into_inner(ast.left))?;
				let namespace = match &left {
					Expression::Column(ColumnExpression(col)) => col.name.text().to_string(),
					other => unimplemented!("unsupported namespace expression: {other:?}"),
				};

				// The right side may contain keywords (e.g. `undefined`) that should be
				// treated as identifiers in a namespace context. Extract the name from the
				// raw AST token before compiling, so keywords are treated as identifier_or_keyword.
				let right_ast = BumpBox::into_inner(ast.right);
				Self::compile_namespace_right(&namespace, right_ast)
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

	/// Compile the right-hand side of a namespace access (`ns::...`).
	///
	/// Keywords like `undefined` or `true` are treated as identifiers in this
	/// context so that `is::none(x)` resolves to a function call rather
	/// than parsing `undefined` as the literal keyword.
	fn compile_namespace_right(namespace: &str, right_ast: Ast<'_>) -> crate::Result<Expression> {
		// Helper: extract a token's text from any AST node that should be treated
		// as an identifier_or_keyword in namespace position.
		fn identifier_or_keyword_name(ast: &Ast<'_>) -> Option<String> {
			Some(ast.token().fragment.text().to_string())
		}

		match right_ast {
			// ns::func(args)  where func is parsed as Infix(left, Call, right)
			Ast::Infix(infix) if matches!(infix.operator, InfixOperator::Call(_)) => {
				let func_name = identifier_or_keyword_name(&infix.left)
					.expect("namespace function name must be extractable");
				let full_name = format!("{}::{}", namespace, func_name);

				let right = Self::compile(BumpBox::into_inner(infix.right))?;
				let Expression::Tuple(tuple) = right else {
					panic!("expected tuple arguments for namespaced call");
				};

				Ok(Expression::Call(CallExpression {
					func: IdentExpression(Fragment::testing(&full_name)),
					args: tuple.expressions,
					fragment: infix.token.fragment.to_owned(),
				}))
			}
			// ns::func(args) where func is parsed as CallFunction
			// (happens when the namespace token is a keyword like `is`,
			// so the parser treats the right side as a standalone call)
			Ast::CallFunction(call) => {
				let func_name = call.function.name.text().to_string();
				let full_name = if call.function.namespaces.is_empty() {
					format!("{}::{}", namespace, func_name)
				} else {
					let sub_ns = call
						.function
						.namespaces
						.iter()
						.map(|ns| ns.text())
						.collect::<Vec<_>>()
						.join("::");
					format!("{}::{}::{}", namespace, sub_ns, func_name)
				};

				let mut arg_expressions = Vec::new();
				for arg_ast in call.arguments.nodes {
					let compiled = Self::compile(arg_ast)?;
					let compiled = match &compiled {
						Expression::Column(col_expr) => {
							if let Ok(ty) = Type::from_str(col_expr.0.name.text()) {
								Expression::Type(TypeExpression {
									fragment: col_expr.0.name.clone(),
									ty,
								})
							} else {
								compiled
							}
						}
						_ => compiled,
					};
					arg_expressions.push(compiled);
				}

				Ok(Expression::Call(CallExpression {
					func: IdentExpression(Fragment::testing(&full_name)),
					args: arg_expressions,
					fragment: call.token.fragment.to_owned(),
				}))
			}
			// ns::name  (bare namespaced reference, no call)
			other => {
				if let Some(name) = identifier_or_keyword_name(&other) {
					let full_name = format!("{}::{}", namespace, name);
					Ok(Expression::Column(ColumnExpression(ColumnIdentifier {
						primitive: ColumnPrimitive::Primitive {
							namespace: Fragment::Internal {
								text: Arc::from("_context"),
							},
							primitive: Fragment::Internal {
								text: Arc::from("_context"),
							},
						},
						name: Fragment::testing(&full_name),
					})))
				} else {
					let compiled = Self::compile(other)?;
					match compiled {
						Expression::Column(ColumnExpression(col)) => {
							let full_name = format!("{}::{}", namespace, col.name.text());
							Ok(Expression::Column(ColumnExpression(ColumnIdentifier {
								primitive: col.primitive,
								name: Fragment::testing(&full_name),
							})))
						}
						other => unimplemented!(
							"unsupported namespace right-hand side: {other:?}"
						),
					}
				}
			}
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapExpression {
	pub expressions: Vec<Expression>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendExpression {
	pub expressions: Vec<Expression>,
	pub fragment: Fragment,
}
