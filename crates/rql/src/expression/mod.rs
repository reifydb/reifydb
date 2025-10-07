// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod join;
mod name;

pub use join::JoinConditionCompiler;
pub use name::*;
use reifydb_core::interface::evaluate::expression::{
	AddExpression, AliasExpression, AndExpression, BetweenExpression, CallExpression, CastExpression,
	ColumnExpression, ConstantExpression, DivExpression, EqExpression, Expression, GreaterThanEqExpression,
	GreaterThanExpression, IdentExpression, LessThanEqExpression, LessThanExpression, MulExpression,
	NotEqExpression, OrExpression, PrefixExpression, PrefixOperator, RemExpression, SubExpression, TupleExpression,
	TypeExpression, VariableExpression, XorExpression,
};
use reifydb_type::{Fragment, OwnedFragment};

use crate::{
	ast,
	ast::{Ast, AstInfix, AstLiteral, InfixOperator, parse_str},
	convert_data_type,
};

pub fn parse_expression(rql: &str) -> crate::Result<Vec<Expression>> {
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
				let Ast::Identifier(alias) = *ast.left else {
					unimplemented!()
				};

				let right = Self::compile(*ast.right)?;

				Ok(Expression::Alias(AliasExpression {
					alias: IdentExpression(alias.token.fragment),
					expression: Box::new(right),
					fragment: token.fragment,
				}))
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
