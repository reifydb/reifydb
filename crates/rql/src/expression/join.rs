// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::query::unsupported_source_qualification,
	interface::identifier::{ColumnIdentifier, ColumnShape},
};
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	Result,
	ast::ast::{Ast, AstInfix, AstPrefixOperator, InfixOperator},
	bump::BumpBox,
	expression::{
		AccessShapeExpression, AddExpression, AndExpression, DivExpression, EqExpression, Expression,
		ExpressionCompiler, GreaterThanEqExpression, GreaterThanExpression, LessThanEqExpression,
		LessThanExpression, MulExpression, NotEqExpression, OrExpression, PrefixExpression, PrefixOperator,
		RemExpression, SubExpression, TupleExpression, XorExpression,
	},
};

pub struct JoinConditionCompiler {
	alias: Option<Fragment>,
}

impl JoinConditionCompiler {
	pub fn new(alias: Option<Fragment>) -> Self {
		Self {
			alias,
		}
	}

	pub fn compile(&self, ast: Ast<'_>) -> Result<Expression> {
		match ast {
			Ast::Infix(ast_infix) if matches!(ast_infix.operator, InfixOperator::AccessTable(_)) => {
				self.compile_qualified_column(ast_infix)
			}

			Ast::Infix(ast_infix) => self.compile_infix(ast_infix),

			Ast::Tuple(tuple) => {
				let mut expressions = Vec::with_capacity(tuple.len());
				for ast in tuple.nodes {
					expressions.push(self.compile(ast)?);
				}

				if expressions.len() == 1 {
					Ok(expressions.into_iter().next().unwrap())
				} else {
					Ok(Expression::Tuple(TupleExpression {
						expressions,
						fragment: tuple.token.fragment.to_owned(),
					}))
				}
			}

			Ast::Prefix(prefix) => {
				let inner = self.compile(BumpBox::into_inner(prefix.node))?;
				let (fragment, operator) = match prefix.operator {
					AstPrefixOperator::Plus(token) => (
						token.fragment.to_owned(),
						PrefixOperator::Plus(token.fragment.to_owned()),
					),
					AstPrefixOperator::Negate(token) => (
						token.fragment.to_owned(),
						PrefixOperator::Minus(token.fragment.to_owned()),
					),
					AstPrefixOperator::Not(token) => (
						token.fragment.to_owned(),
						PrefixOperator::Not(token.fragment.to_owned()),
					),
				};

				Ok(Expression::Prefix(PrefixExpression {
					expression: Box::new(inner),
					operator,
					fragment,
				}))
			}

			_ => ExpressionCompiler::compile(ast),
		}
	}

	fn compile_qualified_column(&self, ast: AstInfix<'_>) -> Result<Expression> {
		assert!(matches!(ast.operator, InfixOperator::AccessTable(_)));

		let Ast::Identifier(left) = BumpBox::into_inner(ast.left) else {
			unimplemented!("Expected identifier on left side of column qualification");
		};
		let Ast::Identifier(right) = BumpBox::into_inner(ast.right) else {
			unimplemented!("Expected identifier on right side of column qualification");
		};

		if let Some(ref alias) = self.alias
			&& left.token.fragment.text() == alias.text()
		{
			let column = ColumnIdentifier {
				shape: ColumnShape::Alias(alias.clone()),
				name: right.token.fragment.to_owned(),
			};
			return Ok(Expression::AccessSource(AccessShapeExpression {
				column,
			}));
		}

		return_error!(unsupported_source_qualification(
			left.token.fragment.to_owned(),
			left.token.fragment.text()
		))
	}

	fn compile_infix(&self, ast: AstInfix<'_>) -> Result<Expression> {
		match ast.operator {
			InfixOperator::AccessTable(_) => self.compile_qualified_column(ast),
			InfixOperator::Add(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::Add(AddExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::Divide(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::Div(DivExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::Multiply(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::Mul(MulExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::Rem(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::Rem(RemExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::Subtract(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::Sub(SubExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::Equal(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::Equal(EqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::NotEqual(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::NotEqual(NotEqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::LessThan(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::LessThan(LessThanExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::LessThanEqual(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::LessThanEqual(LessThanEqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::GreaterThan(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::GreaterThan(GreaterThanExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::GreaterThanEqual(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::GreaterThanEqual(GreaterThanEqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::And(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::And(AndExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::Or(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::Or(OrExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			InfixOperator::Xor(token) => {
				let left = self.compile(BumpBox::into_inner(ast.left))?;
				let right = self.compile(BumpBox::into_inner(ast.right))?;
				Ok(Expression::Xor(XorExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment.to_owned(),
				}))
			}
			_ => ExpressionCompiler::compile(Ast::Infix(ast)),
		}
	}
}
