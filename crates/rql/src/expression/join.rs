// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::return_error;
use reifydb_type::{Fragment, diagnostic::query::unsupported_source_qualification};

use crate::{
	ast::{Ast, AstInfix, InfixOperator},
	expression::{
		AccessSourceExpression, AddExpression, AndExpression, DivExpression, EqExpression, Expression,
		ExpressionCompiler, GreaterThanEqExpression, GreaterThanExpression, LessThanEqExpression,
		LessThanExpression, MulExpression, NotEqExpression, OrExpression, PrefixExpression, PrefixOperator,
		RemExpression, SubExpression, TupleExpression, XorExpression,
	},
};

/// Compiles join conditions with proper alias scoping
/// The alias (if present) is only valid within the ON clause
pub struct JoinConditionCompiler {
	/// The alias for the other side of the join (if any)
	alias: Option<Fragment>,
}

impl JoinConditionCompiler {
	pub fn new(alias: Option<Fragment>) -> Self {
		Self {
			alias,
		}
	}

	/// Compile a join condition expression
	/// This handles the special case where alias.column references are valid
	pub fn compile(&self, ast: Ast) -> crate::Result<Expression> {
		match ast {
			// Handle alias.column references in join conditions
			Ast::Infix(ast_infix) if matches!(ast_infix.operator, InfixOperator::AccessTable(_)) => {
				self.compile_qualified_column(ast_infix)
			}
			// For all other expressions, delegate to the standard compiler
			// but recursively handle any infix operations that might contain qualified columns
			Ast::Infix(ast_infix) => self.compile_infix(ast_infix),
			// Handle tuples (parenthesized expressions) - need to recursively compile with
			// JoinConditionCompiler
			Ast::Tuple(tuple) => {
				let mut expressions = Vec::with_capacity(tuple.len());
				for ast in tuple.nodes {
					expressions.push(self.compile(ast)?);
				}
				// If it's a single expression in parentheses, just return that expression
				if expressions.len() == 1 {
					Ok(expressions.into_iter().next().unwrap())
				} else {
					// Multiple expressions in a tuple
					Ok(Expression::Tuple(TupleExpression {
						expressions,
						fragment: tuple.token.fragment,
					}))
				}
			}
			// Handle prefix operators (!, -, +) - need to recursively compile the inner expression
			Ast::Prefix(prefix) => {
				use crate::ast::AstPrefixOperator;

				let inner = self.compile(*prefix.node)?;
				let (fragment, operator) = match prefix.operator {
					AstPrefixOperator::Plus(token) => {
						(token.fragment.clone(), PrefixOperator::Plus(token.fragment))
					}
					AstPrefixOperator::Negate(token) => {
						(token.fragment.clone(), PrefixOperator::Minus(token.fragment))
					}
					AstPrefixOperator::Not(token) => {
						(token.fragment.clone(), PrefixOperator::Not(token.fragment))
					}
				};

				Ok(Expression::Prefix(PrefixExpression {
					expression: Box::new(inner),
					operator,
					fragment,
				}))
			}
			// All other AST nodes compile normally
			_ => ExpressionCompiler::compile(ast),
		}
	}

	fn compile_qualified_column(&self, ast: AstInfix) -> crate::Result<Expression> {
		assert!(matches!(ast.operator, InfixOperator::AccessTable(_)));

		let Ast::Identifier(left) = *ast.left else {
			unimplemented!("Expected identifier on left side of column qualification");
		};
		let Ast::Identifier(right) = *ast.right else {
			unimplemented!("Expected identifier on right side of column qualification");
		};

		use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnSource};

		// Check if this is referencing the join alias
		if let Some(ref alias) = self.alias {
			if left.token.fragment.text() == alias.text() {
				// This is a reference to the right side via alias
				let column = ColumnIdentifier {
					source: ColumnSource::Alias(alias.clone()),
					name: right.token.fragment,
				};
				return Ok(Expression::AccessSource(AccessSourceExpression {
					column,
				}));
			}
		}

		// Otherwise, this is an error - we don't support table qualification in the new design
		// except for the join alias
		return_error!(unsupported_source_qualification(left.token.fragment.clone(), left.token.fragment.text()))
	}

	fn compile_infix(&self, ast: AstInfix) -> crate::Result<Expression> {
		match ast.operator {
			InfixOperator::AccessTable(_) => self.compile_qualified_column(ast),
			InfixOperator::Add(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::Add(AddExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::Divide(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::Div(DivExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::Multiply(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::Mul(MulExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::Rem(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::Rem(RemExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::Subtract(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::Sub(SubExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::Equal(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::Equal(EqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::NotEqual(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::NotEqual(NotEqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::LessThan(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::LessThan(LessThanExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::LessThanEqual(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::LessThanEqual(LessThanEqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::GreaterThan(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::GreaterThan(GreaterThanExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::GreaterThanEqual(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::GreaterThanEqual(GreaterThanEqExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::And(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::And(AndExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::Or(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::Or(OrExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			InfixOperator::Xor(token) => {
				let left = self.compile(*ast.left)?;
				let right = self.compile(*ast.right)?;
				Ok(Expression::Xor(XorExpression {
					left: Box::new(left),
					right: Box::new(right),
					fragment: token.fragment,
				}))
			}
			_ => {
				// For any other operators, use the standard expression compiler
				ExpressionCompiler::compile(Ast::Infix(ast))
			}
		}
	}
}
