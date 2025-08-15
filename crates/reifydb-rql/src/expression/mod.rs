// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	OwnedSpan,
	interface::expression::{
		AccessSourceExpression, AddExpression, AliasExpression,
		AndExpression, BetweenExpression, CallExpression,
		CastExpression, ColumnExpression, ConstantExpression,
		DivExpression, EqualExpression, Expression,
		GreaterThanEqualExpression, GreaterThanExpression,
		IdentExpression, LessThanEqualExpression, LessThanExpression,
		MulExpression, NotEqualExpression, OrExpression,
		ParameterExpression, PrefixExpression, PrefixOperator,
		RemExpression, SubExpression, TupleExpression, TypeExpression,
		XorExpression,
	},
};

use crate::{
	ast,
	ast::{
		Ast, AstInfix, AstLiteral, InfixOperator, lex::ParameterKind,
		parse,
	},
	convert_data_type,
};

pub fn parse_expression(rql: &str) -> crate::Result<Vec<Expression>> {
	let statements = parse(rql)?;
	if statements.is_empty() {
		return Ok(vec![]);
	}

	let mut result = Vec::new();
	for statement in statements {
		for ast in statement.0 {
			result.push(ExpressionCompiler::compile(ast)?);
		}
	}

	Ok(result)
}

pub struct ExpressionCompiler {}

impl ExpressionCompiler {
	pub fn compile(ast: Ast) -> crate::Result<Expression> {
		match ast {
            Ast::Literal(literal) => match literal {
                AstLiteral::Boolean(_) => {
                    Ok(Expression::Constant(ConstantExpression::Bool { span: literal.span() }))
                }
                AstLiteral::Number(_) => {
                    Ok(Expression::Constant(ConstantExpression::Number { span: literal.span() }))
                }
                AstLiteral::Temporal(_) => {
                    Ok(Expression::Constant(ConstantExpression::Temporal { span: literal.span() }))
                }
                AstLiteral::Text(_) => {
                    Ok(Expression::Constant(ConstantExpression::Text { span: literal.span() }))
                }
                AstLiteral::Undefined(_) => {
                    Ok(Expression::Constant(ConstantExpression::Undefined { span: literal.span() }))
                }
            },
            Ast::Identifier(identifier) => {
                Ok(Expression::Column(ColumnExpression(identifier.span())))
            }
            Ast::CallFunction(call) => {
                // Build the full function name from namespace + function
                let full_name = if call.namespaces.is_empty() {
                    call.function.value().to_string()
                } else {
                    let namespace_path =
                        call.namespaces.iter().map(|id| id.value()).collect::<Vec<_>>().join("::");
                    format!("{}::{}", namespace_path, call.function.value())
                };

                // Compile arguments
                let mut arg_expressions = Vec::new();
                for arg_ast in call.arguments.nodes {
                    arg_expressions.push(Self::compile(arg_ast)?);
                }

                Ok(Expression::Call(CallExpression {
                    func: IdentExpression(OwnedSpan::testing(&full_name)),
                    args: arg_expressions,
                    span: call.token.span,
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
                    span: between.token.span,
                }))
            }
            Ast::Tuple(tuple) => {
                let mut expressions = Vec::with_capacity(tuple.len());

                for ast in tuple.nodes {
                    expressions.push(Self::compile(ast)?);
                }

                Ok(Expression::Tuple(TupleExpression { expressions, span: tuple.token.span }))
            }
            Ast::Prefix(prefix) => {
                let (span, operator) = match prefix.operator {
                    ast::AstPrefixOperator::Plus(token) => {
                        (token.span.clone(), PrefixOperator::Plus(token.span))
                    }
                    ast::AstPrefixOperator::Negate(token) => {
                        (token.span.clone(), PrefixOperator::Minus(token.span))
                    }
                    ast::AstPrefixOperator::Not(token) => {
                        (token.span.clone(), PrefixOperator::Not(token.span))
                    }
                };

                Ok(Expression::Prefix(PrefixExpression {
                    operator,
                    expression: Box::new(Self::compile(*prefix.node)?),
                    span,
                }))
            }
            Ast::Cast(node) => {
                let mut tuple = node.tuple;
                let node = tuple.nodes.pop().unwrap();
                let node = node.as_identifier();
                let span = node.span.clone();
                let ty = convert_data_type(node)?;

                let expr = tuple.nodes.pop().unwrap();

                Ok(Expression::Cast(CastExpression {
                    span: tuple.token.span,
                    expression: Box::new(Self::compile(expr)?),
                    to: TypeExpression { span, ty },
                }))
            }
            Ast::ParameterRef(param) => {
                match param.kind {
                    ParameterKind::Positional(_) => {
                        Ok(Expression::Parameter(ParameterExpression::Positional {
                            span: param.token.span,
                        }))
                    }
                    ParameterKind::Named => {
                        Ok(Expression::Parameter(ParameterExpression::Named {
                            span: param.token.span,
                        }))
                    }
                }
            }
            ast => unimplemented!("{:?}", ast),
        }
	}

	fn infix(ast: AstInfix) -> crate::Result<Expression> {
		match ast.operator {
			InfixOperator::AccessTable(_) => {
				let Ast::Identifier(left) = *ast.left else {
					unimplemented!()
				};
				let Ast::Identifier(right) = *ast.right else {
					unimplemented!()
				};

				Ok(Expression::AccessSource(
					AccessSourceExpression {
						source: left.span(),
						column: right.span(),
					},
				))
			}

			InfixOperator::Add(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;
				Ok(Expression::Add(AddExpression {
					left: Box::new(left),
					right: Box::new(right),
					span: token.span,
				}))
			}
			InfixOperator::Divide(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;
				Ok(Expression::Div(DivExpression {
					left: Box::new(left),
					right: Box::new(right),
					span: token.span,
				}))
			}
			InfixOperator::Subtract(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;
				Ok(Expression::Sub(SubExpression {
					left: Box::new(left),
					right: Box::new(right),
					span: token.span,
				}))
			}
			InfixOperator::Rem(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;
				Ok(Expression::Rem(RemExpression {
					left: Box::new(left),
					right: Box::new(right),
					span: token.span,
				}))
			}
			InfixOperator::Multiply(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;
				Ok(Expression::Mul(MulExpression {
					left: Box::new(left),
					right: Box::new(right),
					span: token.span,
				}))
			}
			InfixOperator::Call(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				let Expression::Column(ColumnExpression(span)) =
					left
				else {
					panic!()
				};
				let Expression::Tuple(tuple) = right else {
					panic!()
				};

				Ok(Expression::Call(CallExpression {
					func: IdentExpression(span),
					args: tuple.expressions,
					span: token.span,
				}))
			}
			InfixOperator::GreaterThan(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::GreaterThan(
					GreaterThanExpression {
						left: Box::new(left),
						right: Box::new(right),
						span: token.span,
					},
				))
			}
			InfixOperator::GreaterThanEqual(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::GreaterThanEqual(
					GreaterThanEqualExpression {
						left: Box::new(left),
						right: Box::new(right),
						span: token.span,
					},
				))
			}
			InfixOperator::LessThan(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::LessThan(LessThanExpression {
					left: Box::new(left),
					right: Box::new(right),
					span: token.span,
				}))
			}
			InfixOperator::LessThanEqual(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::LessThanEqual(
					LessThanEqualExpression {
						left: Box::new(left),
						right: Box::new(right),
						span: token.span,
					},
				))
			}
			InfixOperator::Equal(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::Equal(EqualExpression {
					left: Box::new(left),
					right: Box::new(right),
					span: token.span,
				}))
			}
			InfixOperator::NotEqual(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::NotEqual(NotEqualExpression {
					left: Box::new(left),
					right: Box::new(right),
					span: token.span,
				}))
			}
			InfixOperator::As(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::Alias(AliasExpression {
					alias: IdentExpression(right.span()),
					expression: Box::new(left),
					span: token.span,
				}))
			}

			InfixOperator::And(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::And(AndExpression {
					left: Box::new(left),
					right: Box::new(right),
					span: token.span,
				}))
			}

			InfixOperator::Or(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::Or(OrExpression {
					left: Box::new(left),
					right: Box::new(right),
					span: token.span,
				}))
			}

			InfixOperator::Xor(token) => {
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::Xor(XorExpression {
					left: Box::new(left),
					right: Box::new(right),
					span: token.span,
				}))
			}

			InfixOperator::Assign(token) => {
				// Treat = as == for equality comparison in
				// expressions
				let left = Self::compile(*ast.left)?;
				let right = Self::compile(*ast.right)?;

				Ok(Expression::Equal(EqualExpression {
					left: Box::new(left),
					right: Box::new(right),
					span: token.span,
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
