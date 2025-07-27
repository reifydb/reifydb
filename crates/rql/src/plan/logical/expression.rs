// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast;
use crate::ast::{Ast, AstInfix, AstLiteral, InfixOperator};
use crate::expression::{
    AccessTableExpression, AddExpression, AliasExpression, AndExpression, CallExpression, CastExpression,
    ColumnExpression, ConstantExpression, DataTypeExpression, DivExpression, EqualExpression,
    Expression, GreaterThanEqualExpression, GreaterThanExpression, IdentExpression,
    LessThanEqualExpression, LessThanExpression, MulExpression, NotEqualExpression,
    OrExpression, PrefixExpression, PrefixOperator, RemExpression, SubExpression, TupleExpression,
    XorExpression,
};
use crate::plan::logical::{Compiler, convert_data_type};

impl Compiler {
    pub(crate) fn compile_expression(ast: Ast) -> crate::Result<Expression> {
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
            Ast::Infix(ast) => Self::compile_expression_infix(ast),
            Ast::Tuple(tuple) => {
                let mut expressions = Vec::with_capacity(tuple.len());

                for ast in tuple.nodes {
                    expressions.push(Self::compile_expression(ast)?);
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
                    expression: Box::new(Self::compile_expression(*prefix.node)?),
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
                    expression: Box::new(Self::compile_expression(expr)?),
                    to: DataTypeExpression { span, ty },
                }))
            }
            ast => unimplemented!("{:?}", ast),
        }
    }

    pub(crate) fn compile_expression_infix(ast: AstInfix) -> crate::Result<Expression> {
        match ast.operator {
            InfixOperator::AccessTable(_) => {
                let Ast::Identifier(left) = *ast.left else { unimplemented!() };
                let Ast::Identifier(right) = *ast.right else { unimplemented!() };

                Ok(Expression::AccessTable(AccessTableExpression {
                    table: left.span(),
                    column: right.span(),
                }))
            }

            InfixOperator::Add(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;
                Ok(Expression::Add(AddExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Divide(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;
                Ok(Expression::Div(DivExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Subtract(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;
                Ok(Expression::Sub(SubExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Rem(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;
                Ok(Expression::Rem(RemExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Multiply(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;
                Ok(Expression::Mul(MulExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Call(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;

                let Expression::Column(ColumnExpression(span)) = left else { panic!() };
                let Expression::Tuple(tuple) = right else { panic!() };

                Ok(Expression::Call(CallExpression {
                    func: IdentExpression(span),
                    args: tuple.expressions,
                    span: token.span,
                }))
            }
            InfixOperator::GreaterThan(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;

                Ok(Expression::GreaterThan(GreaterThanExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::GreaterThanEqual(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;

                Ok(Expression::GreaterThanEqual(GreaterThanEqualExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::LessThan(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;

                Ok(Expression::LessThan(LessThanExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::LessThanEqual(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;

                Ok(Expression::LessThanEqual(LessThanEqualExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Equal(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;

                Ok(Expression::Equal(EqualExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::NotEqual(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;

                Ok(Expression::NotEqual(NotEqualExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::As(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;

                Ok(Expression::Alias(AliasExpression {
                    alias: IdentExpression(right.span()),
                    expression: Box::new(left),
                    span: token.span,
                }))
            }

            InfixOperator::And(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;

                Ok(Expression::And(AndExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }

            InfixOperator::Or(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;

                Ok(Expression::Or(OrExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }

            InfixOperator::Xor(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;

                Ok(Expression::Xor(XorExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }

            operator => unimplemented!("not implemented: {operator:?}"),
            // InfixOperator::Arrow(_) => {}
            // InfixOperator::AccessPackage(_) => {}
            // InfixOperator::Assign(_) => {}
            // InfixOperator::Subtract(_) => {}
            // InfixOperator::Multiply(_) => {}
            // InfixOperator::Divide(_) => {}
            // InfixOperator::Rem(_) => {}
            // InfixOperator::TypeAscription(_) => {}
        }
    }
}
