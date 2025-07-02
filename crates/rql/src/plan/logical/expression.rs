// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast;
use crate::ast::{Ast, AstInfix, AstLiteral, InfixOperator};
use crate::expression::{
    AccessTableExpression, AddExpression, AliasExpression, CallExpression, CastExpression,
    ColumnExpression, ConstantExpression, DivideExpression, EqualExpression, Expression,
    GreaterThanEqualExpression, GreaterThanExpression, IdentExpression, KindExpression,
    LessThanEqualExpression, LessThanExpression, ModuloExpression, MultiplyExpression,
    NotEqualExpression, PrefixExpression, PrefixOperator, SubtractExpression, TupleExpression,
};
use crate::plan::logical::Compiler;

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
                AstLiteral::Text(_) => {
                    Ok(Expression::Constant(ConstantExpression::Text { span: literal.span() }))
                }
                _ => unimplemented!(),
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
                    ast::AstPrefixOperator::Not(_token) => unimplemented!(),
                };

                Ok(Expression::Prefix(PrefixExpression {
                    operator,
                    expression: Box::new(Self::compile_expression(*prefix.node)?),
                    span,
                }))
            }
            Ast::Cast(node) => {
                let mut tuple = node.tuple;
                let ast_kind = tuple.nodes.pop().unwrap();
                let expr = tuple.nodes.pop().unwrap();
                let kind = ast_kind.as_kind().kind();
                let span = ast_kind.as_kind().token().span.clone();

                Ok(Expression::Cast(CastExpression {
                    span: node.token.span,
                    expression: Box::new(Self::compile_expression(expr)?),
                    to: KindExpression { span, kind },
                }))
            }
            Ast::Kind(node) => Ok(Expression::Kind(KindExpression {
                span: node.token().span.clone(),
                kind: node.kind(),
            })),
            ast => unimplemented!("{:?}", ast),
        }
    }

    pub(crate) fn compile_expression_infix(ast: AstInfix) -> crate::Result<Expression> {
        match ast.operator {
            InfixOperator::AccessTable(token) => {
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
                Ok(Expression::Divide(DivideExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Subtract(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;
                Ok(Expression::Subtract(SubtractExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Modulo(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;
                Ok(Expression::Modulo(ModuloExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Multiply(token) => {
                let left = Self::compile_expression(*ast.left)?;
                let right = Self::compile_expression(*ast.right)?;
                Ok(Expression::Multiply(MultiplyExpression {
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

            operator => unimplemented!("not implemented: {operator:?}"),
            // InfixOperator::Arrow(_) => {}
            // InfixOperator::AccessPackage(_) => {}
            // InfixOperator::Assign(_) => {}
            // InfixOperator::Subtract(_) => {}
            // InfixOperator::Multiply(_) => {}
            // InfixOperator::Divide(_) => {}
            // InfixOperator::Modulo(_) => {}
            // InfixOperator::TypeAscription(_) => {}
        }
    }
}
