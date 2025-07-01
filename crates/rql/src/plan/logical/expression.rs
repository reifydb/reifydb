// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::{Ast, AstInfix, InfixOperator};
use crate::expression::{
    AccessTableExpression, AddExpression, AliasExpression, CallExpression, ColumnExpression,
    DivideExpression, EqualExpression, Expression, GreaterThanEqualExpression,
    GreaterThanExpression, IdentExpression, LessThanEqualExpression, LessThanExpression,
    ModuloExpression, MultiplyExpression, NotEqualExpression, SubtractExpression,
};
use crate::plan::logical::Compiler;

impl Compiler {
    pub(crate) fn compile_expression(&self, ast: Ast) -> crate::Result<Expression> {
        match ast {
            Ast::Infix(ast) => self.compile_expression_infix(ast),
            ast => unimplemented!("{:?}", ast),
        }
    }

    pub(crate) fn compile_expression_infix(&self, ast: AstInfix) -> crate::Result<Expression> {
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
                let left = self.compile_expression(*ast.left)?;
                let right = self.compile_expression(*ast.right)?;
                Ok(Expression::Add(AddExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Divide(token) => {
                let left = self.compile_expression(*ast.left)?;
                let right = self.compile_expression(*ast.right)?;
                Ok(Expression::Divide(DivideExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Subtract(token) => {
                let left = self.compile_expression(*ast.left)?;
                let right = self.compile_expression(*ast.right)?;
                Ok(Expression::Subtract(SubtractExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Modulo(token) => {
                let left = self.compile_expression(*ast.left)?;
                let right = self.compile_expression(*ast.right)?;
                Ok(Expression::Modulo(ModuloExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Multiply(token) => {
                let left = self.compile_expression(*ast.left)?;
                let right = self.compile_expression(*ast.right)?;
                Ok(Expression::Multiply(MultiplyExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Call(token) => {
                let left = self.compile_expression(*ast.left)?;
                let right = self.compile_expression(*ast.right)?;

                let Expression::Column(ColumnExpression(span)) = left else { panic!() };
                let Expression::Tuple(tuple) = right else { panic!() };

                Ok(Expression::Call(CallExpression {
                    func: IdentExpression(span),
                    args: tuple.expressions,
                    span: token.span,
                }))
            }
            InfixOperator::GreaterThan(token) => {
                let left = self.compile_expression(*ast.left)?;
                let right = self.compile_expression(*ast.right)?;

                Ok(Expression::GreaterThan(GreaterThanExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::GreaterThanEqual(token) => {
                let left = self.compile_expression(*ast.left)?;
                let right = self.compile_expression(*ast.right)?;

                Ok(Expression::GreaterThanEqual(GreaterThanEqualExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::LessThan(token) => {
                let left = self.compile_expression(*ast.left)?;
                let right = self.compile_expression(*ast.right)?;

                Ok(Expression::LessThan(LessThanExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::LessThanEqual(token) => {
                let left = self.compile_expression(*ast.left)?;
                let right = self.compile_expression(*ast.right)?;

                Ok(Expression::LessThanEqual(LessThanEqualExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Equal(token) => {
                let left = self.compile_expression(*ast.left)?;
                let right = self.compile_expression(*ast.right)?;

                Ok(Expression::Equal(EqualExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::NotEqual(token) => {
                let left = self.compile_expression(*ast.left)?;
                let right = self.compile_expression(*ast.right)?;

                Ok(Expression::NotEqual(NotEqualExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::As(token) => {
                let left = self.compile_expression(*ast.left)?;
                let right = self.compile_expression(*ast.right)?;

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
