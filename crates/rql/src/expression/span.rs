// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

use crate::expression::{
    AddExpression, CastExpression, ConstantExpression, DivideExpression, Expression,
    ModuloExpression, MultiplyExpression, PrefixExpression, SubtractExpression,
};
use reifydb_core::Span;

impl Expression {
    pub fn lazy_span(&self) -> impl Fn() -> Span + '_ {
        move || match self {
            Expression::AccessTable(expr) => expr.span(),
            Expression::Alias(expr) => expr.expression.span(),
            Expression::Cast(CastExpression { expression: expr, .. }) => expr.span(),
            Expression::Constant(expr) => match expr {
                ConstantExpression::Undefined { span }
                | ConstantExpression::Bool { span }
                | ConstantExpression::Number { span }
                | ConstantExpression::Text { span } => span.clone(),
            },
            Expression::Column(expr) => expr.0.clone(),

            Expression::Add(expr) => expr.span(),
            Expression::Subtract(expr) => expr.span(),
            Expression::GreaterThan(expr) => expr.span.clone(),
            Expression::GreaterThanEqual(expr) => expr.span.clone(),
            Expression::LessThan(expr) => expr.span.clone(),
            Expression::LessThanEqual(expr) => expr.span.clone(),
            Expression::Equal(expr) => expr.span.clone(),
            Expression::NotEqual(expr) => expr.span.clone(),

            Expression::Multiply(expr) => expr.span(),
            Expression::Divide(expr) => expr.span(),
            Expression::Modulo(expr) => expr.span(),

            Expression::Tuple(_expr) => {
                // let spans = expr.elements.iter().map(|e| e.span()).collect::<Vec<_>>();
                // Span::merge_all(spans).unwrap()
                unimplemented!()
            }
            Expression::DataType(expr) => expr.span.clone(),

            Expression::Prefix(expr) => expr.span(),

            Expression::Call(expr) => expr.span(),
        }
    }
}

impl AddExpression {
    pub fn span(&self) -> Span {
        Span::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

impl ConstantExpression {
    pub fn span(&self) -> Span {
        match self {
            ConstantExpression::Undefined { span } => span.clone(),
            ConstantExpression::Bool { span } => span.clone(),
            ConstantExpression::Number { span } => span.clone(),
            ConstantExpression::Text { span } => span.clone(),
        }
    }
}

impl PrefixExpression {
    pub fn span(&self) -> Span {
        Span::merge_all([self.span.clone(), self.expression.span()])
    }
}

impl SubtractExpression {
    pub fn span(&self) -> Span {
        Span::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

impl MultiplyExpression {
    pub fn span(&self) -> Span {
        Span::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

impl DivideExpression {
    pub fn span(&self) -> Span {
        Span::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

impl ModuloExpression {
    pub fn span(&self) -> Span {
        Span::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

impl Expression {
    pub fn span(&self) -> Span {
        self.lazy_span()()
    }
}
