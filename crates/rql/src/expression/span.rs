// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

use crate::expression::{
    AddExpression, ConstantExpression, Expression, PrefixExpression, SubtractExpression,
};
use reifydb_diagnostic::Span;

impl Expression {
    pub fn lazy_span<'a>(&'a self) -> impl Fn() -> Span + 'a {
        move || match self {
            Expression::Constant(expr) => match expr {
                ConstantExpression::Undefined { span }
                | ConstantExpression::Bool { span }
                | ConstantExpression::Number { span }
                | ConstantExpression::Text { span } => span.clone(),
            },
            Expression::Column(expr) => expr.0.clone(),

            Expression::Add(expr) => expr.span(),
            Expression::Subtract(expr) => expr.span(),

            Expression::Multiply(expr) => {
                // Span::merge_all([expr.left.span(), &expr.span, expr.right.span()])
                unimplemented!()
            }

            Expression::Divide(expr) => {
                // Span::merge_all([expr.left.span(), &expr.span, expr.right.span()])
                unimplemented!()
            }

            Expression::Modulo(expr) => {
                // Span::merge_all([expr.left.span(), &expr.span, expr.right.span()])
                unimplemented!()
            }

            Expression::Tuple(expr) => {
                // let spans = expr.elements.iter().map(|e| e.span()).collect::<Vec<_>>();
                // Span::merge_all(spans).unwrap()
                unimplemented!()
            }

            Expression::Prefix(expr) => expr.span(),

            Expression::Call(expr) => {
                // let mut spans = vec![&expr.span, expr.callee.span()];
                // spans.extend(expr.arguments.iter().map(|a| a.span()));
                // Span::merge_all(spans).unwrap()
                unimplemented!()
            }
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

impl Expression {
    pub fn span(&self) -> Span {
        self.lazy_span()()
    }
}
