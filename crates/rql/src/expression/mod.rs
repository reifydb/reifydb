// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_diagnostic::Span;
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub struct AliasExpression {
    pub alias: Option<String>,
    pub expression: Expression,
}

impl Display for AliasExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(alias) = &self.alias {
            Display::fmt(&alias, f)
        } else {
            Display::fmt(&self.expression, f)
        }
    }
}

#[derive(Debug, Clone)]
pub enum Expression {
    Constant(ConstantExpression),

    Column(ColumnExpression),

    Add(AddExpression),

    Divide(DivideExpression),

    Call(CallExpression),

    Modulo(ModuloExpression),

    Multiply(MultiplyExpression),

    Subtract(SubtractExpression),

    Tuple(TupleExpression),

    Prefix(PrefixExpression),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConstantExpression {
    Undefined(Span),
    Bool(Span),
    // any number
    Number(Span),
    // any textual representation can be String, Text, ...
    Text(Span),
}

impl Display for ConstantExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ConstantExpression::Undefined(_) => write!(f, "undefined"),
            ConstantExpression::Bool(span) => write!(f, "{}", span.fragment),
            ConstantExpression::Number(span) => write!(f, "{}", span.fragment),
            ConstantExpression::Text(span) => write!(f, "\"{}\"", span.fragment),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AddExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct DivideExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct SubtractExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct ModuloExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct MultiplyExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct ColumnExpression(pub Span);

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Constant(span) => write!(f, "{}", span),
            Expression::Column(ColumnExpression(span)) => write!(f, "{}", span.fragment),
            Expression::Add(AddExpression { left, right, .. }) => {
                write!(f, "({} + {})", left, right)
            }
            Expression::Divide(DivideExpression { left, right, .. }) => {
                write!(f, "({} / {})", left, right)
            }
            Expression::Call(call) => write!(f, "{}", call),
            Expression::Modulo(ModuloExpression { left, right, .. }) => {
                write!(f, "({} % {})", left, right)
            }
            Expression::Multiply(MultiplyExpression { left, right, .. }) => {
                write!(f, "({} * {})", left, right)
            }
            Expression::Subtract(SubtractExpression { left, right, .. }) => {
                write!(f, "({} - {})", left, right)
            }
            Expression::Tuple(tuple) => write!(f, "({})", tuple),
            Expression::Prefix(prefix) => write!(f, "{}", prefix),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CallExpression {
    pub func: IdentExpression,
    pub args: Vec<Expression>,
    pub span: Span,
}

impl Display for CallExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let args = self.args.iter().map(|arg| format!("{}", arg)).collect::<Vec<_>>().join(", ");
        write!(f, "{}({})", self.func, args)
    }
}

#[derive(Debug, Clone)]
pub struct IdentExpression(pub Span);

impl IdentExpression {
    pub fn name(&self) -> &str {
        &self.0.fragment
    }
}

impl Display for IdentExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.fragment)
    }
}

#[derive(Debug, Clone)]
pub enum PrefixOperator {
    Minus(Span),
    Plus(Span),
}

impl Display for PrefixOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PrefixOperator::Minus(_) => write!(f, "-"),
            PrefixOperator::Plus(_) => write!(f, "+"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrefixExpression {
    pub operator: PrefixOperator,
    pub expression: Box<Expression>,
    pub span: Span,
}

impl Display for PrefixExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({}{})", self.operator, self.expression)
    }
}

#[derive(Debug, Clone)]
pub struct TupleExpression {
    pub expressions: Vec<Expression>,
    pub span: Span,
}

impl Display for TupleExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let items =
            self.expressions.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ");
        write!(f, "({})", items)
    }
}
