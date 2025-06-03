// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use constant::ExpressionConstant;

mod constant;

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
    Constant(ExpressionConstant),

    Column(ExpressionColumn),

    Add(ExpressionAdd),

    Divide(ExpressionDivide),

    Call(ExpressionCall),

    Modulo(ExpressionModulo),

    Multiply(ExpressionMultiply),

    Subtract(ExpressionSubtract),

    Tuple(ExpressionTuple),

    Prefix(ExpressionPrefix),
}

#[derive(Debug, Clone)]
pub struct ExpressionAdd {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct ExpressionDivide {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct ExpressionSubtract {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct ExpressionModulo {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct ExpressionMultiply {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct ExpressionColumn(pub String);

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Constant(val) => write!(f, "{}", val),
            Expression::Column(ExpressionColumn(name)) => write!(f, "{}", name),
            Expression::Add(ExpressionAdd { left, right }) => write!(f, "({} + {})", left, right),
            Expression::Divide(ExpressionDivide { left, right }) => {
                write!(f, "({} / {})", left, right)
            }
            Expression::Call(call) => write!(f, "{}", call),
            Expression::Modulo(ExpressionModulo { left, right }) => {
                write!(f, "({} % {})", left, right)
            }
            Expression::Multiply(ExpressionMultiply { left, right }) => {
                write!(f, "({} * {})", left, right)
            }
            Expression::Subtract(ExpressionSubtract { left, right }) => {
                write!(f, "({} - {})", left, right)
            }
            Expression::Tuple(tuple) => write!(f, "({})", tuple),
            Expression::Prefix(prefix) => write!(f, "{}", prefix),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExpressionCall {
    pub func: IdentExpression,
    pub args: Vec<Expression>,
}

impl Display for ExpressionCall {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let args = self.args.iter().map(|arg| format!("{}", arg)).collect::<Vec<_>>().join(", ");
        write!(f, "{}({})", self.func, args)
    }
}

#[derive(Debug, Clone)]
pub struct IdentExpression {
    pub name: String,
}

impl Display for IdentExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Clone)]
pub enum PrefixOperator {
    Minus,
    Plus,
}

impl Display for PrefixOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PrefixOperator::Minus => write!(f, "-"),
            PrefixOperator::Plus => write!(f, "+"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExpressionPrefix {
    pub operator: PrefixOperator,
    pub expression: Box<Expression>,
}

impl Display for ExpressionPrefix {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({}{})", self.operator, self.expression)
    }
}

#[derive(Debug, Clone)]
pub struct ExpressionTuple {
    pub expressions: Vec<Expression>,
}

impl Display for ExpressionTuple {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let items =
            self.expressions.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ");
        write!(f, "({})", items)
    }
}
