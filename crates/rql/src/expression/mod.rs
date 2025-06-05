// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use constant::ConstantExpression;

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
    Constant(ConstantExpression),

    Column(ColumnExpression),

    Add(AddExpression),

    Divide(DivideExpression),

    Call(CallExpression),

    Modulo(ModuloExpression),

    Multiply(MultiplyExpression),

    Subtract(SubstractExpression),

    Tuple(TupleExpression),

    Prefix(PrefixExpression),
}

#[derive(Debug, Clone)]
pub struct AddExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct DivideExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct SubstractExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct ModuloExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct MultiplyExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct ColumnExpression(pub String);

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Constant(val) => write!(f, "{}", val),
            Expression::Column(ColumnExpression(name)) => write!(f, "{}", name),
            Expression::Add(AddExpression { left, right }) => write!(f, "({} + {})", left, right),
            Expression::Divide(DivideExpression { left, right }) => {
                write!(f, "({} / {})", left, right)
            }
            Expression::Call(call) => write!(f, "{}", call),
            Expression::Modulo(ModuloExpression { left, right }) => {
                write!(f, "({} % {})", left, right)
            }
            Expression::Multiply(MultiplyExpression { left, right }) => {
                write!(f, "({} * {})", left, right)
            }
            Expression::Subtract(SubstractExpression { left, right }) => {
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
}

impl Display for CallExpression {
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
pub struct PrefixExpression {
    pub operator: PrefixOperator,
    pub expression: Box<Expression>,
}

impl Display for PrefixExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({}{})", self.operator, self.expression)
    }
}

#[derive(Debug, Clone)]
pub struct TupleExpression {
    pub expressions: Vec<Expression>,
}

impl Display for TupleExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let items =
            self.expressions.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ");
        write!(f, "({})", items)
    }
}
