// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Value;

#[derive(Debug, Clone)]
pub struct AliasExpression {
    pub alias: Option<String>,
    pub expression: Expression,
}

#[derive(Debug, Clone)]
pub enum Expression {
    /// l AND lr: logical AND of two booleans
    And(Box<Expression>, Box<Expression>),
    /// a OR b: logical OR of two booleans
    Or(Box<Expression>, Box<Expression>),
    /// NOT a: logical NOT of a boolean
    Not(Box<Expression>),

    /// A constant value.
    Constant(Value),

    Column(String),

    Add(Box<Expression>, Box<Expression>),

    Call(CallExpression),

    Tuple(TupleExpression),

    Prefix(PrefixExpression),
}

#[derive(Debug, Clone)]
pub struct CallExpression {
    pub func: IdentExpression,
    pub args: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct IdentExpression {
    pub name: String,
}

#[derive(Debug, Clone)]
pub enum PrefixOperator {
    Minus,
    Plus,
}

#[derive(Debug, Clone)]
pub struct PrefixExpression {
    pub operator: PrefixOperator,
    pub expression: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct TupleExpression {
    pub expressions: Vec<Expression>,
}
