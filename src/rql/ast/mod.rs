// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use expression::*;
pub use literal::*;

mod expression;
mod literal;
mod operator;

#[derive(Debug, Clone, PartialEq)]
pub enum Ast {
    Block(AstBlock),
    Expression(AstExpression),
    From(AstFrom),
    Identifier(AstIdentifier),
    Literal(AstLiteral),
    Select(AstSelect),
    Where(AstWhere),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstBlock {
    pub nodes: Vec<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstFrom {
    pub alias: Option<Box<Ast>>,
    pub source: Box<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstIdentifier {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstJoin {
    pub source: Box<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstSelect {
    pub columns: Vec<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstWhere {
    pub clause: AstExpression,
}

// FROM users
// SELECT *;

// FROM ( FROM users SELECT *) SELECT *;
