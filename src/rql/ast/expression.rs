// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::ast::operator::AstOperator;
use crate::rql::ast::{AstIdentifier, AstLiteral};

#[derive(Debug, Clone, PartialEq)]
pub enum AstExpression {
    /// All columns, i.e. *.
    All,
    Identifier(AstIdentifier),
    Literal(AstLiteral),
    Operator(AstOperator),
}
