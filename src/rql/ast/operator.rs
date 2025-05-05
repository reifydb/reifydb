// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::ast::expression::AstExpression;

#[derive(Clone, Debug, PartialEq)]
pub enum AstOperator {
    Equal(Box<AstExpression>, Box<AstExpression>),
    GreaterThan(Box<AstExpression>, Box<AstExpression>),
    GreaterThanEqual(Box<AstExpression>, Box<AstExpression>),
}

