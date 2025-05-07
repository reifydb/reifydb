// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::Value;

#[derive(Debug)]
pub enum Expression {
    /// A constant value.
    Constant(Value),

    /// lhs AND rhs: logical AND of two booleans
    And(Box<Expression>, Box<Expression>),
    /// a OR b: logical OR of two booleans
    Or(Box<Expression>, Box<Expression>),
    /// NOT a: logical NOT of a boolean
    Not(Box<Expression>),

    
}
