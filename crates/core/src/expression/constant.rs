// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub enum ConstantExpression {
    Undefined,
    Bool(bool),
    Number(String),
    String(String),
}

impl Display for ConstantExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ConstantExpression::Undefined => write!(f, "undefined"),
            ConstantExpression::Bool(b) => write!(f, "{b}"),
            ConstantExpression::Number(n) => write!(f, "{n}"),
            ConstantExpression::String(s) => write!(f, "\"{s}\""),
        }
    }
}
