// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub enum ExpressionConstant {
    Undefined,
    Bool(bool),
    // any number
    Number(String),
    // any textual representation can be String, Text, ...
    Text(String),
}

impl Display for ExpressionConstant {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ExpressionConstant::Undefined => write!(f, "undefined"),
            ExpressionConstant::Bool(b) => write!(f, "{b}"),
            ExpressionConstant::Number(n) => write!(f, "{n}"),
            ExpressionConstant::Text(s) => write!(f, "\"{s}\""),
        }
    }
}
