// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::TokenKind;
use crate::ast::lex::{Keyword, Literal, Operator, Separator};
use std::fmt::{Display, Formatter};

impl Display for TokenKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenKind::EOF => write!(f, "end of file"),
            TokenKind::Keyword(k) => write!(f, "{}", k),
            TokenKind::Identifier => write!(f, "identifier"),
            TokenKind::Literal(lit) => write!(f, "{}", lit),
            TokenKind::Operator(op) => write!(f, "{}", op),
            TokenKind::Separator(sep) => write!(f, "{}", sep),
        }
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Literal::False => "false",
            Literal::Number => "number",
            Literal::Text => "string",
            Literal::True => "true",
            Literal::Undefined => "undefined",
        };
        write!(f, "{text}")
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Display for Separator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
