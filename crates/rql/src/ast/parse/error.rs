// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::{Token, TokenKind};

#[derive(Debug, PartialEq)]
pub enum Error {
    InvalidType { got: Token },
    UnexpectedEndOfFile,
    InvalidPolicy { got: Token },
    UnexpectedToken { expected: TokenKind, got: Token },
    UnsupportedToken { got: Token },
}

impl Error {
    pub(crate) fn eof() -> Self {
        Self::UnexpectedEndOfFile
    }
    pub(crate) fn invalid_policy(got: Token) -> Self {
        Self::InvalidPolicy { got }
    }
    pub(crate) fn unexpected(expected: TokenKind, got: Token) -> Self {
        Self::UnexpectedToken { expected, got }
    }
    pub(crate) fn unsupported(got: Token) -> Self {
        Self::UnsupportedToken { got: got }
    }
}
