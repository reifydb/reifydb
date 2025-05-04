// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::frontend::lex::{Token, TokenKind};

#[derive(Debug, PartialEq)]
pub enum Error {
    // InvalidIdentifier(Token),
    // InvalidKind(Token),
    UnexpectedEndOfFile,
    UnexpectedToken { expected: TokenKind, got: TokenKind, span: ErrorSpan },
    // UnsupportedNumber(String),
    UnsupportedToken { got: TokenKind, span: ErrorSpan },
    // UnknownType(Token),
}

impl Error {
    pub(crate) fn eof() -> Self {
        Self::UnexpectedEndOfFile
    }
    pub(crate) fn unexpected(expected: TokenKind, got: &Token) -> Self {
        Self::UnexpectedToken { expected, got: got.kind, span: ErrorSpan::from(got) }
    }
    pub(crate) fn unsupported(got: &Token) -> Self {
        Self::UnsupportedToken { got: got.kind, span: ErrorSpan::from(got) }
    }
}

#[derive(Debug, PartialEq)]
pub struct ErrorSpan {
    pub offset: usize,
    pub line: u32,
}

impl<'a> From<&'a Token<'a>> for ErrorSpan {
    fn from(value: &Token) -> Self {
        Self { offset: value.span.location_offset(), line: value.span.location_line() }
    }
}
