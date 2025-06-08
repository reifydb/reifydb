// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use error::Error;
use nom::branch::alt;
use nom::character::multispace0;
use nom::multi::many0;
use std::fmt::Display;

pub use keyword::Keyword;
pub use operator::Operator;
pub use separator::Separator;

use crate::ast::lex::TokenKind::EOF;
use crate::ast::lex::identifier::parse_identifier;
use crate::ast::lex::keyword::parse_keyword;
use crate::ast::lex::literal::parse_literal;
use crate::ast::lex::operator::parse_operator;
use crate::ast::lex::separator::parse_separator;
use nom::combinator::complete;
use nom::sequence::preceded;
use nom::{IResult, Parser};
use nom_locate::LocatedSpan;
use reifydb_diagnostic::{Line, Offset, Span};

mod display;
mod error;
mod identifier;
mod keyword;
mod literal;
mod operator;
mod separator;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

impl From<Token> for Span {
    fn from(value: Token) -> Self {
        value.span
    }
}

impl Token {
    pub fn is_eof(&self) -> bool {
        self.kind == EOF
    }
    pub fn is_identifier(&self) -> bool {
        self.kind == TokenKind::Identifier
    }
    pub fn is_literal(&self, literal: Literal) -> bool {
        self.kind == TokenKind::Literal(literal)
    }
    pub fn is_separator(&self, separator: Separator) -> bool {
        self.kind == TokenKind::Separator(separator)
    }
    pub fn is_keyword(&self, keyword: Keyword) -> bool {
        self.kind == TokenKind::Keyword(keyword)
    }
    pub fn is_operator(&self, operator: Operator) -> bool {
        self.kind == TokenKind::Operator(operator)
    }
    pub fn value(&self) -> &str {
        self.span.fragment.as_str()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TokenKind {
    EOF,
    Keyword(Keyword),
    Identifier,
    Literal(Literal),
    Operator(Operator),
    Separator(Separator),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Literal {
    False,
    Number,
    Text,
    True,
    Undefined,
}

pub fn lex<'a>(input: impl Into<LocatedSpan<&'a str>>) -> Result<Vec<Token>> {
    match many0(token).parse(input.into()) {
        Ok((_, tokens)) => Ok(tokens),
        Err(err) => Err(Error(format!("{}", err))),
    }
}

fn token(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    complete(preceded(
        multispace0(),
        alt((parse_keyword, parse_operator, parse_literal, parse_identifier, parse_separator)),
    ))
    .parse(input)
}

pub(crate) fn as_span(value: LocatedSpan<&str>) -> Span {
    Span {
        offset: Offset(value.location_offset() as u32),
        line: Line(value.location_line()),
        fragment: value.fragment().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::lex::Literal::{Number, Text};
    use TokenKind::Literal;

    fn span(s: &str) -> LocatedSpan<&str> {
        LocatedSpan::new(s)
    }

    #[test]
    fn test_keyword() {
        let (_rest, token) = token(span("SELECT")).unwrap();
        assert_eq!(token.kind, TokenKind::Keyword(Keyword::Select));
        assert_eq!(token.span.fragment.as_str(), "SELECT");
    }

    #[test]
    fn test_identifier() {
        let (_rest, token) = token(span("my_var123")).unwrap();
        assert_eq!(token.kind, TokenKind::Identifier);
        assert_eq!(token.span.fragment.as_str(), "my_var123");
    }

    #[test]
    fn test_number() {
        let (_rest, token) = token(span("42")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.span.fragment.as_str(), "42");
    }

    #[test]
    fn test_number_negative() {
        let (rest, token) = token(span("-42")).unwrap();
        assert_eq!(token.kind, TokenKind::Operator(Operator::Minus));
        assert_eq!(token.span.fragment.as_str(), "-");
        assert_eq!(rest.fragment().to_string(), "42");
    }

    #[test]
    fn test_text() {
        let (_rest, token) = token(span("'hello world'")).unwrap();
        assert_eq!(token.kind, Literal(Text));
        assert_eq!(token.span.fragment.as_str(), "hello world");
    }

    #[test]
    fn test_operator() {
        let (_rest, token) = token(span("+")).unwrap();
        assert_eq!(token.kind, TokenKind::Operator(Operator::Plus));
        assert_eq!(token.span.fragment.as_str(), "+");
    }

    #[test]
    fn test_separator() {
        let (_rest, token) = token(span(",")).unwrap();
        assert_eq!(token.kind, TokenKind::Separator(Separator::Comma));
        assert_eq!(token.span.fragment.as_str(), ",");
    }

    #[test]
    fn test_skips_whitespace() {
        let (_rest, token) = token(span("   SELECT")).unwrap();
        assert_eq!(token.kind, TokenKind::Keyword(Keyword::Select));
        assert_eq!(token.span.fragment.as_str(), "SELECT");
    }
}
