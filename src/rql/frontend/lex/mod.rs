// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use error::Error;
use nom::branch::alt;
use nom::character::multispace0;
use nom::multi::many0;

use crate::rql::frontend::lex::identifier::parse_identifier;
use crate::rql::frontend::lex::keyword::{parse_keyword, Keyword};
use crate::rql::frontend::lex::number::parse_number;
use crate::rql::frontend::lex::operator::{parse_operator, Operator};
use crate::rql::frontend::lex::separator::{parse_separator, Separator};
use crate::rql::frontend::lex::text::parse_text;
use nom::sequence::preceded;
use nom::{IResult, Parser};
use nom_locate::LocatedSpan;

mod error;
mod identifier;
mod keyword;
mod number;
mod operator;
mod separator;
mod text;

pub type Span<'a> = LocatedSpan<&'a str>;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token<'a> {
    pub kind: TokenKind,
    pub span: Span<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenKind {
    Keyword(Keyword),
    Identifier,
    Number(NumberKind),
    Operator(Operator),
    Separator(Separator),
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NumberKind {
    Decimal,
    Hex,
    Octal,
    Binary,
}

pub fn tokenize(input: Span) -> IResult<Span, Vec<Token>> {
    many0(token).parse(input)
}

fn token(input: Span) -> IResult<Span, Token> {
    preceded(multispace0(), alt((parse_keyword, parse_identifier, parse_number, parse_text, parse_operator, parse_separator))).parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rql::frontend::lex::NumberKind::Decimal;

    fn span(s: &'static str) -> Span<'static> {
        Span::new(s)
    }

    #[test]
    fn test_keyword() {
        let (_rest, token) = token(span("SELECT")).unwrap();
        assert_eq!(token.kind, TokenKind::Keyword(Keyword::Select));
        assert_eq!(*token.span.fragment(), "SELECT");
    }

    #[test]
    fn test_identifier() {
        let (_rest, token) = token(span("my_var123")).unwrap();
        assert_eq!(token.kind, TokenKind::Identifier);
        assert_eq!(*token.span.fragment(), "my_var123");
    }

    #[test]
    fn test_number() {
        let (_rest, token) = token(span("42")).unwrap();
        assert_eq!(token.kind, TokenKind::Number(Decimal));
        assert_eq!(*token.span.fragment(), "42");
    }

    #[test]
    fn test_text() {
        let (_rest, token) = token(span("'hello world'")).unwrap();
        assert_eq!(token.kind, TokenKind::Text);
        assert_eq!(*token.span.fragment(), "hello world");
    }

    #[test]
    fn test_operator() {
        let (_rest, token) = token(span("+")).unwrap();
        assert_eq!(token.kind, TokenKind::Operator(Operator::Plus));
        assert_eq!(*token.span.fragment(), "+");
    }

    #[test]
    fn test_separator() {
        let (_rest, token) = token(span(",")).unwrap();
        assert_eq!(token.kind, TokenKind::Separator(Separator::Comma));
        assert_eq!(*token.span.fragment(), ",");
    }

    #[test]
    fn test_skips_whitespace() {
        let (_rest, token) = token(span("   SELECT")).unwrap();
        assert_eq!(token.kind, TokenKind::Keyword(Keyword::Select));
        assert_eq!(*token.span.fragment(), "SELECT");
    }
}
