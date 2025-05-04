// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use error::Error;
use nom::branch::alt;
use nom::character::multispace0;
use nom::multi::many0;

pub use keyword::Keyword;
pub use operator::Operator;
pub use separator::Separator;

use crate::rql::frontend::lex::identifier::parse_identifier;
use crate::rql::frontend::lex::keyword::parse_keyword;
use crate::rql::frontend::lex::literal::parse_literal;
use crate::rql::frontend::lex::operator::parse_operator;
use crate::rql::frontend::lex::separator::parse_separator;
use nom::combinator::complete;
use nom::sequence::preceded;
use nom::{IResult, Parser};
use nom_locate::LocatedSpan;

mod error;
mod identifier;
mod keyword;
mod literal;
mod operator;
mod separator;

pub type Span<'a> = LocatedSpan<&'a str>;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token<'a> {
    pub kind: TokenKind,
    pub span: Span<'a>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TokenKind {
    Keyword(Keyword),
    Identifier,
    Literal(Literal),
    Operator(Operator),
    Separator(Separator),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Literal {
    False,
    Number(NumberKind),
    Text,
    True,
    Undefined,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum NumberKind {
    Decimal,
    Hex,
    Octal,
    Binary,
}

pub fn lex<'a>(input: impl Into<Span<'a>>) -> Result<Vec<Token<'a>>> {
    match many0(token).parse(input.into()) {
        Ok((_, tokens)) => Ok(tokens),
        Err(err) => Err(Error(format!("{}", err))),
    }
}

fn token(input: Span) -> IResult<Span, Token> {
    complete(preceded(multispace0(), alt((parse_keyword, parse_literal, parse_identifier, parse_operator, parse_separator)))).parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rql::frontend::lex::Literal::{Number, Text};
    use crate::rql::frontend::lex::NumberKind::Decimal;
    use TokenKind::Literal;

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
        assert_eq!(token.kind, Literal(Number(Decimal)));
        assert_eq!(*token.span.fragment(), "42");
    }

    #[test]
    fn test_text() {
        let (_rest, token) = token(span("'hello world'")).unwrap();
        assert_eq!(token.kind, Literal(Text));
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
