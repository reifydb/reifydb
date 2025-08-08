// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::Token;
use crate::ast::TokenKind::Literal;
use crate::ast::lex::Literal::{False, True};
use crate::ast::lex::as_span;
use nom::branch::alt;
use nom::bytes::tag_no_case;
use nom::combinator::map;
use nom::{IResult, Parser};
use nom_locate::LocatedSpan;

pub(crate) fn parse_boolean(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    alt((
        map(tag_no_case("true"), |span: LocatedSpan<&str>| Token {
            kind: Literal(True),
            span: as_span(span),
        }),
        map(tag_no_case("false"), |span: LocatedSpan<&str>| Token {
            kind: Literal(False),
            span: as_span(span),
        }),
    ))
    .parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::lex::Literal::{False, True};
    use crate::ast::lex::literal::parse_literal;

    #[test]
    fn test_boolean_true() {
        let (_rest, token) = parse_literal(LocatedSpan::new("true")).unwrap();
        assert_eq!(token.kind, Literal(True));
    }

    #[test]
    fn test_boolean_false() {
        let (_rest, token) = parse_literal(LocatedSpan::new("false")).unwrap();
        assert_eq!(token.kind, Literal(False));
    }
}
