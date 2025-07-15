// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::Token;
use crate::ast::TokenKind::Literal;
use crate::ast::lex::Literal::Undefined;
use crate::ast::lex::as_span;
use nom::branch::alt;
use nom::bytes::tag_no_case;
use nom::combinator::map;
use nom::{IResult, Parser};
use nom_locate::LocatedSpan;

pub(crate) fn parse_undefined(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    alt((map(tag_no_case("undefined"), |span: LocatedSpan<&str>| Token {
        kind: Literal(Undefined),
        span: as_span(span),
    }),))
    .parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::lex::Literal::Undefined;
    use crate::ast::lex::literal::parse_literal;

    #[test]
    fn test_undefined() {
        let (_rest, token) = parse_literal(LocatedSpan::new("undefined")).unwrap();
        assert_eq!(token.kind, Literal(Undefined));
    }
}
