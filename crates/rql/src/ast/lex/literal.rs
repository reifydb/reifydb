// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::Literal::{False, Number, Text, True, Undefined};
use crate::ast::lex::{Token, TokenKind, as_span};
use TokenKind::Literal;
use nom::branch::alt;
use nom::bytes::{is_not, tag, tag_no_case, take_while1};
use nom::character::{char, digit1, multispace0};
use nom::combinator::{complete, map, recognize};
use nom::number::double;
use nom::sequence::{delimited, preceded};
use nom::{IResult, Parser};
use nom_locate::LocatedSpan;

/// Parses any literal
pub(crate) fn parse_literal(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    preceded(multispace0(), alt((parse_text, parse_number, parse_boolean, parse_undefined)))
        .parse(input)
}

fn parse_boolean(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
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

/// Parses any text
fn parse_text(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    let (rest, span) = delimited(char('\''), is_not("'"), char('\'')).parse(input)?;
    Ok((rest, Token { kind: Literal(Text), span: as_span(span) }))
}

/// Parses any numeric token (float, int, hex, octal, binary)
fn parse_number(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    alt((parse_hex, parse_octal, parse_binary, parse_float, parse_decimal)).parse(input)
}

fn parse_hex(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    let inner = recognize(preceded(tag("0x"), take_while1(|c: char| c.is_ascii_hexdigit())));
    let (rest, span) = complete(inner).parse(input)?;
    Ok((rest, Token { kind: Literal(Number), span: as_span(span) }))
}

fn parse_octal(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    let inner = recognize(preceded(tag("0o"), take_while1(|c: char| ('0'..='7').contains(&c))));
    let (rest, span) = complete(inner).parse(input)?;
    Ok((rest, Token { kind: Literal(Number), span: as_span(span) }))
}

fn parse_binary(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    let inner = recognize(preceded(tag("0b"), take_while1(|c: char| c == '0' || c == '1')));
    let (rest, span) = complete(inner).parse(input)?;
    Ok((rest, Token { kind: Literal(Number), span: as_span(span) }))
}

fn parse_float(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    let (rest, span) = complete(recognize(double())).parse(input)?;
    Ok((rest, Token { kind: Literal(Number), span: as_span(span) }))
}

fn parse_decimal(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    let (rest, span) = complete(digit1()).parse(input)?;
    Ok((rest, Token { kind: Literal(Number), span: as_span(span) }))
}

fn parse_undefined(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    alt((map(tag_no_case("undefined"), |span: LocatedSpan<&str>| Token {
        kind: Literal(Undefined),
        span: as_span(span),
    }),))
    .parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::lex::Literal::{False, True, Undefined};
    use nom::Offset;

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

    #[test]
    fn test_number_hex() {
        let (_rest, token) = parse_literal(LocatedSpan::new("0x2A")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "0x2A");
    }

    #[test]
    fn test_number_octal() {
        let (_rest, token) = parse_literal(LocatedSpan::new("0o777")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "0o777");
    }

    #[test]
    fn test_number_binary() {
        let (_rest, token) = parse_literal(LocatedSpan::new("0b1010")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "0b1010");
    }

    #[test]
    fn test_number_decimal() {
        let (_rest, token) = parse_literal(LocatedSpan::new("100")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "100");
    }

    #[test]
    fn test_number_float() {
        let (_rest, token) = parse_literal(LocatedSpan::new("42.5")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "42.5");
    }

    #[test]
    fn test_text() {
        let input = LocatedSpan::new("'hello'");
        let (rest, token) = parse_literal(input).unwrap();
        assert_eq!(token.kind, Literal(Text));
        assert_eq!(&token.span.fragment, "hello");
        assert_eq!(rest.fragment().len(), 0);
    }

    #[test]
    fn test_text_with_trailing() {
        let input = LocatedSpan::new("'data'123");
        let (rest, token) = parse_literal(input).unwrap();
        assert_eq!(&token.span.fragment, "data");
        assert_eq!(*rest.fragment(), "123");
        assert_eq!(input.offset(&rest), 6); // 'data' is 6 chars
    }

    #[test]
    fn test_text_unterminated_fails() {
        let input = LocatedSpan::new("'not closed");
        let result = parse_literal(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_undefined() {
        let (_rest, token) = parse_literal(LocatedSpan::new("undefined")).unwrap();
        assert_eq!(token.kind, Literal(Undefined));
    }
}
