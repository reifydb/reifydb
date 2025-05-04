// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::frontend::lex::Literal::{False, Number, Text, True, Undefined};
use crate::rql::frontend::lex::NumberKind::{Binary, Decimal, Hex, Octal};
use crate::rql::frontend::lex::{Span, Token, TokenKind};
use nom::branch::alt;
use nom::bytes::{is_not, tag, tag_no_case, take_while1};
use nom::character::{char, digit1, multispace0};
use nom::combinator::{complete, map, opt, recognize};
use nom::number::double;
use nom::sequence::{delimited, pair, preceded};
use nom::{IResult, Parser};
use TokenKind::Literal;

/// Parses any literal
pub(crate) fn parse_literal(input: Span) -> IResult<Span, Token> {
    preceded(multispace0(), alt((parse_text, parse_number, parse_boolean, parse_undefined))).parse(input)
}

fn parse_boolean(input: Span) -> IResult<Span, Token> {
    alt((
        map(tag_no_case("true"), |span: Span| Token { kind: Literal(True), span }),
        map(tag_no_case("false"), |span: Span| Token { kind: Literal(False), span }),
    ))
    .parse(input)
}

/// Parses any text
fn parse_text(input: Span) -> IResult<Span, Token> {
    let (rest, span) = delimited(char('\''), is_not("'"), char('\'')).parse(input)?;
    Ok((rest, Token { kind: Literal(Text), span }))
}

/// Parses any numeric token (float, int, hex, octal, binary)
fn parse_number(input: Span) -> IResult<Span, Token> {
    alt((parse_hex, parse_octal, parse_binary, parse_float, parse_decimal)).parse(input)
}

fn parse_hex(input: Span) -> IResult<Span, Token> {
    let (rest, span) = complete(recognize(preceded(tag("0x"), take_while1(|c: char| c.is_ascii_hexdigit())))).parse(input)?;
    Ok((rest, Token { kind: Literal(Number(Hex)), span }))
}

fn parse_octal(input: Span) -> IResult<Span, Token> {
    let (rest, span) = complete(recognize(preceded(tag("0o"), take_while1(|c: char| c >= '0' && c <= '7')))).parse(input)?;
    Ok((rest, Token { kind: Literal(Number(Octal)), span }))
}

fn parse_binary(input: Span) -> IResult<Span, Token> {
    let (rest, span) = complete(recognize(preceded(tag("0b"), take_while1(|c: char| c == '0' || c == '1')))).parse(input)?;
    Ok((rest, Token { kind: Literal(Number(Binary)), span }))
}

fn parse_float(input: Span) -> IResult<Span, Token> {
    let (rest, span) = complete(recognize(double())).parse(input)?;
    Ok((rest, Token { kind: Literal(Number(Decimal)), span }))
}

fn parse_decimal(input: Span) -> IResult<Span, Token> {
    let (rest, span) = complete(recognize(pair(opt(char('-')), digit1()))).parse(input)?;
    Ok((rest, Token { kind: Literal(Number(Decimal)), span }))
}

fn parse_undefined(input: Span) -> IResult<Span, Token> {
    alt((map(tag_no_case("undefined"), |span: Span| Token { kind: Literal(Undefined), span }),)).parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rql::frontend::lex::Literal::{False, True, Undefined};
    use nom::Offset;

    #[test]
    fn test_boolean_true() {
        let (_rest, token) = parse_literal(Span::new("true")).unwrap();
        assert_eq!(token, Token { kind: Literal(True), span: Span::new("true") });
    }

    #[test]
    fn test_boolean_false() {
        let (_rest, token) = parse_literal(Span::new("false")).unwrap();
        assert_eq!(token, Token { kind: Literal(False), span: Span::new("false") });
    }

    #[test]
    fn test_number_hex() {
        let (_rest, token) = parse_literal(Span::new("0x2A")).unwrap();
        assert_eq!(token, Token { kind: Literal(Number(Hex)), span: Span::new("0x2A") });
    }

    #[test]
    fn test_number_octal() {
        let (_rest, token) = parse_literal(Span::new("0o777")).unwrap();
        assert_eq!(token, Token { kind: Literal(Number(Octal)), span: Span::new("0o777") });
    }

    #[test]
    fn test_number_binary() {
        let (_rest, token) = parse_literal(Span::new("0b1010")).unwrap();
        assert_eq!(token, Token { kind: Literal(Number(Binary)), span: Span::new("0b1010") });
    }

    #[test]
    fn test_number_decimal() {
        let (_rest, token) = parse_literal(Span::new("100")).unwrap();
        assert_eq!(token, Token { kind: Literal(Number(Decimal)), span: Span::new("100") });
    }

    #[test]
    fn test_number_float() {
        let (_rest, token) = parse_literal(Span::new("-42.5")).unwrap();
        assert_eq!(token, Token { kind: Literal(Number(Decimal)), span: Span::new("-42.5") });
    }

    #[test]
    fn test_text() {
        let input = Span::new("'hello'");
        let (rest, token) = parse_literal(input).unwrap();
        assert_eq!(token.kind, Literal(Text));
        assert_eq!(*token.span.fragment(), "hello");
        assert_eq!(rest.fragment().len(), 0);
    }

    #[test]
    fn test_text_with_trailing() {
        let input = Span::new("'data'123");
        let (rest, token) = parse_literal(input).unwrap();
        assert_eq!(*token.span.fragment(), "data");
        assert_eq!(*rest.fragment(), "123");
        assert_eq!(input.offset(&rest), 6); // 'data' is 6 chars
    }

    #[test]
    fn test_text_unterminated_fails() {
        let input = Span::new("'not closed");
        let result = parse_literal(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_undefined() {
        let (_rest, token) = parse_literal(Span::new("undefined")).unwrap();
        assert_eq!(token, Token { kind: Literal(Undefined), span: Span::new("undefined") });
    }
}
