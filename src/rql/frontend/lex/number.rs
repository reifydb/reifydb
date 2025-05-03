// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::frontend::lex::{NumberKind, Span, Token, TokenKind};
use nom::branch::alt;
use nom::bytes::{tag, take_while1};
use nom::character::{char, digit1};
use nom::combinator::{complete, opt, recognize};
use nom::number::double;
use nom::sequence::{pair, preceded};
use nom::{IResult, Parser};
use NumberKind::{Binary, Decimal, Hex, Octal};
use TokenKind::Number;

/// Parses any numeric token (float, int, hex, octal, binary)
pub fn parse_number(input: Span) -> IResult<Span, Token> {
    alt((parse_hex, parse_octal, parse_binary, parse_float, parse_decimal)).parse(input)
}

fn parse_hex(input: Span) -> IResult<Span, Token> {
    let (rest, span) = complete(recognize(preceded(tag("0x"), take_while1(|c: char| c.is_ascii_hexdigit())))).parse(input)?;
    Ok((rest, Token { kind: Number(Hex), span }))
}

fn parse_octal(input: Span) -> IResult<Span, Token> {
    let (rest, span) = complete(recognize(preceded(tag("0o"), take_while1(|c: char| c >= '0' && c <= '7')))).parse(input)?;
    Ok((rest, Token { kind: Number(Octal), span }))
}

fn parse_binary(input: Span) -> IResult<Span, Token> {
    let (rest, span) = complete(recognize(preceded(tag("0b"), take_while1(|c: char| c == '0' || c == '1')))).parse(input)?;
    Ok((rest, Token { kind: Number(Binary), span }))
}

fn parse_float(input: Span) -> IResult<Span, Token> {
    let (rest, span) = complete(recognize(double())).parse(input)?;
    Ok((rest, Token { kind: Number(Decimal), span }))
}

fn parse_decimal(input: Span) -> IResult<Span, Token> {
    let (rest, span) = complete(recognize(pair(opt(char('-')), digit1()))).parse(input)?;
    Ok((rest, Token { kind: Number(Decimal), span }))
}

#[cfg(test)]
mod tests {
    use crate::rql::frontend::lex::number::parse_number;
    use crate::rql::frontend::lex::NumberKind::{Binary, Decimal, Hex, Octal};
    use crate::rql::frontend::lex::TokenKind::Number;
    use crate::rql::frontend::lex::{Span, Token};

    #[test]
    fn test_hex() {
        let (_rest, token) = parse_number(Span::new("0x2A")).unwrap();
        assert_eq!(token, Token { kind: Number(Hex), span: Span::new("0x2A") });
    }

    #[test]
    fn test_octal() {
        let (_rest, token) = parse_number(Span::new("0o777")).unwrap();
        assert_eq!(token, Token { kind: Number(Octal), span: Span::new("0o777") });
    }

    #[test]
    fn test_binary() {
        let (_rest, token) = parse_number(Span::new("0b1010")).unwrap();
        assert_eq!(token, Token { kind: Number(Binary), span: Span::new("0b1010") });
    }

    #[test]
    fn test_decimal() {
        let (_rest, token) = parse_number(Span::new("100")).unwrap();
        assert_eq!(token, Token { kind: Number(Decimal), span: Span::new("100") });
    }

    #[test]
    fn test_float() {
        let (_rest, token) = parse_number(Span::new("-42.5")).unwrap();
        assert_eq!(token, Token { kind: Number(Decimal), span: Span::new("-42.5") });
    }
}
