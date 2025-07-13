// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::Literal::{False, Number, Text, True, Undefined};
use crate::ast::lex::{Token, TokenKind, as_span};
use TokenKind::Literal;
use nom::branch::alt;
use nom::bytes::{is_not, tag_no_case};
use nom::character::{char, multispace0};
use nom::combinator::{complete, map};
use nom::error::Error;
use nom::error::ErrorKind::Verify;
use nom::sequence::{delimited, preceded};
use nom::{AsChar, IResult, Parser};
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
    alt((parse_decimal, parse_hex, parse_binary, parse_octal)).parse(input)
}

fn parse_hex(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    use nom::{
        bytes::complete::tag,
        bytes::complete::take_while1,
        combinator::{complete, recognize},
        sequence::pair,
    };

    fn is_hex_or_underscore(c: char) -> bool {
        c.is_ascii_hexdigit() || c == '_'
    }

    fn is_valid_hex_with_underscores(s: &str) -> bool {
        let bytes = s.as_bytes();
        if bytes.first() == Some(&b'_') || bytes.last() == Some(&b'_') {
            return false;
        }
        !s.contains("__")
    }

    let full = recognize(pair(tag("0x"), take_while1(is_hex_or_underscore)));

    let (rest, span) = complete(full).parse(input)?;

    let literal = *span.fragment();
    if !is_valid_hex_with_underscores(&literal[2..]) {
        return Err(nom::Err::Error(Error::new(input, Verify)));
    }

    Ok((rest, Token { kind: Literal(Number), span: as_span(span) }))
}

fn parse_octal(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    use nom::{
        bytes::complete::{tag, take_while1},
        combinator::{complete, recognize},
        error::{Error, ErrorKind::Verify},
        sequence::pair,
    };

    fn not_whitespace(c: char) -> bool {
        !c.is_whitespace()
    }

    fn is_valid_octal_format(s: &str) -> bool {
        // Must not start or end with _
        if s.starts_with('_') || s.ends_with('_') {
            return false;
        }

        // Must not contain double underscores
        if s.contains("__") {
            return false;
        }

        // Must contain at least one binary digit
        s.chars().all(|c| c.is_oct_digit() || c == '_')
    }

    let (rest, span) =
        complete(recognize(pair(tag("0o"), take_while1(not_whitespace)))).parse(input)?;

    let literal = *span.fragment();
    let suffix = &literal[2..]; // after "0x"
    if !is_valid_octal_format(suffix) {
        return Err(nom::Err::Error(Error::new(input, Verify)));
    }

    Ok((rest, Token { kind: Literal(Number), span: as_span(span) }))
}

fn parse_binary(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    use nom::error::{Error, ErrorKind::Verify};
    use nom::{
        bytes::complete::{tag, take_while1},
        combinator::{complete, recognize},
        sequence::pair,
    };

    fn not_whitespace(c: char) -> bool {
        !c.is_whitespace()
    }

    fn is_valid_binary_format(s: &str) -> bool {
        // Must not start or end with _
        if s.starts_with('_') || s.ends_with('_') {
            return false;
        }

        // Must not contain double underscores
        if s.contains("__") {
            return false;
        }

        // Must contain at least one binary digit
        s.chars().all(|c| c == '0' || c == '1' || c == '_')
    }

    // Recognize "0b" + valid char sequence
    let (rest, span) =
        complete(recognize(pair(tag("0b"), take_while1(not_whitespace)))).parse(input)?;

    let literal = *span.fragment();
    let suffix = &literal[2..]; // skip "0b"
    if !is_valid_binary_format(suffix) {
        return Err(nom::Err::Error(Error::new(input, Verify)));
    }

    Ok((rest, Token { kind: Literal(Number), span: as_span(span) }))
}

fn parse_decimal(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    use nom::{
        branch::alt,
        bytes::complete::take_while1,
        character::complete::{char, one_of},
        combinator::{opt, recognize},
        sequence::{pair, preceded},
    };

    // Reject binary, hex, octal
    if input.fragment().starts_with("0b")
        || input.fragment().starts_with("0x")
        || input.fragment().starts_with("0o")
    {
        return Err(nom::Err::Error(Error::new(input, Verify)));
    }

    fn is_digit_or_underscore(c: char) -> bool {
        c.is_ascii_digit() || c == '_'
    }

    fn is_valid_decimal_format(s: &str) -> bool {
        if s.matches('.').count() > 1 {
            return false;
        }

        if s.starts_with('_') || s.ends_with('_') || s.contains("__") {
            return false;
        }

        if !s.chars().any(|c| c.is_ascii_digit()) {
            return false;
        }

        // Reject underscore directly around dot
        if s.contains("._") || s.contains("_.") {
            return false;
        }

        true
    }

    // Parts for decimal
    let fraction = preceded(char('.'), take_while1(is_digit_or_underscore));

    // Combine these variants:
    let integer_dot = complete(recognize(pair(take_while1(is_digit_or_underscore), char('.'))));
    let dot_fraction = complete(recognize(pair(char('.'), take_while1(is_digit_or_underscore))));
    let int_frac = complete(recognize(pair(take_while1(is_digit_or_underscore), fraction)));
    let just_integer = complete(recognize(take_while1(is_digit_or_underscore)));

    let base = alt((int_frac, integer_dot, dot_fraction, just_integer));

    // Exponent: e[+/-]?digits
    let exponent =
        complete(recognize((one_of("eE"), opt(one_of("+-")), take_while1(is_digit_or_underscore))));

    let mut full = complete(recognize(pair(base, opt(exponent))));

    let (rest, span) = full.parse(input)?;
    let literal = *span.fragment();

    if !is_valid_decimal_format(literal) {
        return Err(nom::Err::Error(Error::new(input, Verify)));
    }

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
    fn test_number_hex_with_underscores() {
        let (_rest, token) = parse_literal(LocatedSpan::new("0x2A_AA")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "0x2A_AA");
    }

    #[test]
    fn test_number_octal() {
        let (_rest, token) = parse_literal(LocatedSpan::new("0o777")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "0o777");
    }

    #[test]
    fn test_number_octal_with_underscores() {
        let (_rest, token) = parse_literal(LocatedSpan::new("0o777_555")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "0o777_555");
    }

    #[test]
    fn test_number_binary() {
        let (_rest, token) = parse_literal(LocatedSpan::new("0b1010")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "0b1010");
    }

    #[test]
    fn test_number_binary_with_underscores() {
        let (_rest, token) = parse_literal(LocatedSpan::new("0b111_010")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "0b111_010");
    }

    #[test]
    fn test_number_decimal() {
        let (_rest, token) = parse_literal(LocatedSpan::new("100")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "100");
    }

    #[test]
    fn test_number_decimal_with_underscores() {
        let (_rest, token) = parse_literal(LocatedSpan::new("1_000_000")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "1_000_000");
    }

    #[test]
    fn test_number_decimal_scientific() {
        let (_rest, token) = parse_literal(LocatedSpan::new("1e+500")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "1e+500");
    }

    #[test]
    fn test_number_float() {
        let (_rest, token) = parse_literal(LocatedSpan::new("42.5")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "42.5");
    }

    #[test]
    fn test_number_float_with_underscores() {
        let (_rest, token) = parse_literal(LocatedSpan::new("1_142.5_234")).unwrap();
        assert_eq!(token.kind, Literal(Number));
        assert_eq!(token.value(), "1_142.5_234");
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

    #[test]
    fn test_parse_decimal() {
        let cases = [
            ("0", true),
            ("42", true),
            ("1234567890", true),
            ("1_000", true),
            ("12_345_678", true),
            ("0.0", true),
            ("42.5", true),
            ("3.14159", true),
            ("1_000.000_1", true),
            ("0.123", true),
            ("123.", true),
            (".456", true),
            ("1_000.0", true),
            ("1.2e10", true),
            ("3.e+5", true),
            (".4e-2", true),
            // invalid cases
            ("_", false),
            ("1__", false),
            ("_123", false),
            ("123_", false),
            ("1._0", false),
            ("1_.0", false),
            ("1.0_", false),
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(LocatedSpan::new(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Number), "input = {}", input);
                    assert_eq!(token.value(), input, "input = {}", input);
                }
                (Err(_), false) => {} // expected failure
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }

    #[test]
    fn test_parse_hex() {
        let cases = [
            ("0x0", true),
            ("0x2A", true),
            ("0xDEADBEEF", true),
            ("0x2A_AA", true),
            ("0xAB_CD_EF", true),
            // invalid
            ("0x_", false),
            ("0x__AB", false),
            ("0xAB__CD", false),
            ("0x_1234", false),
            ("0x1234_", false),
            ("0x", false),
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(LocatedSpan::new(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Number), "input = {}", input);
                    assert_eq!(token.value(), input, "input = {}", input);
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }

    #[test]
    fn test_parse_octal() {
        let cases = [
            ("0o0", true),
            ("0o7", true),
            ("0o1234567", true),
            ("0o12_34_56", true),
            // invalid
            ("0o_", false),
            ("0o_123", false),
            ("0o123_", false),
            ("0o12__34", false),
            ("0o8", false), // 8 is not valid in octal
            ("0o", false),
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(LocatedSpan::new(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Number), "input = {}", input);
                    assert_eq!(token.value(), input, "input = {}", input);
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }

    #[test]
    fn test_parse_binary() {
        let cases = [
            ("0b0", true),
            ("0b1", true),
            ("0b101010", true),
            ("0b1010_1100", true),
            ("0b1_0_1_0", true),
            // invalid
            ("0b_", false),
            ("0b_1010", false),
            ("0b1010_", false),
            ("0b10__10", false),
            ("0b102", false), // 2 is not valid in binary
            ("0b", false),
        ];

        for (input, should_parse) in cases {
            let result = parse_literal(LocatedSpan::new(input));
            match (result, should_parse) {
                (Ok((_rest, token)), true) => {
                    assert_eq!(token.kind, Literal(Number), "input = {}", input);
                    assert_eq!(token.value(), input, "input = {}", input);
                }
                (Err(_), false) => {}
                (Ok(_), false) => panic!("input {:?} should NOT parse but did", input),
                (Err(e), true) => panic!("input {:?} should parse but failed: {:?}", input, e),
            }
        }
    }
}
