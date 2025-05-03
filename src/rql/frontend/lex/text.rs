// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::frontend::lex::{Span, Token, TokenKind};
use nom::bytes::is_not;
use nom::character::char;
use nom::sequence::delimited;
use nom::{IResult, Parser};

/// Parses any text
pub(crate) fn parse_text(input: Span) -> IResult<Span, Token> {
    let (rest, span) = delimited(char('\''), is_not("'"), char('\'')).parse(input)?;
    Ok((rest, Token { kind: TokenKind::Text, span }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::Offset;

    #[test]
    fn test_text() {
        let input = Span::new("'hello'");
        let (rest, token) = parse_text(input).unwrap();
        assert_eq!(token.kind, TokenKind::Text);
        assert_eq!(*token.span.fragment(), "hello");
        assert_eq!(rest.fragment().len(), 0);
    }

    #[test]
    fn test_text_with_trailing() {
        let input = Span::new("'data'123");
        let (rest, token) = parse_text(input).unwrap();
        assert_eq!(*token.span.fragment(), "data");
        assert_eq!(*rest.fragment(), "123");
        assert_eq!(input.offset(&rest), 6); // 'data' is 6 chars
    }

    #[test]
    fn test_text_unterminated_fails() {
        let input = Span::new("'not closed");
        let result = parse_text(input);
        assert!(result.is_err());
    }
}
