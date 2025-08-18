// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use nom::{IResult, Parser, character::char, sequence::delimited};
use nom_locate::LocatedSpan;

use crate::ast::{
	Token,
	TokenKind::Literal,
	lex::{Literal::Text, as_fragment},
};

/// Parses text with support for both single and double quotes, allowing mixing
pub(crate) fn parse_text(
	input: LocatedSpan<&str>,
) -> IResult<LocatedSpan<&str>, Token> {
	use nom::{branch::alt, bytes::complete::take_while};

	let parse_single_quoted = |input| {
		let (rest, fragment) = delimited(
			char('\''),
			take_while(|c| c != '\''),
			char('\''),
		)
		.parse(input)?;
		Ok((
			rest,
			Token {
				kind: Literal(Text),
				fragment: as_fragment(fragment),
			},
		))
	};

	let parse_double_quoted = |input| {
		let (rest, fragment) = delimited(
			char('"'),
			take_while(|c| c != '"'),
			char('"'),
		)
		.parse(input)?;
		Ok((
			rest,
			Token {
				kind: Literal(Text),
				fragment: as_fragment(fragment),
			},
		))
	};

	alt((parse_single_quoted, parse_double_quoted)).parse(input)
}

#[cfg(test)]
mod tests {
	use nom::Offset;

	use super::*;
	use crate::ast::lex::literal::parse_literal;

	#[test]
	fn test_text_single_quotes() {
		let input = LocatedSpan::new("'hello'");
		let (rest, token) = parse_literal(input).unwrap();
		assert_eq!(token.kind, Literal(Text));
		assert_eq!(token.fragment.fragment(), "hello");
		assert_eq!(rest.fragment().len(), 0);
	}

	#[test]
	fn test_text_double_quotes() {
		let input = LocatedSpan::new("\"hello\"");
		let (rest, token) = parse_literal(input).unwrap();
		assert_eq!(token.kind, Literal(Text));
		assert_eq!(token.fragment.fragment(), "hello");
		assert_eq!(rest.fragment().len(), 0);
	}

	#[test]
	fn test_text_single_quotes_with_double_inside() {
		let input = LocatedSpan::new("'some text\"xx\"no problem'");
		let (rest, token) = parse_literal(input).unwrap();
		assert_eq!(token.kind, Literal(Text));
		assert_eq!(token.fragment.fragment(), "some text\"xx\"no problem");
		assert_eq!(rest.fragment().len(), 0);
	}

	#[test]
	fn test_text_double_quotes_with_single_inside() {
		let input = LocatedSpan::new("\"some text'xx'no problem\"");
		let (rest, token) = parse_literal(input).unwrap();
		assert_eq!(token.kind, Literal(Text));
		assert_eq!(token.fragment.fragment(), "some text'xx'no problem");
		assert_eq!(rest.fragment().len(), 0);
	}

	#[test]
	fn test_text_with_trailing() {
		let input = LocatedSpan::new("'data'123");
		let (rest, token) = parse_literal(input).unwrap();
		assert_eq!(token.fragment.fragment(), "data");
		assert_eq!(*rest.fragment(), "123");
		assert_eq!(input.offset(&rest), 6); // 'data' is 6 chars
	}

	#[test]
	fn test_text_double_quotes_with_trailing() {
		let input = LocatedSpan::new("\"data\"123");
		let (rest, token) = parse_literal(input).unwrap();
		assert_eq!(token.fragment.fragment(), "data");
		assert_eq!(*rest.fragment(), "123");
		assert_eq!(input.offset(&rest), 6); // "data" is 6 chars
	}

	#[test]
	fn test_text_single_unterminated_fails() {
		let input = LocatedSpan::new("'not closed");
		let result = parse_literal(input);
		assert!(result.is_err());
	}

	#[test]
	fn test_text_double_unterminated_fails() {
		let input = LocatedSpan::new("\"not closed");
		let result = parse_literal(input);
		assert!(result.is_err());
	}

	#[test]
	fn test_text_empty_single_quotes() {
		let input = LocatedSpan::new("''");
		let (rest, token) = parse_literal(input).unwrap();
		assert_eq!(token.kind, Literal(Text));
		assert_eq!(token.fragment.fragment(), "");
		assert_eq!(rest.fragment().len(), 0);
	}

	#[test]
	fn test_text_empty_double_quotes() {
		let input = LocatedSpan::new("\"\"");
		let (rest, token) = parse_literal(input).unwrap();
		assert_eq!(token.kind, Literal(Text));
		assert_eq!(token.fragment.fragment(), "");
		assert_eq!(rest.fragment().len(), 0);
	}

	#[test]
	fn test_text_mixed_quotes_complex() {
		let input = LocatedSpan::new(
			"'He said \"Hello\" and she replied \"Hi\"'",
		);
		let (rest, token) = parse_literal(input).unwrap();
		assert_eq!(token.kind, Literal(Text));
		assert_eq!(
			token.fragment.fragment(),
			"He said \"Hello\" and she replied \"Hi\""
		);
		assert_eq!(rest.fragment().len(), 0);
	}

	#[test]
	fn test_text_multiple_nested_quotes() {
		let input =
			LocatedSpan::new("\"It's a 'nice' day, isn't it?\"");
		let (rest, token) = parse_literal(input).unwrap();
		assert_eq!(token.kind, Literal(Text));
		assert_eq!(
			token.fragment.fragment(),
			"It's a 'nice' day, isn't it?"
		);
		assert_eq!(rest.fragment().len(), 0);
	}
}
