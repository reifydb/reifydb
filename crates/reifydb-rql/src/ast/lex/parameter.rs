// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use nom::{
	IResult, Parser,
	branch::alt,
	bytes::complete::{tag, take_while1},
	combinator::{complete, recognize},
	sequence::preceded,
};
use nom_locate::LocatedSpan;

use crate::ast::lex::{Token, TokenKind, as_span};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ParameterKind {
	Positional(u32), // $1, $2, etc.
	Named,           // $name, $user_id, etc.
}

pub(crate) fn parse_parameter(
	input: LocatedSpan<&str>,
) -> IResult<LocatedSpan<&str>, Token> {
	alt((parse_positional_parameter, parse_named_parameter)).parse(input)
}

fn parse_positional_parameter(
	input: LocatedSpan<&str>,
) -> IResult<LocatedSpan<&str>, Token> {
	let (rest, span) = complete(recognize(preceded(
		tag("$"),
		take_while1(|c: char| c.is_ascii_digit()),
	)))
	.parse(input)?;

	// Extract the number part for validation
	let num_str = &span.fragment()[1..]; // Skip the '$'
	if let Ok(num) = num_str.parse::<u32>() {
		if num > 0 {
			Ok((
				rest,
				Token {
					kind: TokenKind::Parameter(
						ParameterKind::Positional(num),
					),
					span: as_span(span),
				},
			))
		} else {
			// $0 is not a valid parameter
			Err(nom::Err::Error(nom::error::Error::new(
				input,
				nom::error::ErrorKind::Verify,
			)))
		}
	} else {
		Err(nom::Err::Error(nom::error::Error::new(
			input,
			nom::error::ErrorKind::Verify,
		)))
	}
}

fn parse_named_parameter(
	input: LocatedSpan<&str>,
) -> IResult<LocatedSpan<&str>, Token> {
	let (rest, span) = complete(recognize(preceded(
		tag("$"),
		take_while1(is_parameter_name_char),
	)))
	.parse(input)?;

	// Make sure the first character after $ is alphabetic or underscore
	let name_part = &span.fragment()[1..]; // Skip the '$'
	if name_part.chars().next().map_or(false, is_parameter_name_start) {
		Ok((
			rest,
			Token {
				kind: TokenKind::Parameter(
					ParameterKind::Named,
				),
				span: as_span(span),
			},
		))
	} else {
		Err(nom::Err::Error(nom::error::Error::new(
			input,
			nom::error::ErrorKind::Verify,
		)))
	}
}

fn is_parameter_name_start(c: char) -> bool {
	c.is_ascii_alphabetic() || c == '_'
}

fn is_parameter_name_char(c: char) -> bool {
	c.is_ascii_alphanumeric() || c == '_'
}

#[cfg(test)]
mod tests {
	use nom_locate::LocatedSpan;

	use super::*;
	use crate::ast::lex::TokenKind;

	#[test]
	fn test_positional_parameter() {
		let (_rest, result) =
			parse_parameter(LocatedSpan::new("$1")).unwrap();
		assert_eq!(
			result.kind,
			TokenKind::Parameter(ParameterKind::Positional(1))
		);
		assert_eq!(&result.span.fragment, "$1");

		let (_rest, result) =
			parse_parameter(LocatedSpan::new("$42")).unwrap();
		assert_eq!(
			result.kind,
			TokenKind::Parameter(ParameterKind::Positional(42))
		);
		assert_eq!(&result.span.fragment, "$42");
	}

	#[test]
	fn test_named_parameter() {
		let (_rest, result) =
			parse_parameter(LocatedSpan::new("$name")).unwrap();
		assert_eq!(
			result.kind,
			TokenKind::Parameter(ParameterKind::Named)
		);
		assert_eq!(&result.span.fragment, "$name");

		let (_rest, result) =
			parse_parameter(LocatedSpan::new("$user_id")).unwrap();
		assert_eq!(
			result.kind,
			TokenKind::Parameter(ParameterKind::Named)
		);
		assert_eq!(&result.span.fragment, "$user_id");

		let (_rest, result) =
			parse_parameter(LocatedSpan::new("$_private")).unwrap();
		assert_eq!(
			result.kind,
			TokenKind::Parameter(ParameterKind::Named)
		);
		assert_eq!(&result.span.fragment, "$_private");
	}

	#[test]
	fn test_invalid_parameters() {
		// $0 is not valid
		assert!(parse_parameter(LocatedSpan::new("$0")).is_err());

		// $ alone is not valid
		assert!(parse_parameter(LocatedSpan::new("$")).is_err());

		// $123name is parsed as $123
		let (rest, result) =
			parse_parameter(LocatedSpan::new("$123name")).unwrap();
		assert_eq!(
			result.kind,
			TokenKind::Parameter(ParameterKind::Positional(123))
		);
		assert_eq!(rest.fragment(), &"name");
	}
}
