// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use nom::{IResult, Parser, branch::alt, bytes::tag_no_case, combinator::map};
use nom_locate::LocatedSpan;

use crate::ast::{
	Token,
	TokenKind::Literal,
	lex::{
		Literal::{False, True},
		as_fragment,
	},
};

pub(crate) fn parse_boolean(
	input: LocatedSpan<&str>,
) -> IResult<LocatedSpan<&str>, Token> {
	alt((
		map(tag_no_case("true"), |fragment: LocatedSpan<&str>| Token {
			kind: Literal(True),
			fragment: as_fragment(fragment),
		}),
		map(tag_no_case("false"), |fragment: LocatedSpan<&str>| Token {
			kind: Literal(False),
			fragment: as_fragment(fragment),
		}),
	))
	.parse(input)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ast::lex::{
		Literal::{False, True},
		literal::parse_literal,
	};

	#[test]
	fn test_boolean_true() {
		let (_rest, token) =
			parse_literal(LocatedSpan::new("true")).unwrap();
		assert_eq!(token.kind, Literal(True));
	}

	#[test]
	fn test_boolean_false() {
		let (_rest, token) =
			parse_literal(LocatedSpan::new("false")).unwrap();
		assert_eq!(token.kind, Literal(False));
	}
}
