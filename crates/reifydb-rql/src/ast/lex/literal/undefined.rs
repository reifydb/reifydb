// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use nom::{IResult, Parser, branch::alt, bytes::tag_no_case, combinator::map};
use nom_locate::LocatedSpan;

use crate::ast::{
	Token,
	TokenKind::Literal,
	lex::{Literal::Undefined, as_fragment},
};

pub(crate) fn parse_undefined(
	input: LocatedSpan<&str>,
) -> IResult<LocatedSpan<&str>, Token> {
	alt((map(tag_no_case("undefined"), |fragment: LocatedSpan<&str>| {
		Token {
			kind: Literal(Undefined),
			fragment: as_fragment(fragment),
		}
	}),))
	.parse(input)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ast::lex::{Literal::Undefined, literal::parse_literal};

	#[test]
	fn test_undefined() {
		let (_rest, token) =
			parse_literal(LocatedSpan::new("undefined")).unwrap();
		assert_eq!(token.kind, Literal(Undefined));
	}
}
