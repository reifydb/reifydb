// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use nom::{
	IResult, Parser,
	bytes::{complete::take_while1, take_while},
	combinator::{complete, recognize},
	sequence::pair,
};
use nom_locate::LocatedSpan;

use crate::ast::lex::{Token, TokenKind, as_fragment};

pub(crate) fn parse_identifier(
	input: LocatedSpan<&str>,
) -> IResult<LocatedSpan<&str>, Token> {
	let (rest, fragment) = complete(recognize(pair(
		take_while1(is_identifier_start),
		take_while(is_identifier_char),
	)))
	.parse(input)?;
	Ok((
		rest,
		Token {
			kind: TokenKind::Identifier,
			fragment: as_fragment(fragment),
		},
	))
}

fn is_identifier_start(c: char) -> bool {
	c.is_ascii_alphabetic() || c == '_'
}

fn is_identifier_char(c: char) -> bool {
	c.is_ascii_alphanumeric() || c == '_'
}

#[cfg(test)]
mod tests {
	use nom_locate::LocatedSpan;

	use crate::ast::lex::{TokenKind, identifier::parse_identifier};

	#[test]
	fn test_identifier() {
		let (_rest, result) =
			parse_identifier(LocatedSpan::new("user_referral"))
				.unwrap();
		assert_eq!(result.kind, TokenKind::Identifier);
		assert_eq!(result.fragment.fragment(), "user_referral");
	}
}
