// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use nom::{IResult, Input, Parser, branch::alt, bytes::tag, combinator::value};
use nom_locate::LocatedSpan;

use crate::ast::lex::{Token, TokenKind, as_fragment};

macro_rules! separator {
    (
        $( $value:ident => $tag:literal ),*
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum Separator {  $( $value ),* }

        impl Separator {
            pub fn as_str(&self) -> &'static str {
                match self {
                    $( Separator::$value => $tag ),*
                }
            }
        }
    };
}

separator! {
    Semicolon => ";",
    Comma => ",",
    NewLine => "\n"
}

pub(crate) fn parse_separator(
	input: LocatedSpan<&str>,
) -> IResult<LocatedSpan<&str>, Token> {
	let start = input;

	let parser = alt((alt((
		value(Separator::Semicolon, tag(";")),
		value(Separator::Comma, tag(",")),
		value(Separator::NewLine, tag("\n")),
	)),));

	parser.map(|sep| Token {
		kind: TokenKind::Separator(sep),
		fragment: as_fragment(start.take(sep.as_str().len())),
	})
	.parse(input)
}

#[cfg(test)]
mod tests {
	use nom_locate::LocatedSpan;
	use reifydb_core::Fragment;

	use crate::ast::lex::{
		TokenKind,
		separator::{Separator, parse_separator},
	};

	#[test]
	fn test_parse_separator_invalid() {
		let input = LocatedSpan::new("foobar rest");
		let result = parse_separator(input);

		assert!(
			result.is_err(),
			"expected error parsing invalid separator, got: {:?}",
			result
		);
	}

	fn check_separator(op: Separator, symbol: &str) {
		let input_str = format!("{symbol} rest");
		let input = LocatedSpan::new(input_str.as_str());

		let result = parse_separator(input).unwrap();
		let (remaining, token) = result;

		assert_eq!(
			TokenKind::Separator(op),
			token.kind,
			"ty mismatch for symbol: {}",
			symbol
		);
		assert_eq!(token.fragment.fragment(), symbol);
		assert_eq!(token.fragment.column().0, 1);
		assert_eq!(token.fragment.line().0, 1);
		assert_eq!(*remaining.fragment(), " rest");
	}

	macro_rules! generate_test {
        ($($name:ident => ($variant:ident, $symbol:literal)),*) => {
            $(
                #[test]
                fn $name() {
                    check_separator(Separator::$variant, $symbol);
                }
            )*
        };
    }

	generate_test! {
	    test_separator_semicolon => (Semicolon, ";"),
	    test_separator_comma => (Comma, ","),
	    test_separator_new_line => (NewLine, "\n")
	}
}
