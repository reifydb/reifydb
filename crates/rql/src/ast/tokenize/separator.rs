// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{
	cursor::Cursor,
	token::{Token, TokenKind},
};

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

/// Scan for a separator token
pub fn scan_separator(cursor: &mut Cursor) -> Option<Token> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	let sep = match cursor.peek()? {
		';' => Separator::Semicolon,
		',' => Separator::Comma,
		'\n' => Separator::NewLine,
		_ => return None,
	};

	cursor.consume();
	Some(Token {
		kind: TokenKind::Separator(sep),
		fragment: cursor.make_fragment(start_pos, start_line, start_column),
	})
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::ast::tokenize::tokenize;

	#[test]
	fn test_parse_separator_invalid() {
		let tokens = tokenize("foobar rest").unwrap();
		// Should parse as identifier, not separator
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
	}

	fn check_separator(op: Separator, symbol: &str) {
		let input_str = format!("{symbol} rest");
		let tokens = tokenize(&input_str).unwrap();

		assert!(tokens.len() >= 2);
		assert_eq!(TokenKind::Separator(op), tokens[0].kind, "type mismatch for symbol: {}", symbol);
		assert_eq!(tokens[0].fragment.text(), symbol);
		assert_eq!(tokens[0].fragment.column().0, 1);
		assert_eq!(tokens[0].fragment.line().0, 1);
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
	    test_separator_comma => (Comma, ",")
	}

	// Special test for newline
	// Note: Newlines are treated as whitespace and skipped during
	// tokenization So they won't produce separator tokens in the current
	// implementation
	#[test]
	fn test_separator_new_line() {
		// Newlines are skipped as whitespace, so "\n rest" just
		// produces "rest"
		let tokens = tokenize("\n rest").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "rest");
		// The token is on line 2 because the newline was consumed
		assert_eq!(tokens[0].fragment.line().0, 2);
		assert_eq!(tokens[0].fragment.column().0, 2);
	}
}
