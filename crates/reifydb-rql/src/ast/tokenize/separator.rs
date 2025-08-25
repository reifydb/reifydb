// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::cursor::Cursor;
use crate::ast::lex::{Separator, Token, TokenKind};

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
		fragment: cursor.make_fragment(
			start_pos,
			start_line,
			start_column,
		),
	})
}

#[cfg(test)]
mod tests {
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
		assert_eq!(
			TokenKind::Separator(op),
			tokens[0].kind,
			"type mismatch for symbol: {}",
			symbol
		);
		assert_eq!(tokens[0].fragment.fragment(), symbol);
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
		assert_eq!(tokens[0].fragment.fragment(), "rest");
		// The token is on line 2 because the newline was consumed
		assert_eq!(tokens[0].fragment.line().0, 2);
		assert_eq!(tokens[0].fragment.column().0, 2);
	}
}
