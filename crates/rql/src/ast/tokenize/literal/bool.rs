// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	Token, TokenKind,
	tokenize::{Literal, cursor::Cursor, identifier::is_identifier_char},
};

/// Scan for a boolean literal (true/false)
pub fn scan_boolean(cursor: &mut Cursor) -> Option<Token> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	if cursor.peek_str(4).eq_ignore_ascii_case("true") {
		let next = cursor.peek_ahead(4);
		if next.map_or(true, |c| !is_identifier_char(c)) {
			cursor.consume_str_ignore_case("true");
			return Some(Token {
				kind: TokenKind::Literal(Literal::True),
				fragment: cursor.make_fragment(start_pos, start_line, start_column),
			});
		}
	}

	if cursor.peek_str(5).eq_ignore_ascii_case("false") {
		let next = cursor.peek_ahead(5);
		if next.map_or(true, |c| !is_identifier_char(c)) {
			cursor.consume_str_ignore_case("false");
			return Some(Token {
				kind: TokenKind::Literal(Literal::False),
				fragment: cursor.make_fragment(start_pos, start_line, start_column),
			});
		}
	}

	None
}

#[cfg(test)]
mod tests {
	use Literal::{False, True};

	use super::*;
	use crate::ast::tokenize::tokenize;

	#[test]
	fn test_boolean_true() {
		let tokens = tokenize("true").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(True));
		assert_eq!(tokens[0].fragment.text(), "true");
	}

	#[test]
	fn test_boolean_false() {
		let tokens = tokenize("false").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(False));
		assert_eq!(tokens[0].fragment.text(), "false");
	}

	#[test]
	fn test_boolean_case_insensitive() {
		let tokens = tokenize("TRUE").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(True));

		let tokens = tokenize("False").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(False));

		let tokens = tokenize("TrUe").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(True));
	}

	#[test]
	fn test_boolean_with_trailing() {
		let tokens = tokenize("true123").unwrap();
		// Should parse as identifier, not boolean
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "true123");

		let tokens = tokenize("false_value").unwrap();
		// Should parse as identifier, not boolean
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "false_value");
	}

	#[test]
	fn test_boolean_separated() {
		let tokens = tokenize("true false").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(True));
		assert_eq!(tokens[1].kind, TokenKind::Literal(False));

		let tokens = tokenize("true,false").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(True));
		assert_eq!(tokens[2].kind, TokenKind::Literal(False));
	}
}
