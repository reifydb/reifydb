// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::token::{
	cursor::Cursor,
	identifier::is_identifier_char,
	token::{Literal, Token, TokenKind},
};

/// Scan for a boolean literal (true/false)
pub fn scan_boolean<'b>(cursor: &mut Cursor<'b>) -> Option<Token<'b>> {
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
pub mod tests {
	use Literal::{False, True};

	use super::*;
	use crate::{bump::Bump, token::tokenize};

	#[test]
	fn test_boolean_true() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "true").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(True));
		assert_eq!(tokens[0].fragment.text(), "true");
	}

	#[test]
	fn test_boolean_false() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "false").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(False));
		assert_eq!(tokens[0].fragment.text(), "false");
	}

	#[test]
	fn test_boolean_case_insensitive() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "TRUE").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(True));

		let tokens = tokenize(&bump, "False").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(False));

		let tokens = tokenize(&bump, "TrUe").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(True));
	}

	#[test]
	fn test_boolean_with_trailing() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "true123").unwrap();
		// Should parse as identifier, not boolean
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "true123");

		let tokens = tokenize(&bump, "false_value").unwrap();
		// Should parse as identifier, not boolean
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "false_value");
	}

	#[test]
	fn test_boolean_separated() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "true false").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(True));
		assert_eq!(tokens[1].kind, TokenKind::Literal(False));

		let tokens = tokenize(&bump, "true,false").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(True));
		assert_eq!(tokens[2].kind, TokenKind::Literal(False));
	}
}
