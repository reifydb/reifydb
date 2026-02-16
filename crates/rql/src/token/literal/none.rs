// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::token::{
	cursor::Cursor,
	identifier::is_identifier_char,
	token::{Literal::None, Token, TokenKind},
};

/// Scan for none literal
pub fn scan_none<'b>(cursor: &mut Cursor<'b>) -> Option<Token<'b>> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	if cursor.peek_str(4).eq_ignore_ascii_case("none") {
		let next = cursor.peek_ahead(4);
		if next.map_or(true, |c| !is_identifier_char(c)) {
			cursor.consume_str_ignore_case("none");
			return Some(Token {
				kind: TokenKind::Literal(None),
				fragment: cursor.make_fragment(start_pos, start_line, start_column),
			});
		}
	}

	Option::None
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{
		bump::Bump,
		token::{keyword::Keyword, tokenize},
	};

	#[test]
	fn test_none() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "none").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(None));
		assert_eq!(tokens[0].fragment.text(), "none");
	}

	#[test]
	fn test_none_case_insensitive() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "NONE").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(None));

		let tokens = tokenize(&bump, "None").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(None));

		let tokens = tokenize(&bump, "NoNe").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(None));
	}

	#[test]
	fn test_none_with_trailing() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "none123").unwrap();
		// Should parse as identifier, not none
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "none123");

		let tokens = tokenize(&bump, "none_value").unwrap();
		// Should parse as identifier, not none
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "none_value");
	}

	#[test]
	fn test_none_separated() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "none, none").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(None));
		assert_eq!(tokens[2].kind, TokenKind::Literal(None));
	}

	#[test]
	fn test_none_in_expression() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "value == none").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Value));
		assert_eq!(tokens[0].fragment.text(), "value");
		assert_eq!(tokens[2].kind, TokenKind::Literal(None));
	}
}
