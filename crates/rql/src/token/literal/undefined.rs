// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::token::{
	cursor::Cursor,
	identifier::is_identifier_char,
	token::{Literal::Undefined, Token, TokenKind},
};

/// Scan for undefined literal
pub fn scan_undefined<'b>(cursor: &mut Cursor<'b>) -> Option<Token<'b>> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	if cursor.peek_str(9).eq_ignore_ascii_case("undefined") {
		let next = cursor.peek_ahead(9);
		if next.map_or(true, |c| !is_identifier_char(c)) {
			cursor.consume_str_ignore_case("undefined");
			return Some(Token {
				kind: TokenKind::Literal(Undefined),
				fragment: cursor.make_fragment(start_pos, start_line, start_column),
			});
		}
	}

	None
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{
		bump::Bump,
		token::{keyword::Keyword, tokenize},
	};

	#[test]
	fn test_undefined() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "undefined").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Undefined));
		assert_eq!(tokens[0].fragment.text(), "undefined");
	}

	#[test]
	fn test_undefined_case_insensitive() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "UNDEFINED").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Undefined));

		let tokens = tokenize(&bump, "Undefined").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Undefined));

		let tokens = tokenize(&bump, "UnDeFiNeD").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Undefined));
	}

	#[test]
	fn test_undefined_with_trailing() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "undefined123").unwrap();
		// Should parse as identifier, not undefined
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "undefined123");

		let tokens = tokenize(&bump, "undefined_value").unwrap();
		// Should parse as identifier, not undefined
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "undefined_value");
	}

	#[test]
	fn test_undefined_separated() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "undefined, undefined").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Undefined));
		assert_eq!(tokens[2].kind, TokenKind::Literal(Undefined));
	}

	#[test]
	fn test_undefined_in_expression() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "value == undefined").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Value));
		assert_eq!(tokens[0].fragment.text(), "value");
		assert_eq!(tokens[2].kind, TokenKind::Literal(Undefined));
	}
}
