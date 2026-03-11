// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{
	cursor::Cursor,
	token::{Token, TokenKind},
};

/// Scan for an identifier token
pub fn scan_identifier<'b>(cursor: &mut Cursor<'b>) -> Option<Token<'b>> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	if !cursor.peek().map_or(false, is_identifier_start) {
		return None;
	}

	cursor.consume_while(is_identifier_char);

	Some(Token {
		kind: TokenKind::Identifier,
		fragment: cursor.make_fragment(start_pos, start_line, start_column),
	})
}

/// Scan for a backtick-quoted identifier (`...`)
pub fn scan_quoted_identifier<'b>(cursor: &mut Cursor<'b>) -> Option<Token<'b>> {
	if cursor.peek()? != '`' {
		return None;
	}

	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();
	cursor.consume(); // consume opening backtick

	let ident_start = cursor.pos();

	while let Some(ch) = cursor.peek() {
		if ch == '`' {
			let ident_end = cursor.pos();
			cursor.consume(); // consume closing backtick

			let fragment = cursor.make_utf8_fragment(
				ident_start,
				ident_end,
				start_line,
				start_column + 1,
				start_pos,
			);

			return Some(Token {
				kind: TokenKind::Identifier,
				fragment,
			});
		}
		cursor.consume();
	}

	None // Unterminated quoted identifier
}

/// Scan for an identifier that starts with a digit (e.g., `10min`, `5sec`, `10_min`).
/// Returns `None` if the token is a pure number (no alpha chars), or starts with
/// a number literal prefix like `0x`, `0b`, `0o`.
pub fn scan_digit_starting_identifier<'b>(cursor: &mut Cursor<'b>) -> Option<Token<'b>> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	let state = cursor.save_state();

	// Must start with an ASCII digit
	if !cursor.peek().map_or(false, |c| c.is_ascii_digit()) {
		return None;
	}

	// Guard: bail on 0x/0b/0o prefixes (those are number literals)
	let prefix = cursor.peek_str(2);
	if prefix.eq_ignore_ascii_case("0x") || prefix.eq_ignore_ascii_case("0b") || prefix.eq_ignore_ascii_case("0o") {
		return None;
	}

	let mut has_alpha = false;
	cursor.consume_while(|c| {
		if is_identifier_char(c) {
			if c.is_ascii_alphabetic() {
				has_alpha = true;
			}
			true
		} else {
			false
		}
	});

	if !has_alpha {
		// Pure number — restore cursor and return None
		cursor.restore_state(state);
		return None;
	}

	Some(Token {
		kind: TokenKind::Identifier,
		fragment: cursor.make_fragment(start_pos, start_line, start_column),
	})
}

pub fn is_identifier_start(ch: char) -> bool {
	ch.is_ascii_alphabetic() || ch == '_'
}

pub fn is_identifier_char(ch: char) -> bool {
	ch.is_ascii_alphanumeric() || ch == '_'
}

#[cfg(test)]
pub mod tests {
	use crate::{
		bump::Bump,
		token::{
			token::{Literal, TokenKind},
			tokenize,
		},
	};

	#[test]
	fn test_identifier() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "user_referral").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "user_referral");
	}

	#[test]
	fn test_quoted_identifier_simple() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "`my-identifier`").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "my-identifier");
	}

	#[test]
	fn test_quoted_identifier_with_spaces() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "`my table name`").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "my table name");
	}

	#[test]
	fn test_quoted_identifier_with_special_chars() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "`user@domain.com`").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "user@domain.com");
	}

	#[test]
	fn test_quoted_identifier_starting_with_digit() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "`123-table`").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "123-table");
	}

	#[test]
	fn test_quoted_identifier_empty() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "``").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "");
	}

	#[test]
	fn test_quoted_identifier_unterminated() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "`unclosed");
		// Should fail or return no identifier token
		assert!(tokens.is_err() || tokens.unwrap().iter().all(|t| !matches!(t.kind, TokenKind::Identifier)));
	}

	#[test]
	fn test_digit_starting_identifier_10min() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "10min").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "10min");
	}

	#[test]
	fn test_digit_starting_identifier_5sec() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "5sec").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "5sec");
	}

	#[test]
	fn test_digit_starting_identifier_with_underscore() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "10_min").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "10_min");
	}

	#[test]
	fn test_pure_number_still_number() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "42").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Literal(Literal::Number));
		assert_eq!(tokens[0].fragment.text(), "42");
	}

	#[test]
	fn test_hex_still_number() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "0xFF").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Literal(Literal::Number));
		assert_eq!(tokens[0].fragment.text(), "0xFF");
	}

	#[test]
	fn test_binary_still_number() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "0b1010").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Literal(Literal::Number));
		assert_eq!(tokens[0].fragment.text(), "0b1010");
	}

	#[test]
	fn test_scientific_still_number() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "1.23e10").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Literal(Literal::Number));
		assert_eq!(tokens[0].fragment.text(), "1.23e10");
	}

	#[test]
	fn test_float_with_trailing_identifier() {
		let bump = Bump::new();
		// 3.14px should be Number("3.14") + Identifier("px"), not an infinite loop
		let tokens = tokenize(&bump, "3.14px").unwrap();
		assert_eq!(tokens.len(), 2);
		assert_eq!(tokens[0].kind, TokenKind::Literal(Literal::Number));
		assert_eq!(tokens[0].fragment.text(), "3.14");
		assert_eq!(tokens[1].kind, TokenKind::Identifier);
		assert_eq!(tokens[1].fragment.text(), "px");
	}
}
