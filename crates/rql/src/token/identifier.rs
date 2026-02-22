// SPDX-License-Identifier: AGPL-3.0-or-later
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
		token::{token::TokenKind, tokenize},
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
}
