// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{
	cursor::Cursor,
	token::{Token, TokenKind},
};

/// Scan for an identifier token
pub fn scan_identifier<'a>(cursor: &mut Cursor<'a>) -> Option<Token<'a>> {
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
pub fn scan_quoted_identifier<'a>(cursor: &mut Cursor<'a>) -> Option<Token<'a>> {
	if cursor.peek()? != '`' {
		return None;
	}

	let start_line = cursor.line();
	let start_column = cursor.column();
	cursor.consume(); // consume opening backtick

	let ident_start = cursor.pos();

	while let Some(ch) = cursor.peek() {
		if ch == '`' {
			let ident_end = cursor.pos();
			cursor.consume(); // consume closing backtick

			let fragment = cursor.make_utf8_fragment(ident_start, ident_end, start_line, start_column + 1);

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
mod tests {
	use crate::ast::tokenize::{TokenKind, tokenize};

	#[test]
	fn test_identifier() {
		let tokens = tokenize("user_referral").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "user_referral");
	}

	#[test]
	fn test_quoted_identifier_simple() {
		let tokens = tokenize("`my-identifier`").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "my-identifier");
	}

	#[test]
	fn test_quoted_identifier_with_spaces() {
		let tokens = tokenize("`my table name`").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "my table name");
	}

	#[test]
	fn test_quoted_identifier_with_special_chars() {
		let tokens = tokenize("`user@domain.com`").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "user@domain.com");
	}

	#[test]
	fn test_quoted_identifier_starting_with_digit() {
		let tokens = tokenize("`123-table`").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "123-table");
	}

	#[test]
	fn test_quoted_identifier_empty() {
		let tokens = tokenize("``").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.text(), "");
	}

	#[test]
	fn test_quoted_identifier_unterminated() {
		let tokens = tokenize("`unclosed");
		// Should fail or return no identifier token
		assert!(tokens.is_err() || tokens.unwrap().iter().all(|t| !matches!(t.kind, TokenKind::Identifier)));
	}
}
