// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::cursor::Cursor;
use crate::ast::lex::{Token, TokenKind};

/// Scan for an identifier token
pub fn scan_identifier(cursor: &mut Cursor) -> Option<Token> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	if !cursor.peek().map_or(false, is_identifier_start) {
		return None;
	}

	cursor.consume_while(is_identifier_char);

	Some(Token {
		kind: TokenKind::Identifier,
		fragment: cursor.make_fragment(
			start_pos,
			start_line,
			start_column,
		),
	})
}

pub fn is_identifier_start(ch: char) -> bool {
	ch.is_ascii_alphabetic() || ch == '_'
}

pub fn is_identifier_char(ch: char) -> bool {
	ch.is_ascii_alphanumeric() || ch == '_'
}

#[cfg(test)]
mod tests {
	use crate::ast::{lex::TokenKind, tokenize::tokenize};

	#[test]
	fn test_identifier() {
		let tokens = tokenize("user_referral").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].fragment.fragment(), "user_referral");
	}
}
