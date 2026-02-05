// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{
	cursor::Cursor,
	identifier::{is_identifier_char, is_identifier_start},
	token::{Token, TokenKind},
};

/// Scan for a variable token ($variable_name)
pub fn scan_variable(cursor: &mut Cursor) -> Option<Token> {
	if cursor.peek() != Some('$') {
		return None;
	}

	let state = cursor.save_state();
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	cursor.consume(); // consume '$'

	// Check for variable name ($variable_name)
	if let Some(ch) = cursor.peek() {
		// Variables can start with letter, underscore, or digit
		if is_identifier_start(ch) || ch.is_ascii_digit() {
			cursor.consume_while(is_identifier_char);
			return Some(Token {
				kind: TokenKind::Variable,
				fragment: cursor.make_fragment(start_pos, start_line, start_column),
			});
		}
	}

	// Not a variable, restore state
	cursor.restore_state(state);
	None
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::token::tokenize;

	#[test]
	fn test_variable_basic() {
		let tokens = tokenize("$name").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Variable);
		assert_eq!(tokens[0].fragment.text(), "$name");

		let tokens = tokenize("$user_id").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Variable);
		assert_eq!(tokens[0].fragment.text(), "$user_id");

		let tokens = tokenize("$_private").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Variable);
		assert_eq!(tokens[0].fragment.text(), "$_private");
	}

	#[test]
	fn test_variable_with_numbers() {
		let tokens = tokenize("$var123").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Variable);
		assert_eq!(tokens[0].fragment.text(), "$var123");

		let tokens = tokenize("$test_2").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Variable);
		assert_eq!(tokens[0].fragment.text(), "$test_2");
	}

	#[test]
	fn test_numeric_variables() {
		// $1, $2 are now variables too (no more parameters)
		let tokens = tokenize("$1").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Variable);
		assert_eq!(tokens[0].fragment.text(), "$1");

		let tokens = tokenize("$42").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Variable);
		assert_eq!(tokens[0].fragment.text(), "$42");
	}
}
