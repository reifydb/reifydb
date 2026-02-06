// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{
	cursor::Cursor,
	identifier::{is_identifier_char, is_identifier_start},
	token::{Token, TokenKind},
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ParameterKind {
	Positional(u32), // $1, $2, etc.
	Named,           // $name, $user_id, etc.
}

/// Scan for a parameter token ($1, $name, etc.)
pub fn scan_parameter<'b>(cursor: &mut Cursor<'b>) -> Option<Token<'b>> {
	if cursor.peek() != Some('$') {
		return None;
	}

	let state = cursor.save_state();
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	cursor.consume(); // consume '$'

	// Check for positional parameter ($1, $2, etc.)
	if let Some(ch) = cursor.peek() {
		if ch.is_ascii_digit() {
			let num_str = cursor.consume_while(|c| c.is_ascii_digit());
			if let Ok(num) = num_str.parse::<u32>() {
				if num > 0 {
					return Some(Token {
						kind: TokenKind::Parameter(ParameterKind::Positional(num)),
						fragment: cursor.make_fragment(start_pos, start_line, start_column),
					});
				}
			}
			// $0 is invalid, restore state
			cursor.restore_state(state);
			return None;
		}

		// Check for named parameter ($name, $user_id, etc.)
		if is_identifier_start(ch) {
			cursor.consume_while(is_identifier_char);
			return Some(Token {
				kind: TokenKind::Parameter(ParameterKind::Named),
				fragment: cursor.make_fragment(start_pos, start_line, start_column),
			});
		}
	}

	// Just a $ by itself, restore state
	cursor.restore_state(state);
	None
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::bump::Bump;
	use crate::token::tokenize;

	#[test]
	fn test_positional_parameter() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "$1").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Parameter(ParameterKind::Positional(1)));
		assert_eq!(tokens[0].fragment.text(), "$1");

		let tokens = tokenize(&bump, "$42").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Parameter(ParameterKind::Positional(42)));
		assert_eq!(tokens[0].fragment.text(), "$42");
	}

	#[test]
	fn test_named_parameter() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "$name").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Parameter(ParameterKind::Named));
		assert_eq!(tokens[0].fragment.text(), "$name");

		let tokens = tokenize(&bump, "$user_id").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Parameter(ParameterKind::Named));
		assert_eq!(tokens[0].fragment.text(), "$user_id");

		let tokens = tokenize(&bump, "$_private").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Parameter(ParameterKind::Named));
		assert_eq!(tokens[0].fragment.text(), "$_private");
	}

	#[test]
	fn test_invalid_parameters() {
		let bump = Bump::new();
		// $0 is not valid - should be parsed as $ and 0
		let result = tokenize(&bump, "$0");
		assert!(result.is_err()
			|| result.as_ref().unwrap()[0].kind != TokenKind::Parameter(ParameterKind::Positional(0)));

		// $ alone is not valid
		let result = tokenize(&bump, "$");
		assert!(result.is_err()
			|| (result.is_ok()
				&& result.unwrap().iter().all(|t| !matches!(t.kind, TokenKind::Parameter(_)))));

		// $123name is parsed as $123 followed by name
		let tokens = tokenize(&bump, "$123name").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Parameter(ParameterKind::Positional(123)));
		assert_eq!(tokens[0].fragment.text(), "$123");
		assert_eq!(tokens[1].kind, TokenKind::Identifier);
		assert_eq!(tokens[1].fragment.text(), "name");
	}
}
