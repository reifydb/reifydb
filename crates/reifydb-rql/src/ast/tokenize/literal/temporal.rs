// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::OwnedFragment;

use crate::ast::{
	lex::{Literal, Token, TokenKind},
	tokenize::cursor::Cursor,
};

/// Scan for temporal literal (dates/times starting with @)
pub fn scan_temporal(cursor: &mut Cursor) -> Option<Token> {
	if cursor.peek() != Some('@') {
		return None;
	}

	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	cursor.consume(); // consume '@'

	// Accept any sequence of characters that could be part of a temporal
	// literal. This includes letters, digits, colons, hyphens, dots, +, -,
	// /, T, etc.
	let content = cursor.consume_while(|c| {
		c.is_ascii_alphanumeric()
			|| c == '-' || c == ':'
			|| c == '.' || c == '+'
			|| c == '/' || c == 'T'
	});

	if content.is_empty() {
		// Just @ without any content - backtrack
		// We already consumed @, so go back one position
		// Actually, we can't backtrack easily here, so just return None
		// The @ will be caught as an unexpected character
		return None;
	}

	// Create fragment with the content (excluding @)
	let fragment =
		cursor.make_fragment(start_pos, start_line, start_column);
	// The fragment includes the @, but we want just the temporal content
	let text_value = fragment.fragment();
	let text_without_at = if text_value.starts_with('@') {
		&text_value[1..]
	} else {
		text_value
	}
	.to_string();
	let fragment = OwnedFragment::Statement {
		text: text_without_at,
		line: fragment.line(),
		column: fragment.column(),
	};

	Some(Token {
		kind: TokenKind::Literal(Literal::Temporal),
		fragment,
	})
}

#[cfg(test)]
mod tests {
	use Literal::Temporal;

	use super::*;
	use crate::ast::tokenize::tokenize;

	#[test]
	fn test_temporal_date() {
		let tokens = tokenize("@2024-01-15").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Temporal));
		assert_eq!(tokens[0].fragment.fragment(), "2024-01-15");
	}

	#[test]
	fn test_temporal_datetime() {
		let tokens = tokenize("@2024-01-15T10:30:00").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Temporal));
		assert_eq!(
			tokens[0].fragment.fragment(),
			"2024-01-15T10:30:00"
		);
	}

	#[test]
	fn test_temporal_with_timezone() {
		let tokens = tokenize("@2024-01-15T10:30:00+05:30").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Temporal));
		assert_eq!(
			tokens[0].fragment.fragment(),
			"2024-01-15T10:30:00+05:30"
		);
	}

	#[test]
	fn test_temporal_time_only() {
		let tokens = tokenize("@10:30:00").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Temporal));
		assert_eq!(tokens[0].fragment.fragment(), "10:30:00");
	}

	#[test]
	fn test_temporal_with_microseconds() {
		let tokens = tokenize("@2024-01-15T10:30:00.123456").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Temporal));
		assert_eq!(
			tokens[0].fragment.fragment(),
			"2024-01-15T10:30:00.123456"
		);
	}

	#[test]
	fn test_temporal_alternative_format() {
		let tokens = tokenize("@2024/01/15").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Temporal));
		assert_eq!(tokens[0].fragment.fragment(), "2024/01/15");
	}

	#[test]
	fn test_temporal_with_trailing() {
		let tokens = tokenize("@2024-01-15 rest").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Temporal));
		assert_eq!(tokens[0].fragment.fragment(), "2024-01-15");
		assert_eq!(tokens[1].kind, TokenKind::Identifier);
		assert_eq!(tokens[1].fragment.fragment(), "rest");
	}

	#[test]
	fn test_invalid_temporal() {
		// Just @ without content should fail to tokenize
		let result = tokenize("@");
		assert!(result.is_err(), "@ alone should fail to tokenize");

		// @ followed by invalid characters should fail
		let result = tokenize("@#invalid");
		assert!(
			result.is_err(),
			"@# should fail to tokenize as # is not valid"
		);

		// @ followed by space should fail since @ alone is not valid
		let result = tokenize("@ 2024");
		assert!(
			result.is_err(),
			"@ followed by space should fail to tokenize"
		);
	}
}
