// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::tokenize::{Literal::Text, Token, TokenKind, cursor::Cursor};

/// Scan for a text literal ('...' or "...")
pub fn scan_text(cursor: &mut Cursor) -> Option<Token> {
	let quote = cursor.peek()?;
	if quote != '\'' && quote != '"' {
		return None;
	}

	let _start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	cursor.consume(); // consume opening quote

	let text_start = cursor.pos();

	while let Some(ch) = cursor.peek() {
		if ch == quote {
			let text_end = cursor.pos();
			cursor.consume(); // consume closing quote

			return Some(Token {
				kind: TokenKind::Literal(Text),
				fragment: cursor.make_utf8_fragment(
					text_start,
					text_end,
					start_line,
					start_column + 1, /* +1 for opening
					                   * quote */
				),
			});
		}

		cursor.consume();
	}

	None // Unterminated string
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ast::tokenize::{Literal::Number, tokenize};

	#[test]
	fn test_text_single_quotes() {
		let tokens = tokenize("'hello'").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Text));
		assert_eq!(tokens[0].fragment.text(), "hello");
	}

	#[test]
	fn test_text_double_quotes() {
		let tokens = tokenize("\"hello\"").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Text));
		assert_eq!(tokens[0].fragment.text(), "hello");
	}

	#[test]
	fn test_text_single_quotes_with_double_inside() {
		let tokens = tokenize("'some text\"xx\"no problem'").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Text));
		assert_eq!(tokens[0].fragment.text(), "some text\"xx\"no problem");
	}

	#[test]
	fn test_text_double_quotes_with_single_inside() {
		let tokens = tokenize("\"some text'xx'no problem\"").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Text));
		assert_eq!(tokens[0].fragment.text(), "some text'xx'no problem");
	}

	#[test]
	fn test_text_with_trailing() {
		let tokens = tokenize("'data' 123").unwrap();
		assert_eq!(tokens[0].fragment.text(), "data");
		assert_eq!(tokens[1].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[1].fragment.text(), "123");
	}

	#[test]
	fn test_text_double_quotes_with_trailing() {
		let tokens = tokenize("\"data\" 123").unwrap();
		assert_eq!(tokens[0].fragment.text(), "data");
		assert_eq!(tokens[1].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[1].fragment.text(), "123");
	}

	#[test]
	fn test_text_single_unterminated_fails() {
		let tokens = tokenize("'not closed");
		// Should fail or return no text token
		assert!(tokens.is_err() || tokens.unwrap().iter().all(|t| !matches!(t.kind, TokenKind::Literal(Text))));
	}

	#[test]
	fn test_text_double_unterminated_fails() {
		let tokens = tokenize("\"not closed");
		// Should fail or return no text token
		assert!(tokens.is_err() || tokens.unwrap().iter().all(|t| !matches!(t.kind, TokenKind::Literal(Text))));
	}

	#[test]
	fn test_text_empty_single_quotes() {
		let tokens = tokenize("''").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Text));
		assert_eq!(tokens[0].fragment.text(), "");
	}

	#[test]
	fn test_text_empty_double_quotes() {
		let tokens = tokenize("\"\"").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Text));
		assert_eq!(tokens[0].fragment.text(), "");
	}

	#[test]
	fn test_text_mixed_quotes_comptokenize() {
		let tokens = tokenize("'He said \"Hello\" and she replied \"Hi\"'").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Text));
		assert_eq!(tokens[0].fragment.text(), "He said \"Hello\" and she replied \"Hi\"");
	}

	#[test]
	fn test_text_multiple_nested_quotes() {
		let tokens = tokenize("\"It's a 'nice' day, isn't it?\"").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Text));
		assert_eq!(tokens[0].fragment.text(), "It's a 'nice' day, isn't it?");
	}
}
