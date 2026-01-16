// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! String literal scanner.

use super::LiteralKind;
use crate::token::{
	cursor::Cursor,
	error::LexError,
	span::Span,
	token::{Token, TokenKind},
};

/// Scan for a string literal ('...' or "...").
///
/// The cursor should be positioned at the opening quote.
/// Returns a token with span containing only the string content (excluding quotes).
/// Escape sequences are skipped but not processed.
pub fn scan_string(
	cursor: &mut Cursor,
	quote: char,
	start: usize,
	start_line: u32,
	start_column: u32,
) -> Result<Token, LexError> {
	cursor.advance(); // consume opening quote

	let content_start = cursor.position();

	loop {
		match cursor.peek() {
			None | Some('\n') => {
				let span = cursor.span_from(start, start_line, start_column);
				return Err(LexError::UnterminatedString {
					line: start_line,
					column: start_column,
					span,
				});
			}
			Some(ch) if ch == quote => {
				// Create span for content only (excluding quotes)
				let span = Span::new(
					content_start as u32,
					cursor.position() as u32,
					start_line,
					start_column + 1,
				);
				cursor.advance(); // consume closing quote
				return Ok(Token::new(TokenKind::Literal(LiteralKind::String), span));
			}
			Some('\\') => {
				// Skip escape sequence (backslash + next char) without processing
				cursor.advance();
				if cursor.peek().is_some() {
					cursor.advance();
				}
			}
			Some(_) => {
				cursor.advance();
			}
		}
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::token::{error::LexError, lexer::Lexer, literal::LiteralKind, token::TokenKind};

	fn tokenize_with_text(source: &str) -> Result<(Vec<crate::token::token::Token>, String), LexError> {
		let bump = Bump::new();
		let result = Lexer::new(source, &bump).tokenize()?;
		let source_copy = result.source.to_string();
		Ok((result.tokens.into_iter().collect(), source_copy))
	}

	fn tokenize(source: &str) -> Result<Vec<crate::token::token::Token>, LexError> {
		let bump = Bump::new();
		let result = Lexer::new(source, &bump).tokenize()?;
		Ok(result.tokens.into_iter().collect())
	}

	#[test]
	fn test_text_single_quotes() {
		let (tokens, source) = tokenize_with_text("'hello'").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::String)));
		assert_eq!(tokens[0].text(&source), "hello");
	}

	#[test]
	fn test_text_double_quotes() {
		let (tokens, source) = tokenize_with_text("\"hello\"").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::String)));
		assert_eq!(tokens[0].text(&source), "hello");
	}

	#[test]
	fn test_text_single_quotes_with_double_inside() {
		let (tokens, source) = tokenize_with_text("'some text\"xx\"no problem'").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::String)));
		assert_eq!(tokens[0].text(&source), "some text\"xx\"no problem");
	}

	#[test]
	fn test_text_double_quotes_with_single_inside() {
		let (tokens, source) = tokenize_with_text("\"some text'xx'no problem\"").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::String)));
		assert_eq!(tokens[0].text(&source), "some text'xx'no problem");
	}

	#[test]
	fn test_text_with_trailing() {
		let (tokens, source) = tokenize_with_text("'data' 123").unwrap();
		assert_eq!(tokens[0].text(&source), "data");
		assert!(matches!(tokens[1].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert_eq!(tokens[1].text(&source), "123");
	}

	#[test]
	fn test_text_double_quotes_with_trailing() {
		let (tokens, source) = tokenize_with_text("\"data\" 123").unwrap();
		assert_eq!(tokens[0].text(&source), "data");
		assert!(matches!(tokens[1].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert_eq!(tokens[1].text(&source), "123");
	}

	#[test]
	fn test_text_single_unterminated_fails() {
		let result = tokenize("'not closed");
		assert!(matches!(result, Err(LexError::UnterminatedString { .. })));
	}

	#[test]
	fn test_text_double_unterminated_fails() {
		let result = tokenize("\"not closed");
		assert!(matches!(result, Err(LexError::UnterminatedString { .. })));
	}

	#[test]
	fn test_text_empty_single_quotes() {
		let (tokens, source) = tokenize_with_text("''").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::String)));
		assert_eq!(tokens[0].text(&source), "");
	}

	#[test]
	fn test_text_empty_double_quotes() {
		let (tokens, source) = tokenize_with_text("\"\"").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::String)));
		assert_eq!(tokens[0].text(&source), "");
	}

	#[test]
	fn test_text_mixed_quotes_complex() {
		let (tokens, source) = tokenize_with_text("'He said \"Hello\" and she replied \"Hi\"'").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::String)));
		assert_eq!(tokens[0].text(&source), "He said \"Hello\" and she replied \"Hi\"");
	}

	#[test]
	fn test_text_multiple_nested_quotes() {
		let (tokens, source) = tokenize_with_text("\"It's a 'nice' day, isn't it?\"").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::String)));
		assert_eq!(tokens[0].text(&source), "It's a 'nice' day, isn't it?");
	}

	#[test]
	fn test_text_with_escape_sequence() {
		// Escape sequences are skipped but not processed
		let (tokens, source) = tokenize_with_text("'hello\\nworld'").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::String)));
		assert_eq!(tokens[0].text(&source), "hello\\nworld");
	}

	#[test]
	fn test_text_with_escaped_quote() {
		let (tokens, source) = tokenize_with_text("'it\\'s fine'").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::String)));
		assert_eq!(tokens[0].text(&source), "it\\'s fine");
	}
}
