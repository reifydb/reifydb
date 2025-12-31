// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Temporal literal scanner (dates/times starting with @).

use super::LiteralKind;
use crate::token::{
	cursor::Cursor,
	error::LexError,
	span::Span,
	token::{Token, TokenKind},
};

/// Scan for temporal literal (dates/times starting with @).
///
/// The cursor should be positioned at '@'.
/// Returns a token with span excluding the '@' prefix.
pub fn scan_temporal(cursor: &mut Cursor, start: usize, start_line: u32, start_column: u32) -> Result<Token, LexError> {
	cursor.advance(); // consume '@'

	let content_start = cursor.position();

	// Consume temporal characters: alphanumeric, -, :, ., +, /
	cursor.advance_while(|c| c.is_ascii_alphanumeric() || c == '-' || c == ':' || c == '.' || c == '+' || c == '/');

	let content_end = cursor.position();

	if content_start == content_end {
		// Just @ without any content - treat as unexpected character
		let span = cursor.span_from(start, start_line, start_column);
		return Err(LexError::UnexpectedChar {
			ch: '@',
			line: start_line,
			column: start_column,
			span,
		});
	}

	// Create span for content only (excluding @)
	let span = Span::new(
		content_start as u32,
		content_end as u32,
		start_line,
		start_column + 1, // +1 for '@'
	);

	Ok(Token::new(TokenKind::Literal(LiteralKind::Temporal), span))
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use crate::token::{LexError, Lexer, LiteralKind, Token, TokenKind};

	fn tokenize(source: &str) -> Result<Vec<Token>, LexError> {
		let bump = Bump::new();
		let result = Lexer::new(source, &bump).tokenize()?;
		Ok(result.tokens.into_iter().collect())
	}

	fn tokenize_with_text(source: &str) -> Result<(Vec<Token>, String), LexError> {
		let bump = Bump::new();
		let result = Lexer::new(source, &bump).tokenize()?;
		let source_copy = result.source.to_string();
		Ok((result.tokens.into_iter().collect(), source_copy))
	}

	#[test]
	fn test_temporal_date() {
		let (tokens, source) = tokenize_with_text("@2024-01-15").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Temporal)));
		assert_eq!(tokens[0].text(&source), "2024-01-15");
	}

	#[test]
	fn test_temporal_datetime() {
		let (tokens, source) = tokenize_with_text("@2024-01-15T10:30:00").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Temporal)));
		assert_eq!(tokens[0].text(&source), "2024-01-15T10:30:00");
	}

	#[test]
	fn test_temporal_with_timezone() {
		let (tokens, source) = tokenize_with_text("@2024-01-15T10:30:00+05:30").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Temporal)));
		assert_eq!(tokens[0].text(&source), "2024-01-15T10:30:00+05:30");
	}

	#[test]
	fn test_temporal_time_only() {
		let (tokens, source) = tokenize_with_text("@10:30:00").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Temporal)));
		assert_eq!(tokens[0].text(&source), "10:30:00");
	}

	#[test]
	fn test_temporal_with_microseconds() {
		let (tokens, source) = tokenize_with_text("@2024-01-15T10:30:00.123456").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Temporal)));
		assert_eq!(tokens[0].text(&source), "2024-01-15T10:30:00.123456");
	}

	#[test]
	fn test_temporal_alternative_format() {
		let (tokens, source) = tokenize_with_text("@2024/01/15").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Temporal)));
		assert_eq!(tokens[0].text(&source), "2024/01/15");
	}

	#[test]
	fn test_temporal_with_trailing() {
		let (tokens, source) = tokenize_with_text("@2024-01-15 rest").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Temporal)));
		assert_eq!(tokens[0].text(&source), "2024-01-15");
		assert!(matches!(tokens[1].kind, TokenKind::Identifier));
		assert_eq!(tokens[1].text(&source), "rest");
	}

	#[test]
	fn test_invalid_temporal() {
		use crate::token::LexError;

		// Just @ without content should fail
		let result = tokenize("@");
		assert!(matches!(
			result,
			Err(LexError::UnexpectedChar {
				ch: '@',
				..
			})
		));

		// @ followed by space should fail
		let result = tokenize("@ 2024");
		assert!(matches!(
			result,
			Err(LexError::UnexpectedChar {
				ch: '@',
				..
			})
		));
	}
}
