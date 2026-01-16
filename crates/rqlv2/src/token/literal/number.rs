// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Number literal scanner.

use super::LiteralKind;
use crate::token::{
	cursor::Cursor,
	error::LexError,
	token::{Token, TokenKind},
};

/// Scan for a number literal.
///
/// Supports:
/// - Decimal integers: 42, 1_234_567
/// - Floats: 3.14, .5, 1.23e10
/// - Hex: 0xFF, 0xDEAD_BEEF
/// - Binary: 0b1010, 0b1111_0000
/// - Octal: 0o777, 0o12_34
/// - Scientific notation: 1.23e10, 5E-3
pub fn scan_number(cursor: &mut Cursor, start: usize, start_line: u32, start_column: u32) -> Result<Token, LexError> {
	// Check for prefixed numbers: 0x, 0b, 0o
	let prefix = cursor.peek_str(2).to_ascii_lowercase();

	if prefix == "0x" {
		cursor.advance(); // '0'
		cursor.advance(); // 'x'
		cursor.advance_while(|c| c.is_ascii_hexdigit() || c == '_');
	} else if prefix == "0b" {
		cursor.advance(); // '0'
		cursor.advance(); // 'b'
		cursor.advance_while(|c| c == '0' || c == '1' || c == '_');
	} else if prefix == "0o" {
		cursor.advance(); // '0'
		cursor.advance(); // 'o'
		cursor.advance_while(|c| ('0'..='7').contains(&c) || c == '_');
	} else {
		// Decimal number
		// Handle leading dot for floats like .5
		let has_leading_dot = cursor.peek() == Some('.');
		if has_leading_dot {
			cursor.advance();
		}

		// Integer part
		cursor.advance_while(|c| c.is_ascii_digit() || c == '_');

		// Fractional part (if not leading dot)
		if !has_leading_dot && cursor.peek() == Some('.') {
			if cursor.peek_ahead(1).map_or(false, |c| c.is_ascii_digit()) {
				cursor.advance(); // '.'
				cursor.advance_while(|c| c.is_ascii_digit() || c == '_');
			}
		}

		// Scientific notation
		if let Some('e') | Some('E') = cursor.peek() {
			cursor.advance();
			if matches!(cursor.peek(), Some('+') | Some('-')) {
				cursor.advance();
			}
			cursor.advance_while(|c| c.is_ascii_digit() || c == '_');
		}
	}

	let span = cursor.span_from(start, start_line, start_column);
	let text = span.text(cursor.source());

	// Determine if integer or float
	// Note: Hex numbers (0x...) can contain 'e' as a hex digit, so check prefix first
	let is_prefixed = text.len() >= 2
		&& text.starts_with('0')
		&& matches!(text.chars().nth(1), Some('x') | Some('X') | Some('b') | Some('B') | Some('o') | Some('O'));

	let kind = if is_prefixed {
		LiteralKind::Integer
	} else if text.contains('.') || text.to_ascii_lowercase().contains('e') {
		LiteralKind::Float
	} else {
		LiteralKind::Integer
	};

	Ok(Token::new(TokenKind::Literal(kind), span))
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::token::{lexer::Lexer, literal::LiteralKind, token::TokenKind};

	fn tokenize_with_text(
		source: &str,
	) -> Result<(Vec<crate::token::token::Token>, String), crate::token::error::LexError> {
		let bump = Bump::new();
		let result = Lexer::new(source, &bump).tokenize()?;
		let source_copy = result.source.to_string();
		Ok((result.tokens.into_iter().collect(), source_copy))
	}

	#[test]
	fn test_decimal_integer() {
		let (tokens, source) = tokenize_with_text("42").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert_eq!(tokens[0].text(&source), "42");
	}

	#[test]
	fn test_decimal_float() {
		let (tokens, source) = tokenize_with_text("3.14").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Float)));
		assert_eq!(tokens[0].text(&source), "3.14");
	}

	#[test]
	fn test_decimal_with_underscores() {
		let (tokens, source) = tokenize_with_text("1_234_567").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert_eq!(tokens[0].text(&source), "1_234_567");
	}

	#[test]
	fn test_scientific_notation() {
		let (tokens, source) = tokenize_with_text("1.23e10").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Float)));
		assert_eq!(tokens[0].text(&source), "1.23e10");

		let (tokens, source) = tokenize_with_text("5E-3").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Float)));
		assert_eq!(tokens[0].text(&source), "5E-3");
	}

	#[test]
	fn test_hex_number() {
		let (tokens, source) = tokenize_with_text("0x2A").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert_eq!(tokens[0].text(&source), "0x2A");

		let (tokens, source) = tokenize_with_text("0xDEAD_BEEF").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert_eq!(tokens[0].text(&source), "0xDEAD_BEEF");
	}

	#[test]
	fn test_binary_number() {
		let (tokens, source) = tokenize_with_text("0b1010").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert_eq!(tokens[0].text(&source), "0b1010");

		let (tokens, source) = tokenize_with_text("0b1111_0000").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert_eq!(tokens[0].text(&source), "0b1111_0000");
	}

	#[test]
	fn test_octal_number() {
		let (tokens, source) = tokenize_with_text("0o777").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert_eq!(tokens[0].text(&source), "0o777");

		let (tokens, source) = tokenize_with_text("0o12_34").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert_eq!(tokens[0].text(&source), "0o12_34");
	}

	#[test]
	fn test_leading_dot() {
		let (tokens, source) = tokenize_with_text(".5").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Float)));
		assert_eq!(tokens[0].text(&source), ".5");
	}

	#[test]
	fn test_leading_dot_decimal_standalone() {
		let (tokens, source) = tokenize_with_text(".5").unwrap();
		assert_eq!(tokens.len(), 2); // .5, EOF
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Float)));
		assert_eq!(tokens[0].text(&source), ".5");
	}

	#[test]
	fn test_number_with_trailing() {
		// Numbers directly followed by letters token as separate tokens
		let (tokens, source) = tokenize_with_text("42abc").unwrap();
		assert_eq!(tokens.len(), 3); // 42, abc, EOF
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert_eq!(tokens[0].text(&source), "42");
		assert!(matches!(tokens[1].kind, TokenKind::Identifier));
		assert_eq!(tokens[1].text(&source), "abc");

		// With proper spacing
		let (tokens, source) = tokenize_with_text("42 abc").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert_eq!(tokens[0].text(&source), "42");
	}

	#[test]
	fn test_invalid_binary() {
		// Invalid binary (contains 2) - parses as 0b10 followed by 2
		let (tokens, source) = tokenize_with_text("0b102").unwrap();
		assert_eq!(tokens[0].text(&source), "0b10");
		assert_eq!(tokens[1].text(&source), "2");
	}

	#[test]
	fn test_float_without_leading_zero() {
		let (tokens, source) = tokenize_with_text(".123").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Float)));
		assert_eq!(tokens[0].text(&source), ".123");
	}

	#[test]
	fn test_float_with_exponent_sign() {
		let (tokens, source) = tokenize_with_text("1e+10").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Float)));
		assert_eq!(tokens[0].text(&source), "1e+10");

		let (tokens, source) = tokenize_with_text("1e-10").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Float)));
		assert_eq!(tokens[0].text(&source), "1e-10");
	}
}
