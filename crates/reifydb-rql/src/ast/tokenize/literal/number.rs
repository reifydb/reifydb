// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	lex::{Literal, Token, TokenKind},
	tokenize::{cursor::Cursor, identifier::is_identifier_char},
};

/// Scan for a number literal
pub fn scan_number(cursor: &mut Cursor) -> Option<Token> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	// Check for hex (0x...)
	if cursor.peek_str(2).eq_ignore_ascii_case("0x") {
		cursor.consume();
		cursor.consume();
		let hex_part = cursor
			.consume_while(|c| c.is_ascii_hexdigit() || c == '_');
		if !hex_part.is_empty()
			&& !hex_part.starts_with('_')
			&& !hex_part.ends_with('_')
			&& !hex_part.contains("__")
		{
			return Some(Token {
				kind: TokenKind::Literal(Literal::Number),
				fragment: cursor.make_fragment(
					start_pos,
					start_line,
					start_column,
				),
			});
		}
		return None;
	}

	// Check for binary (0b...)
	if cursor.peek_str(2).eq_ignore_ascii_case("0b") {
		cursor.consume();
		cursor.consume();
		let bin_part = cursor
			.consume_while(|c| c == '0' || c == '1' || c == '_');
		if !bin_part.is_empty()
			&& !bin_part.starts_with('_')
			&& !bin_part.ends_with('_')
			&& !bin_part.contains("__")
		{
			return Some(Token {
				kind: TokenKind::Literal(Literal::Number),
				fragment: cursor.make_fragment(
					start_pos,
					start_line,
					start_column,
				),
			});
		}
		return None;
	}

	// Check for octal (0o...)
	if cursor.peek_str(2).eq_ignore_ascii_case("0o") {
		cursor.consume();
		cursor.consume();
		let oct_part = cursor
			.consume_while(|c| c >= '0' && c <= '7' || c == '_');
		if !oct_part.is_empty()
			&& !oct_part.starts_with('_')
			&& !oct_part.ends_with('_')
			&& !oct_part.contains("__")
		{
			return Some(Token {
				kind: TokenKind::Literal(Literal::Number),
				fragment: cursor.make_fragment(
					start_pos,
					start_line,
					start_column,
				),
			});
		}
		return None;
	}

	// Decimal number (including float and scientific notation)
	let state = cursor.save_state();

	// Check for leading dot (.123)
	let has_leading_dot = cursor.peek() == Some('.');
	if has_leading_dot {
		cursor.consume();
		if !cursor.peek().map_or(false, |c| c.is_ascii_digit()) {
			cursor.restore_state(state);
			return None;
		}
	} else if !cursor.peek().map_or(false, |c| c.is_ascii_digit()) {
		return None;
	}

	// Integer part (if no leading dot)
	if !has_leading_dot {
		cursor.consume_while(|c| c.is_ascii_digit() || c == '_');
	}

	// Fractional part
	if cursor.peek() == Some('.') && !has_leading_dot {
		if cursor.peek_ahead(1).map_or(false, |c| c.is_ascii_digit()) {
			cursor.consume(); // consume '.'
			cursor.consume_while(|c| {
				c.is_ascii_digit() || c == '_'
			});
		}
	} else if has_leading_dot {
		// Already consumed the dot
		cursor.consume_while(|c| c.is_ascii_digit() || c == '_');
	}

	// Scientific notation (e/E)
	if let Some(e) = cursor.peek() {
		if e == 'e' || e == 'E' {
			cursor.consume();
			if let Some(sign) = cursor.peek() {
				if sign == '+' || sign == '-' {
					cursor.consume();
				}
			}
			let exp_part = cursor.consume_while(|c| {
				c.is_ascii_digit() || c == '_'
			});
			if exp_part.is_empty() {
				// Invalid scientific notation
				cursor.restore_state(state);
				return None;
			}
		}
	}

	// Make sure we consumed something
	if cursor.pos() == start_pos {
		return None;
	}

	// Check that next character isn't an identifier continuation
	if cursor.peek().map_or(false, |c| is_identifier_char(c) && c != '.') {
		cursor.restore_state(state);
		return None;
	}

	Some(Token {
		kind: TokenKind::Literal(Literal::Number),
		fragment: cursor.make_fragment(
			start_pos,
			start_line,
			start_column,
		),
	})
}

#[cfg(test)]
mod tests {
	use Literal::Number;

	use super::*;
	use crate::ast::tokenize::tokenize;

	#[test]
	fn test_decimal_integer() {
		let tokens = tokenize("42").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), "42");
	}

	#[test]
	fn test_decimal_float() {
		let tokens = tokenize("3.14").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), "3.14");
	}

	#[test]
	fn test_decimal_with_underscores() {
		let tokens = tokenize("1_234_567").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), "1_234_567");
	}

	#[test]
	fn test_scientific_notation() {
		let tokens = tokenize("1.23e10").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), "1.23e10");

		let tokens = tokenize("5E-3").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), "5E-3");
	}

	#[test]
	fn test_hex_number() {
		let tokens = tokenize("0x2A").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), "0x2A");

		let tokens = tokenize("0xDEAD_BEEF").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), "0xDEAD_BEEF");
	}

	#[test]
	fn test_binary_number() {
		let tokens = tokenize("0b1010").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), "0b1010");

		let tokens = tokenize("0b1111_0000").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), "0b1111_0000");
	}

	#[test]
	fn test_octal_number() {
		let tokens = tokenize("0o777").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), "0o777");

		let tokens = tokenize("0o12_34").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), "0o12_34");
	}

	#[test]
	fn test_leading_dot() {
		let tokens = tokenize(".5").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), ".5");
	}

	#[test]
	fn test_number_with_trailing() {
		// Numbers directly followed by letters are invalid tokens
		// "42abc" cannot be tokenized as it's neither a number nor an
		// identifier
		let result = tokenize("42abc");
		assert!(result.is_err(), "42abc should fail to tokenize");

		// With proper spacing, it works
		let tokens = tokenize("42 abc").unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Literal(Number));
		assert_eq!(tokens[0].fragment.fragment(), "42");
		assert_eq!(tokens[1].kind, TokenKind::Identifier);
		assert_eq!(tokens[1].fragment.fragment(), "abc");
	}

	#[test]
	fn test_invalid_numbers() {
		// Invalid hex (starts with _)
		let result = tokenize("0x_FF");
		assert!(result.is_err()
			|| result.unwrap()[0].fragment.fragment() != "0x_FF");

		// Invalid binary (contains 2)
		let result = tokenize("0b102");
		assert!(result.is_ok()); // Will be parsed as 0b10 followed by 2
		let tokens = result.unwrap();
		assert_eq!(tokens[0].fragment.fragment(), "0b10");
		assert_eq!(tokens[1].fragment.fragment(), "2");
	}
}
