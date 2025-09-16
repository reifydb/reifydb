// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::diagnostic::ast;

mod cursor;
mod identifier;
mod keyword;
mod literal;
mod operator;
mod parameter;
mod scanner;
mod separator;
mod token;

use cursor::Cursor;
use reifydb_type::Error;
use scanner::{scan_identifier, scan_keyword, scan_literal, scan_operator, scan_parameter, scan_separator};
pub use token::{Keyword, Literal, Operator, ParameterKind, Separator, Token, TokenKind};

/// Tokenize the input string into a vector of tokens
pub fn tokenize<'a>(input: &'a str) -> crate::Result<Vec<Token<'a>>> {
	let mut cursor = Cursor::new(input);
	// Estimate token count: rough heuristic of 1 token per 6 characters
	// with minimum of 8 and maximum reasonable limit
	let estimated_tokens = (input.len() / 6).max(8).min(2048);
	let mut tokens = Vec::with_capacity(estimated_tokens);

	while !cursor.is_eof() {
		// Skip whitespace at the beginning of each token
		cursor.skip_whitespace();

		if cursor.is_eof() {
			break;
		}

		// Character-based dispatch for better performance
		let token = match cursor.peek() {
			Some(ch) => match ch {
				// Parameters start with $
				'$' => scan_parameter(&mut cursor),

				// String literals
				'\'' | '"' => scan_literal(&mut cursor),

				// Numbers
				'0'..='9' => scan_literal(&mut cursor),

				// Dot could be operator or start of decimal
				// literal
				'.' => {
					// Check if followed by digit - if so,
					// try literal first
					if cursor.peek_ahead(1).map_or(false, |ch| ch.is_ascii_digit()) {
						scan_literal(&mut cursor).or_else(|| scan_operator(&mut cursor))
					} else {
						scan_operator(&mut cursor).or_else(|| scan_literal(&mut cursor))
					}
				}

				// Pure punctuation operators
				'(' | ')' | '[' | ']' | '{' | '}' | '+' | '*' | '/' | '^' | '%' | '?' => {
					scan_operator(&mut cursor)
				}

				// Multi-char operators starting with these
				// chars - try operator first
				'<' | '>' | ':' | '&' | '|' | '=' | '!' => scan_operator(&mut cursor),

				// Minus could be operator or negative number
				'-' => scan_operator(&mut cursor).or_else(|| scan_literal(&mut cursor)),

				// Separators
				',' | ';' => scan_separator(&mut cursor),

				// Letters could be keywords, literals
				// (true/false/undefined), word operators, or
				// identifiers
				'a'..='z' | 'A'..='Z' | '_' => {
					// Try in order: keyword, literal,
					// operator, identifier
					scan_keyword(&mut cursor)
						.or_else(|| scan_literal(&mut cursor))
						.or_else(|| scan_operator(&mut cursor))
						.or_else(|| scan_identifier(&mut cursor))
				}

				// Everything else - try all scanners in order
				_ => scan_literal(&mut cursor)
					.or_else(|| scan_operator(&mut cursor))
					.or_else(|| scan_parameter(&mut cursor))
					.or_else(|| scan_identifier(&mut cursor))
					.or_else(|| scan_separator(&mut cursor)),
			},
			None => None,
		};

		match token {
			Some(tok) => tokens.push(tok),
			None => {
				// Unable to tokenize - report error with
				// current character
				let ch = cursor.peek().unwrap_or('?');
				return Err(Error(ast::tokenize_error(format!(
					"Unexpected character '{}' at line {}, column {}",
					ch,
					cursor.line(),
					cursor.column()
				))));
			}
		}
	}

	Ok(tokens)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_tokenize_simple() {
		let tokens = tokenize("MAP * FROM users").unwrap();
		assert_eq!(tokens.len(), 4);
		assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Map));
		assert_eq!(tokens[1].kind, TokenKind::Operator(Operator::Asterisk));
		assert_eq!(tokens[2].kind, TokenKind::Keyword(Keyword::From));
		assert_eq!(tokens[3].kind, TokenKind::Identifier);
	}

	#[test]
	fn test_tokenize_with_whitespace() {
		let tokens = tokenize("   MAP   *   FROM   users   ").unwrap();
		assert_eq!(tokens.len(), 4);
		assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Map));
		assert_eq!(tokens[1].kind, TokenKind::Operator(Operator::Asterisk));
		assert_eq!(tokens[2].kind, TokenKind::Keyword(Keyword::From));
		assert_eq!(tokens[3].kind, TokenKind::Identifier);
	}

	#[test]
	fn test_tokenize_numbers() {
		let tokens = tokenize("42 3.14 0x2A 0b1010 0o777").unwrap();
		assert_eq!(tokens.len(), 5);
		assert_eq!(tokens[0].kind, TokenKind::Literal(Literal::Number));
		assert_eq!(tokens[0].value(), "42");
		assert_eq!(tokens[1].kind, TokenKind::Literal(Literal::Number));
		assert_eq!(tokens[1].value(), "3.14");
		assert_eq!(tokens[2].kind, TokenKind::Literal(Literal::Number));
		assert_eq!(tokens[2].value(), "0x2A");
		assert_eq!(tokens[3].kind, TokenKind::Literal(Literal::Number));
		assert_eq!(tokens[3].value(), "0b1010");
		assert_eq!(tokens[4].kind, TokenKind::Literal(Literal::Number));
		assert_eq!(tokens[4].value(), "0o777");
	}

	#[test]
	fn test_tokenize_strings() {
		let tokens = tokenize("'hello' \"world\"").unwrap();
		assert_eq!(tokens.len(), 2);
		assert_eq!(tokens[0].kind, TokenKind::Literal(Literal::Text));
		assert_eq!(tokens[0].value(), "hello");
		assert_eq!(tokens[1].kind, TokenKind::Literal(Literal::Text));
		assert_eq!(tokens[1].value(), "world");
	}

	#[test]
	fn test_tokenize_parameters() {
		let tokens = tokenize("$1 + $user_id").unwrap();
		assert_eq!(tokens.len(), 3);
		assert_eq!(tokens[0].kind, TokenKind::Parameter(ParameterKind::Positional(1)));
		assert_eq!(tokens[1].kind, TokenKind::Operator(Operator::Plus));
		assert_eq!(tokens[2].kind, TokenKind::Parameter(ParameterKind::Named));
	}

	#[test]
	fn test_tokenize_operators() {
		let tokens = tokenize("a >= b && c != d").unwrap();
		assert_eq!(tokens.len(), 7);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[1].kind, TokenKind::Operator(Operator::RightAngleEqual));
		assert_eq!(tokens[2].kind, TokenKind::Identifier);
		assert_eq!(tokens[3].kind, TokenKind::Operator(Operator::DoubleAmpersand));
		assert_eq!(tokens[4].kind, TokenKind::Identifier);
		assert_eq!(tokens[5].kind, TokenKind::Operator(Operator::BangEqual));
		assert_eq!(tokens[6].kind, TokenKind::Identifier);
	}

	#[test]
	fn test_tokenize_keywords_case_insensitive() {
		let tokens = tokenize("map Map MAP").unwrap();
		assert_eq!(tokens.len(), 3);
		assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Map));
		assert_eq!(tokens[1].kind, TokenKind::Keyword(Keyword::Map));
		assert_eq!(tokens[2].kind, TokenKind::Keyword(Keyword::Map));
	}

	#[test]
	fn test_tokenize_comptokenize_query() {
		let query = "MAP name, age FROM users WHERE age > 18 AND status = 'active'";
		let tokens = tokenize(query).unwrap();

		assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Map));
		assert_eq!(tokens[1].kind, TokenKind::Identifier);
		assert_eq!(tokens[2].kind, TokenKind::Separator(Separator::Comma));
		assert_eq!(tokens[3].kind, TokenKind::Identifier);
		assert_eq!(tokens[4].kind, TokenKind::Keyword(Keyword::From));
		assert_eq!(tokens[5].kind, TokenKind::Identifier);
		assert_eq!(tokens[6].kind, TokenKind::Keyword(Keyword::Where));
		assert_eq!(tokens[7].kind, TokenKind::Identifier);
		assert_eq!(tokens[8].kind, TokenKind::Operator(Operator::RightAngle));
		assert_eq!(tokens[9].kind, TokenKind::Literal(Literal::Number));
		assert_eq!(tokens[10].kind, TokenKind::Operator(Operator::And));
		assert_eq!(tokens[11].kind, TokenKind::Identifier);
		assert_eq!(tokens[12].kind, TokenKind::Operator(Operator::Equal));
		assert_eq!(tokens[13].kind, TokenKind::Literal(Literal::Text));
		assert_eq!(tokens[13].value(), "active");
	}

	#[test]
	fn test_tokenize_desc_keyword() {
		let tokens = tokenize("DESC").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Desc));
	}

	#[test]
	fn test_tokenize_single_char_identifier() {
		let tokens = tokenize("a").unwrap();
		assert_eq!(tokens.len(), 1);
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
		assert_eq!(tokens[0].value(), "a");
	}

	#[test]
	fn test_tokenize_boolean_literals() {
		let tokens = tokenize("true false TRUE FALSE").unwrap();
		assert_eq!(tokens.len(), 4);
		assert_eq!(tokens[0].kind, TokenKind::Literal(Literal::True));
		assert_eq!(tokens[1].kind, TokenKind::Literal(Literal::False));
		assert_eq!(tokens[2].kind, TokenKind::Literal(Literal::True));
		assert_eq!(tokens[3].kind, TokenKind::Literal(Literal::False));
	}
}
