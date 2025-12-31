// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{iter::Peekable, str::CharIndices};

use thiserror::Error;

use super::token::{Span, Token, TokenKind};

/// Lexer error types.
#[derive(Debug, Clone, Error)]
pub enum LexError {
	#[error("unexpected character '{ch}' at {line}:{column}")]
	UnexpectedChar {
		ch: char,
		line: u32,
		column: u32,
	},

	#[error("unterminated string at {line}:{column}")]
	UnterminatedString {
		line: u32,
		column: u32,
	},

	#[error("invalid number '{text}' at {line}:{column}")]
	InvalidNumber {
		text: String,
		line: u32,
		column: u32,
	},
}

/// Lexer for the DSL.
pub struct Lexer<'a> {
	source: &'a str,
	chars: Peekable<CharIndices<'a>>,
	position: usize,
	line: u32,
	column: u32,
	line_start: usize,
}

impl<'a> Lexer<'a> {
	/// Create a new lexer for the given source.
	pub fn new(source: &'a str) -> Self {
		Self {
			source,
			chars: source.char_indices().peekable(),
			position: 0,
			line: 1,
			column: 1,
			line_start: 0,
		}
	}

	/// Tokenize the entire source into a vector of tokens.
	pub fn tokenize(&mut self) -> Result<Vec<Token>, LexError> {
		let mut tokens = Vec::new();

		loop {
			let token = self.next_token()?;
			let is_eof = token.kind == TokenKind::Eof;
			tokens.push(token);

			if is_eof {
				break;
			}
		}

		Ok(tokens)
	}

	/// Get the next token.
	fn next_token(&mut self) -> Result<Token, LexError> {
		self.skip_whitespace();

		// Check for EOF
		let Some(&(pos, ch)) = self.chars.peek() else {
			return Ok(self.make_token(TokenKind::Eof, self.position));
		};

		let start = pos;
		let start_column = self.column;

		// Dispatch based on first character
		match ch {
			// Identifiers and keywords
			'a'..='z' | 'A'..='Z' | '_' => self.scan_identifier(),

			// Numbers
			'0'..='9' => self.scan_number(),

			// Negative numbers or minus operator
			'-' => {
				self.advance();
				if let Some(&(_, c)) = self.chars.peek() {
					if c.is_ascii_digit() {
						return self.scan_number_with_prefix(start, start_column, true);
					}
				}
				Ok(self.make_token_at(TokenKind::Minus, start, start_column))
			}

			// String literals
			'"' | '\'' => self.scan_string(ch),

			// Two-char operators
			'=' => {
				self.advance();
				if self.match_char('=') {
					Ok(self.make_token_at(TokenKind::Eq, start, start_column))
				} else {
					// Single = is assignment
					Ok(self.make_token_at(TokenKind::Assign, start, start_column))
				}
			}

			'!' => {
				self.advance();
				if self.match_char('=') {
					Ok(self.make_token_at(TokenKind::Ne, start, start_column))
				} else {
					Ok(self.make_token_at(TokenKind::Not, start, start_column))
				}
			}

			'<' => {
				self.advance();
				if self.match_char('=') {
					Ok(self.make_token_at(TokenKind::Le, start, start_column))
				} else {
					Ok(self.make_token_at(TokenKind::Lt, start, start_column))
				}
			}

			'>' => {
				self.advance();
				if self.match_char('=') {
					Ok(self.make_token_at(TokenKind::Ge, start, start_column))
				} else {
					Ok(self.make_token_at(TokenKind::Gt, start, start_column))
				}
			}

			'&' => {
				self.advance();
				if self.match_char('&') {
					Ok(self.make_token_at(TokenKind::And, start, start_column))
				} else {
					Err(LexError::UnexpectedChar {
						ch: '&',
						line: self.line,
						column: start_column,
					})
				}
			}

			'|' => {
				self.advance();
				if self.match_char('|') {
					Ok(self.make_token_at(TokenKind::Or, start, start_column))
				} else {
					Ok(self.make_token_at(TokenKind::Pipe, start, start_column))
				}
			}

			// Single-char operators and delimiters
			'+' => {
				self.advance();
				Ok(self.make_token_at(TokenKind::Plus, start, start_column))
			}

			'*' => {
				self.advance();
				Ok(self.make_token_at(TokenKind::Star, start, start_column))
			}

			'/' => {
				self.advance();
				Ok(self.make_token_at(TokenKind::Slash, start, start_column))
			}

			'(' => {
				self.advance();
				Ok(self.make_token_at(TokenKind::LParen, start, start_column))
			}

			')' => {
				self.advance();
				Ok(self.make_token_at(TokenKind::RParen, start, start_column))
			}

			'[' => {
				self.advance();
				Ok(self.make_token_at(TokenKind::LBracket, start, start_column))
			}

			']' => {
				self.advance();
				Ok(self.make_token_at(TokenKind::RBracket, start, start_column))
			}

			'{' => {
				self.advance();
				Ok(self.make_token_at(TokenKind::LBrace, start, start_column))
			}

			'}' => {
				self.advance();
				Ok(self.make_token_at(TokenKind::RBrace, start, start_column))
			}

			',' => {
				self.advance();
				Ok(self.make_token_at(TokenKind::Comma, start, start_column))
			}

			':' => {
				self.advance();
				if let Some(&(_, ':')) = self.chars.peek() {
					self.advance();
					Ok(self.make_token_at(TokenKind::ColonColon, start, start_column))
				} else {
					Ok(self.make_token_at(TokenKind::Colon, start, start_column))
				}
			}

			'$' => {
				self.advance();
				Ok(self.make_token_at(TokenKind::Dollar, start, start_column))
			}

			'.' => {
				self.advance();
				Ok(self.make_token_at(TokenKind::Dot, start, start_column))
			}

			_ => Err(LexError::UnexpectedChar {
				ch,
				line: self.line,
				column: self.column,
			}),
		}
	}

	/// Skip whitespace characters.
	fn skip_whitespace(&mut self) {
		while let Some(&(_, ch)) = self.chars.peek() {
			match ch {
				' ' | '\t' | '\r' => {
					self.advance();
				}
				'\n' => {
					self.advance();
					self.line += 1;
					self.column = 1;
					self.line_start = self.position;
				}
				'#' => {
					// Skip comment until end of line
					self.advance();
					while let Some(&(_, c)) = self.chars.peek() {
						if c == '\n' {
							break;
						}
						self.advance();
					}
				}
				_ => break,
			}
		}
	}

	/// Scan an identifier or keyword.
	fn scan_identifier(&mut self) -> Result<Token, LexError> {
		let start = self.position;
		let start_column = self.column;

		while let Some(&(_, ch)) = self.chars.peek() {
			if ch.is_alphanumeric() || ch == '_' {
				self.advance();
			} else {
				break;
			}
		}

		let text = &self.source[start..self.position];

		// Check for keyword
		let kind = TokenKind::from_keyword(text).unwrap_or(TokenKind::Ident);

		Ok(Token::new(kind, Span::new(start, self.position, self.line, start_column), text.to_string()))
	}

	/// Scan a number (integer or float).
	fn scan_number(&mut self) -> Result<Token, LexError> {
		let start = self.position;
		let start_column = self.column;
		self.scan_number_with_prefix(start, start_column, false)
	}

	/// Scan a number with optional negative prefix.
	fn scan_number_with_prefix(
		&mut self,
		start: usize,
		start_column: u32,
		negative: bool,
	) -> Result<Token, LexError> {
		// num_start is always current position (after any '-' has been consumed)
		let num_start = self.position;

		// Consume digits
		while let Some(&(_, ch)) = self.chars.peek() {
			if ch.is_ascii_digit() {
				self.advance();
			} else {
				break;
			}
		}

		// Check for decimal point
		let is_float = if let Some(&(_, '.')) = self.chars.peek() {
			// Peek ahead to see if there's a digit after the dot
			let mut temp_chars = self.source[self.position..].char_indices().peekable();
			temp_chars.next(); // skip the dot
			if let Some((_, c)) = temp_chars.peek() {
				if c.is_ascii_digit() {
					self.advance(); // consume the dot
					// Consume fractional digits
					while let Some(&(_, ch)) = self.chars.peek() {
						if ch.is_ascii_digit() {
							self.advance();
						} else {
							break;
						}
					}
					true
				} else {
					false
				}
			} else {
				false
			}
		} else {
			false
		};

		let text = if negative {
			format!("-{}", &self.source[num_start..self.position])
		} else {
			self.source[start..self.position].to_string()
		};

		let kind = if is_float {
			let value: f64 = text.parse().map_err(|_| LexError::InvalidNumber {
				text: text.clone(),
				line: self.line,
				column: start_column,
			})?;
			TokenKind::Float(value)
		} else {
			let value: i64 = text.parse().map_err(|_| LexError::InvalidNumber {
				text: text.clone(),
				line: self.line,
				column: start_column,
			})?;
			TokenKind::Int(value)
		};

		Ok(Token::new(kind, Span::new(start, self.position, self.line, start_column), text))
	}

	/// Scan a string literal.
	fn scan_string(&mut self, quote: char) -> Result<Token, LexError> {
		let start = self.position;
		let start_column = self.column;
		let start_line = self.line;

		self.advance(); // consume opening quote

		let mut value = String::new();

		loop {
			match self.chars.peek() {
				None => {
					return Err(LexError::UnterminatedString {
						line: start_line,
						column: start_column,
					});
				}
				Some(&(_, ch)) if ch == quote => {
					self.advance(); // consume closing quote
					break;
				}
				Some(&(_, '\\')) => {
					self.advance();
					match self.chars.peek() {
						Some(&(_, 'n')) => {
							value.push('\n');
							self.advance();
						}
						Some(&(_, 't')) => {
							value.push('\t');
							self.advance();
						}
						Some(&(_, 'r')) => {
							value.push('\r');
							self.advance();
						}
						Some(&(_, '\\')) => {
							value.push('\\');
							self.advance();
						}
						Some(&(_, c)) if c == quote => {
							value.push(c);
							self.advance();
						}
						_ => {
							value.push('\\');
						}
					}
				}
				Some(&(_, '\n')) => {
					return Err(LexError::UnterminatedString {
						line: start_line,
						column: start_column,
					});
				}
				Some(&(_, ch)) => {
					value.push(ch);
					self.advance();
				}
			}
		}

		let text = &self.source[start..self.position];

		Ok(Token::new(
			TokenKind::String(value),
			Span::new(start, self.position, self.line, start_column),
			text.to_string(),
		))
	}

	/// Advance to the next character and return the current one.
	fn advance(&mut self) -> Option<char> {
		if let Some((pos, ch)) = self.chars.next() {
			self.position = pos + ch.len_utf8();
			self.column += 1;
			Some(ch)
		} else {
			None
		}
	}

	/// Check if the next character matches and consume it if so.
	fn match_char(&mut self, expected: char) -> bool {
		if let Some(&(_, ch)) = self.chars.peek() {
			if ch == expected {
				self.advance();
				return true;
			}
		}
		false
	}

	/// Create a token at the current position.
	fn make_token(&self, kind: TokenKind, start: usize) -> Token {
		Token::new(
			kind,
			Span::new(start, self.position, self.line, self.column),
			self.source.get(start..self.position).unwrap_or("").to_string(),
		)
	}

	/// Create a token with explicit start position and column.
	fn make_token_at(&self, kind: TokenKind, start: usize, start_column: u32) -> Token {
		Token::new(
			kind,
			Span::new(start, self.position, self.line, start_column),
			self.source.get(start..self.position).unwrap_or("").to_string(),
		)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_lex_simple_pipeline() {
		let tokens = Lexer::new("scan users | filter age > 21").tokenize().unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Scan);
		assert_eq!(tokens[1].kind, TokenKind::Ident);
		assert_eq!(tokens[1].text, "users");
		assert_eq!(tokens[2].kind, TokenKind::Pipe);
		assert_eq!(tokens[3].kind, TokenKind::Filter);
		assert_eq!(tokens[4].kind, TokenKind::Ident);
		assert_eq!(tokens[4].text, "age");
		assert_eq!(tokens[5].kind, TokenKind::Gt);
		assert_eq!(tokens[6].kind, TokenKind::Int(21));
		assert_eq!(tokens[7].kind, TokenKind::Eof);
	}

	#[test]
	fn test_lex_string_literals() {
		let tokens = Lexer::new(r#"filter name == "Alice""#).tokenize().unwrap();
		assert!(matches!(
		    &tokens[3].kind,
		    TokenKind::String(s) if s == "Alice"
		));
	}

	#[test]
	fn test_lex_numbers() {
		let tokens = Lexer::new("take 100").tokenize().unwrap();
		assert!(matches!(tokens[1].kind, TokenKind::Int(100)));
	}

	#[test]
	fn test_lex_float() {
		let tokens = Lexer::new("filter score >= 3.14").tokenize().unwrap();
		// tokens: 0=filter, 1=score, 2=>=, 3=3.14, 4=Eof
		assert!(matches!(tokens[3].kind, TokenKind::Float(f) if (f - 3.14).abs() < 0.001));
	}

	#[test]
	fn test_lex_negative_number() {
		let tokens = Lexer::new("filter x > -5").tokenize().unwrap();
		// tokens: 0=filter, 1=x, 2=>, 3=-5, 4=Eof
		assert!(matches!(tokens[3].kind, TokenKind::Int(-5)));
	}

	#[test]
	fn test_lex_keywords() {
		let tokens = Lexer::new("true false null and or not").tokenize().unwrap();
		assert_eq!(tokens[0].kind, TokenKind::Bool(true));
		assert_eq!(tokens[1].kind, TokenKind::Bool(false));
		assert_eq!(tokens[2].kind, TokenKind::Null);
		assert_eq!(tokens[3].kind, TokenKind::And);
		assert_eq!(tokens[4].kind, TokenKind::Or);
		assert_eq!(tokens[5].kind, TokenKind::Not);
	}

	#[test]
	fn test_lex_error_unterminated_string() {
		let result = Lexer::new(r#"filter name == "unclosed"#).tokenize();
		assert!(matches!(result, Err(LexError::UnterminatedString { .. })));
	}
}
