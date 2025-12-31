// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Lexer implementation.

use bumpalo::{Bump, collections::Vec as BumpVec};

use super::{
	cursor::Cursor,
	error::LexError,
	keyword::lookup_keyword,
	literal::LiteralKind,
	operator::{Operator, lookup_word_operator},
	punctuation::Punctuation,
	span::Span,
	token::{Token, TokenKind},
};

/// Result of tokenization.
pub struct TokenizeResult<'bump> {
	/// The tokens, allocated in the bump allocator.
	pub tokens: BumpVec<'bump, Token>,
	/// Original source (for extracting raw token text).
	pub source: &'bump str,
}

impl TokenizeResult<'_> {
	/// Get a token's text from the original source.
	#[inline]
	pub fn text(&self, token: &Token) -> &str {
		&self.source[token.span.start as usize..token.span.end as usize]
	}

	/// Iterator over tokens.
	pub fn iter(&self) -> impl Iterator<Item = &Token> {
		self.tokens.iter()
	}

	/// Number of tokens.
	pub fn len(&self) -> usize {
		self.tokens.len()
	}

	/// Check if empty.
	pub fn is_empty(&self) -> bool {
		self.tokens.is_empty()
	}
}

/// Lexer for RQL v2.
pub struct Lexer<'a, 'bump> {
	cursor: Cursor<'a>,
	bump: &'bump Bump,
}

impl<'a, 'bump> Lexer<'a, 'bump> {
	/// Create a new lexer.
	pub fn new(source: &'a str, bump: &'bump Bump) -> Self {
		Self {
			cursor: Cursor::new(source),
			bump,
		}
	}

	/// Tokenize the entire input.
	pub fn tokenize(mut self) -> Result<TokenizeResult<'bump>, LexError> {
		let mut tokens = BumpVec::new_in(self.bump);

		// Estimate capacity
		let estimated = (self.cursor.source().len() / 6).max(8).min(2048);
		tokens.reserve(estimated);

		loop {
			self.cursor.skip_whitespace_and_comments();

			if self.cursor.is_eof() {
				// Add EOF token
				let span = Span::new(
					self.cursor.position() as u32,
					self.cursor.position() as u32,
					self.cursor.line(),
					self.cursor.column(),
				);
				tokens.push(Token::new(TokenKind::Eof, span));
				break;
			}

			let token = self.next_token()?;
			tokens.push(token);
		}

		// Copy source into bump so it lives as long as result
		let source = self.bump.alloc_str(self.cursor.source());

		Ok(TokenizeResult {
			tokens,
			source,
		})
	}

	fn next_token(&mut self) -> Result<Token, LexError> {
		let start = self.cursor.position();
		let start_line = self.cursor.line();
		let start_column = self.cursor.column();

		let ch = self.cursor.peek().unwrap();

		match ch {
			// Variables: $name or $123
			'$' => self.scan_variable(start, start_line, start_column),

			// Quoted identifiers: `...`
			'`' => self.scan_quoted_identifier(start, start_line, start_column),

			// String literals
			'\'' | '"' => self.scan_string(ch, start, start_line, start_column),

			// Numbers
			'0'..='9' => self.scan_number(start, start_line, start_column),

			// Dot - could be operator or start of decimal
			'.' => {
				if self.cursor.peek_ahead(1).map_or(false, |c| c.is_ascii_digit()) {
					self.scan_number(start, start_line, start_column)
				} else {
					self.scan_operator(start, start_line, start_column)
				}
			}

			// Punctuation
			'(' | ')' | '[' | ']' | '{' | '}' | ',' | ';' => {
				self.scan_punctuation(start, start_line, start_column)
			}

			// Operators
			'+' | '*' | '/' | '^' | '%' | '?' | '<' | '>' | ':' | '&' | '|' | '=' | '!' | '-' => {
				self.scan_operator(start, start_line, start_column)
			}

			// Identifiers, keywords, word operators, or boolean literals
			'a'..='z' | 'A'..='Z' | '_' => self.scan_identifier_or_keyword(start, start_line, start_column),

			// Unexpected character
			_ => {
				let span = self.cursor.span_from(start, start_line, start_column);
				self.cursor.advance();
				Err(LexError::UnexpectedChar {
					ch,
					line: start_line,
					column: start_column,
					span,
				})
			}
		}
	}

	fn scan_variable(&mut self, start: usize, start_line: u32, start_column: u32) -> Result<Token, LexError> {
		self.cursor.advance(); // consume '$'

		// Variable can start with letter, underscore, or digit
		let first = self.cursor.peek();
		if !first.map_or(false, |c| c.is_ascii_alphanumeric() || c == '_') {
			let span = self.cursor.span_from(start, start_line, start_column);
			return Err(LexError::EmptyVariable {
				line: start_line,
				column: start_column,
				span,
			});
		}

		// Consume variable name
		self.cursor.advance_while(|c| c.is_ascii_alphanumeric() || c == '_');

		let span = self.cursor.span_from(start, start_line, start_column);
		Ok(Token::new(TokenKind::Variable, span))
	}

	fn scan_quoted_identifier(
		&mut self,
		start: usize,
		start_line: u32,
		start_column: u32,
	) -> Result<Token, LexError> {
		self.cursor.advance(); // consume opening '`'

		let content_start = self.cursor.position();

		// Find closing backtick
		while let Some(ch) = self.cursor.peek() {
			if ch == '`' {
				// Create span for content only (excluding backticks)
				let span = Span::new(
					content_start as u32,
					self.cursor.position() as u32,
					start_line,
					start_column + 1,
				);
				self.cursor.advance(); // consume closing '`'
				return Ok(Token::new(TokenKind::QuotedIdentifier, span));
			}
			self.cursor.advance();
		}

		let span = self.cursor.span_from(start, start_line, start_column);
		Err(LexError::UnterminatedQuotedIdentifier {
			line: start_line,
			column: start_column,
			span,
		})
	}

	fn scan_string(
		&mut self,
		quote: char,
		start: usize,
		start_line: u32,
		start_column: u32,
	) -> Result<Token, LexError> {
		self.cursor.advance(); // consume opening quote

		let content_start = self.cursor.position();

		loop {
			match self.cursor.peek() {
				None | Some('\n') => {
					let span = self.cursor.span_from(start, start_line, start_column);
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
						self.cursor.position() as u32,
						start_line,
						start_column + 1,
					);
					self.cursor.advance(); // consume closing quote
					return Ok(Token::new(TokenKind::Literal(LiteralKind::String), span));
				}
				Some('\\') => {
					// Skip escape sequence (backslash + next char) without processing
					self.cursor.advance();
					if self.cursor.peek().is_some() {
						self.cursor.advance();
					}
				}
				Some(_) => {
					self.cursor.advance();
				}
			}
		}
	}

	fn scan_number(&mut self, start: usize, start_line: u32, start_column: u32) -> Result<Token, LexError> {
		// Check for prefixed numbers: 0x, 0b, 0o
		let prefix = self.cursor.peek_str(2).to_ascii_lowercase();

		if prefix == "0x" {
			self.cursor.advance(); // '0'
			self.cursor.advance(); // 'x'
			self.cursor.advance_while(|c| c.is_ascii_hexdigit() || c == '_');
		} else if prefix == "0b" {
			self.cursor.advance(); // '0'
			self.cursor.advance(); // 'b'
			self.cursor.advance_while(|c| c == '0' || c == '1' || c == '_');
		} else if prefix == "0o" {
			self.cursor.advance(); // '0'
			self.cursor.advance(); // 'o'
			self.cursor.advance_while(|c| ('0'..='7').contains(&c) || c == '_');
		} else {
			// Decimal number
			// Handle leading dot for floats like .5
			let has_leading_dot = self.cursor.peek() == Some('.');
			if has_leading_dot {
				self.cursor.advance();
			}

			// Integer part
			self.cursor.advance_while(|c| c.is_ascii_digit() || c == '_');

			// Fractional part (if not leading dot)
			if !has_leading_dot && self.cursor.peek() == Some('.') {
				if self.cursor.peek_ahead(1).map_or(false, |c| c.is_ascii_digit()) {
					self.cursor.advance(); // '.'
					self.cursor.advance_while(|c| c.is_ascii_digit() || c == '_');
				}
			}

			// Scientific notation
			if let Some('e') | Some('E') = self.cursor.peek() {
				self.cursor.advance();
				if matches!(self.cursor.peek(), Some('+') | Some('-')) {
					self.cursor.advance();
				}
				self.cursor.advance_while(|c| c.is_ascii_digit() || c == '_');
			}
		}

		let span = self.cursor.span_from(start, start_line, start_column);
		let text = span.text(self.cursor.source());

		// Determine if integer or float
		let kind = if text.contains('.') || text.to_ascii_lowercase().contains('e') {
			LiteralKind::Float
		} else {
			LiteralKind::Integer
		};

		Ok(Token::new(TokenKind::Literal(kind), span))
	}

	fn scan_punctuation(&mut self, start: usize, start_line: u32, start_column: u32) -> Result<Token, LexError> {
		let ch = self.cursor.advance().unwrap();
		let span = self.cursor.span_from(start, start_line, start_column);

		let punct = match ch {
			'(' => Punctuation::OpenParen,
			')' => Punctuation::CloseParen,
			'[' => Punctuation::OpenBracket,
			']' => Punctuation::CloseBracket,
			'{' => Punctuation::OpenCurly,
			'}' => Punctuation::CloseCurly,
			',' => Punctuation::Comma,
			';' => Punctuation::Semicolon,
			_ => unreachable!(),
		};

		Ok(Token::new(TokenKind::Punctuation(punct), span))
	}

	fn scan_operator(&mut self, start: usize, start_line: u32, start_column: u32) -> Result<Token, LexError> {
		let ch = self.cursor.peek().unwrap();

		// Try multi-character operators first
		let op = match ch {
			'<' => {
				self.cursor.advance();
				if self.cursor.try_consume("<") {
					Operator::DoubleLeftAngle
				} else if self.cursor.try_consume("=") {
					Operator::LeftAngleEqual
				} else {
					Operator::LeftAngle
				}
			}
			'>' => {
				self.cursor.advance();
				if self.cursor.try_consume(">") {
					Operator::DoubleRightAngle
				} else if self.cursor.try_consume("=") {
					Operator::RightAngleEqual
				} else {
					Operator::RightAngle
				}
			}
			':' => {
				self.cursor.advance();
				if self.cursor.try_consume(":") {
					Operator::DoubleColon
				} else if self.cursor.try_consume("=") {
					Operator::ColonEqual
				} else {
					Operator::Colon
				}
			}
			'-' => {
				self.cursor.advance();
				if self.cursor.try_consume(">") {
					Operator::Arrow
				} else {
					Operator::Minus
				}
			}
			'.' => {
				self.cursor.advance();
				if self.cursor.try_consume(".") {
					Operator::DoubleDot
				} else {
					Operator::Dot
				}
			}
			'&' => {
				self.cursor.advance();
				if self.cursor.try_consume("&") {
					Operator::DoubleAmpersand
				} else {
					Operator::Ampersand
				}
			}
			'|' => {
				self.cursor.advance();
				if self.cursor.try_consume("|") {
					Operator::DoublePipe
				} else {
					Operator::Pipe
				}
			}
			'=' => {
				self.cursor.advance();
				if self.cursor.try_consume("=") {
					Operator::DoubleEqual
				} else {
					Operator::Equal
				}
			}
			'!' => {
				self.cursor.advance();
				if self.cursor.try_consume("=") {
					Operator::BangEqual
				} else {
					Operator::Bang
				}
			}
			'+' => {
				self.cursor.advance();
				Operator::Plus
			}
			'*' => {
				self.cursor.advance();
				Operator::Asterisk
			}
			'/' => {
				self.cursor.advance();
				Operator::Slash
			}
			'^' => {
				self.cursor.advance();
				Operator::Caret
			}
			'%' => {
				self.cursor.advance();
				Operator::Percent
			}
			'?' => {
				self.cursor.advance();
				Operator::QuestionMark
			}
			_ => unreachable!(),
		};

		let span = self.cursor.span_from(start, start_line, start_column);
		Ok(Token::new(TokenKind::Operator(op), span))
	}

	fn scan_identifier_or_keyword(
		&mut self,
		start: usize,
		start_line: u32,
		start_column: u32,
	) -> Result<Token, LexError> {
		// Consume identifier characters
		self.cursor.advance_while(|c| c.is_ascii_alphanumeric() || c == '_');

		let span = self.cursor.span_from(start, start_line, start_column);
		let text = span.text(self.cursor.source());

		// Check for word operators (AND, OR, NOT, XOR, AS)
		if let Some(op) = lookup_word_operator(text) {
			return Ok(Token::new(TokenKind::Operator(op), span));
		}

		// Check for boolean literals
		let lower = text.to_ascii_lowercase();
		if lower == "true" {
			return Ok(Token::new(TokenKind::Literal(LiteralKind::True), span));
		}
		if lower == "false" {
			return Ok(Token::new(TokenKind::Literal(LiteralKind::False), span));
		}
		if lower == "undefined" || lower == "null" {
			return Ok(Token::new(TokenKind::Literal(LiteralKind::Undefined), span));
		}

		// Check for keywords (case-insensitive)
		if let Some(kw) = lookup_keyword(text) {
			return Ok(Token::new(TokenKind::Keyword(kw), span));
		}

		// Plain identifier
		Ok(Token::new(TokenKind::Identifier, span))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::tokenize::keyword::Keyword;

	fn tokenize(source: &str) -> Result<Vec<Token>, LexError> {
		let bump = Bump::new();
		let result = Lexer::new(source, &bump).tokenize()?;
		Ok(result.tokens.into_iter().collect())
	}

	#[test]
	fn test_simple_query() {
		let tokens = tokenize("FROM users MAP { * }").unwrap();
		assert_eq!(tokens.len(), 7); // FROM, users, MAP, {, *, }, EOF
		assert!(matches!(tokens[0].kind, TokenKind::Keyword(Keyword::From)));
		assert!(matches!(tokens[1].kind, TokenKind::Identifier));
		assert!(matches!(tokens[2].kind, TokenKind::Keyword(Keyword::Map)));
		assert!(matches!(tokens[3].kind, TokenKind::Punctuation(Punctuation::OpenCurly)));
		assert!(matches!(tokens[4].kind, TokenKind::Operator(Operator::Asterisk)));
		assert!(matches!(tokens[5].kind, TokenKind::Punctuation(Punctuation::CloseCurly)));
		assert!(matches!(tokens[6].kind, TokenKind::Eof));
	}

	#[test]
	fn test_case_insensitive_keywords() {
		let tokens = tokenize("from FROM From").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Keyword(Keyword::From)));
		assert!(matches!(tokens[1].kind, TokenKind::Keyword(Keyword::From)));
		assert!(matches!(tokens[2].kind, TokenKind::Keyword(Keyword::From)));
	}

	#[test]
	fn test_string_literal() {
		let bump = Bump::new();
		let result = Lexer::new("'hello world'", &bump).tokenize().unwrap();
		assert!(matches!(result.tokens[0].kind, TokenKind::Literal(LiteralKind::String)));
		assert_eq!(result.text(&result.tokens[0]), "hello world");
	}

	#[test]
	fn test_variable() {
		let bump = Bump::new();
		let result = Lexer::new("$user_id $123", &bump).tokenize().unwrap();
		assert!(matches!(result.tokens[0].kind, TokenKind::Variable));
		assert_eq!(result.text(&result.tokens[0]), "$user_id");
		assert!(matches!(result.tokens[1].kind, TokenKind::Variable));
		assert_eq!(result.text(&result.tokens[1]), "$123");
	}

	#[test]
	fn test_quoted_identifier() {
		let bump = Bump::new();
		let result = Lexer::new("`my table`", &bump).tokenize().unwrap();
		assert!(matches!(result.tokens[0].kind, TokenKind::QuotedIdentifier));
		assert_eq!(result.text(&result.tokens[0]), "my table");
	}

	#[test]
	fn test_numbers() {
		let tokens = tokenize("42 3.14 0xFF 0b1010").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert!(matches!(tokens[1].kind, TokenKind::Literal(LiteralKind::Float)));
		assert!(matches!(tokens[2].kind, TokenKind::Literal(LiteralKind::Integer)));
		assert!(matches!(tokens[3].kind, TokenKind::Literal(LiteralKind::Integer)));
	}

	#[test]
	fn test_operators() {
		let tokens = tokenize("+ - * / == != <= >= && ||").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Operator(Operator::Plus)));
		assert!(matches!(tokens[1].kind, TokenKind::Operator(Operator::Minus)));
		assert!(matches!(tokens[2].kind, TokenKind::Operator(Operator::Asterisk)));
		assert!(matches!(tokens[3].kind, TokenKind::Operator(Operator::Slash)));
		assert!(matches!(tokens[4].kind, TokenKind::Operator(Operator::DoubleEqual)));
		assert!(matches!(tokens[5].kind, TokenKind::Operator(Operator::BangEqual)));
		assert!(matches!(tokens[6].kind, TokenKind::Operator(Operator::LeftAngleEqual)));
		assert!(matches!(tokens[7].kind, TokenKind::Operator(Operator::RightAngleEqual)));
		assert!(matches!(tokens[8].kind, TokenKind::Operator(Operator::DoubleAmpersand)));
		assert!(matches!(tokens[9].kind, TokenKind::Operator(Operator::DoublePipe)));
	}

	#[test]
	fn test_word_operators() {
		let tokens = tokenize("a AND b OR c NOT d").unwrap();
		assert!(matches!(tokens[1].kind, TokenKind::Operator(Operator::And)));
		assert!(matches!(tokens[3].kind, TokenKind::Operator(Operator::Or)));
		assert!(matches!(tokens[5].kind, TokenKind::Operator(Operator::Not)));
	}

	#[test]
	fn test_boolean_literals() {
		let tokens = tokenize("true false TRUE FALSE").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Literal(LiteralKind::True)));
		assert!(matches!(tokens[1].kind, TokenKind::Literal(LiteralKind::False)));
		assert!(matches!(tokens[2].kind, TokenKind::Literal(LiteralKind::True)));
		assert!(matches!(tokens[3].kind, TokenKind::Literal(LiteralKind::False)));
	}

	#[test]
	fn test_line_comment() {
		let tokens = tokenize("SELECT # this is a comment\n*").unwrap();
		assert_eq!(tokens.len(), 3); // SELECT, *, EOF
		assert!(matches!(tokens[0].kind, TokenKind::Keyword(Keyword::Select)));
		assert!(matches!(tokens[1].kind, TokenKind::Operator(Operator::Asterisk)));
	}

	#[test]
	fn test_scan_keyword() {
		let tokens = tokenize("SCAN users").unwrap();
		assert!(matches!(tokens[0].kind, TokenKind::Keyword(Keyword::Scan)));
	}

	#[test]
	fn test_error_unterminated_string() {
		let result = tokenize("'unterminated");
		assert!(matches!(result, Err(LexError::UnterminatedString { .. })));
	}

	#[test]
	fn test_error_unexpected_char() {
		let result = tokenize("@invalid");
		assert!(matches!(
			result,
			Err(LexError::UnexpectedChar {
				ch: '@',
				..
			})
		));
	}
}
