// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use thiserror::Error;

use crate::{
	bump::{Bump, BumpVec},
	token::{
		cursor::Cursor,
		token::{Token, TokenKind},
	},
};

#[derive(Error, Debug)]
pub enum LexerError {
	#[error("Unexpected character '{0}' at line {1}, column {2}")]
	UnexpectedCharacter(char, u32, u32),
	#[error("Unterminated string at line {0}, column {1}")]
	UnterminatedString(u32, u32),
	#[error("Invalid number at line {0}, column {1}")]
	InvalidNumber(u32, u32),
}

pub struct Lexer<'bump> {
	cursor: Cursor<'bump>,
	bump: &'bump Bump,
}

impl<'bump> Lexer<'bump> {
	pub fn new(bump: &'bump Bump, input: &'bump str) -> Self {
		Self {
			cursor: Cursor::new(input),
			bump,
		}
	}

	pub fn tokenize(mut self) -> Result<BumpVec<'bump, Token<'bump>>, LexerError> {
		let mut tokens = BumpVec::new_in(self.bump);

		while !self.cursor.is_eof() {
			self.cursor.skip_whitespace();
			if self.cursor.is_eof() {
				break;
			}

			let start_pos = self.cursor.pos();
			let start_line = self.cursor.line();
			let start_column = self.cursor.column();

			let ch = self.cursor.peek().unwrap();
			let kind = match ch {
				'{' => {
					self.cursor.consume();
					TokenKind::BraceOpen
				}
				'}' => {
					self.cursor.consume();
					TokenKind::BraceClose
				}
				'(' => {
					self.cursor.consume();
					TokenKind::ParenOpen
				}
				')' => {
					self.cursor.consume();
					TokenKind::ParenClose
				}
				'[' => {
					self.cursor.consume();
					TokenKind::BracketOpen
				}
				']' => {
					self.cursor.consume();
					TokenKind::BracketClose
				}
				'!' => {
					self.cursor.consume();
					TokenKind::Exclamation
				}
				'$' => {
					self.cursor.consume();
					TokenKind::Dollar
				}
				':' => {
					self.cursor.consume();
					TokenKind::Colon
				}
				'=' => {
					self.cursor.consume();
					TokenKind::Equals
				}
				'@' => {
					self.cursor.consume();
					TokenKind::At
				}
				'|' => {
					self.cursor.consume();
					TokenKind::Pipe
				}
				'.' => {
					if self.cursor.peek_str(3) == "..." {
						self.cursor.consume();
						self.cursor.consume();
						self.cursor.consume();
						TokenKind::Spread
					} else {
						return Err(LexerError::UnexpectedCharacter(
							ch,
							start_line,
							start_column,
						));
					}
				}
				'"' => self.scan_string()?,
				'0'..='9' | '-' => self.scan_number()?,
				'a'..='z' | 'A'..='Z' | '_' => self.scan_name()?,
				_ => return Err(LexerError::UnexpectedCharacter(ch, start_line, start_column)),
			};

			tokens.push(Token {
				kind,
				fragment: self.cursor.make_fragment(start_pos, start_line, start_column),
			});
		}

		Ok(tokens)
	}

	fn scan_name(&mut self) -> Result<TokenKind, LexerError> {
		self.cursor.consume_while(|ch| ch.is_alphanumeric() || ch == '_');
		Ok(TokenKind::Name)
	}

	fn scan_string(&mut self) -> Result<TokenKind, LexerError> {
		let start_line = self.cursor.line();
		let start_column = self.cursor.column();
		self.cursor.consume();

		let mut escaped = false;
		while let Some(ch) = self.cursor.consume() {
			if escaped {
				escaped = false;
				continue;
			}
			if ch == '\\' {
				escaped = true;
				continue;
			}
			if ch == '"' {
				return Ok(TokenKind::StringLiteral);
			}
		}

		Err(LexerError::UnterminatedString(start_line, start_column))
	}

	fn scan_number(&mut self) -> Result<TokenKind, LexerError> {
		let start_line = self.cursor.line();
		let start_column = self.cursor.column();

		if self.cursor.peek() == Some('-') {
			self.cursor.consume();
		}

		let mut is_float = false;
		self.cursor.consume_while(|ch| ch.is_ascii_digit());

		if self.cursor.peek() == Some('.') {
			is_float = true;
			self.cursor.consume();
			if !self.cursor.peek().map_or(false, |ch| ch.is_ascii_digit()) {
				return Err(LexerError::InvalidNumber(start_line, start_column));
			}
			self.cursor.consume_while(|ch| ch.is_ascii_digit());
		}

		if let Some('e') | Some('E') = self.cursor.peek() {
			is_float = true;
			self.cursor.consume();
			if let Some('+') | Some('-') = self.cursor.peek() {
				self.cursor.consume();
			}
			if !self.cursor.peek().map_or(false, |ch| ch.is_ascii_digit()) {
				return Err(LexerError::InvalidNumber(start_line, start_column));
			}
			self.cursor.consume_while(|ch| ch.is_ascii_digit());
		}

		if is_float {
			Ok(TokenKind::FloatLiteral)
		} else {
			Ok(TokenKind::IntLiteral)
		}
	}
}
