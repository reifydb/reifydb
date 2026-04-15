// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use thiserror::Error;

use crate::{
	ast::ast::*,
	bump::{Bump, BumpBox, BumpVec},
	token::token::{Token, TokenKind},
};

#[derive(Error, Debug)]
pub enum ParserError {
	#[error("Unexpected token {0:?}, expected {1:?} at line {2}, column {3}")]
	UnexpectedToken(TokenKind, TokenKind, u32, u32),
	#[error("Unexpected EOF, expected {0:?} at line {1}, column {2}")]
	UnexpectedEOF(TokenKind, u32, u32),
	#[error("Expected Name at line {0}, column {1}")]
	ExpectedName(u32, u32),
}

pub struct Parser<'bump> {
	tokens: BumpVec<'bump, Token<'bump>>,
	pos: usize,
	bump: &'bump Bump,
}

impl<'bump> Parser<'bump> {
	pub fn new(bump: &'bump Bump, tokens: BumpVec<'bump, Token<'bump>>) -> Self {
		Self {
			tokens,
			pos: 0,
			bump,
		}
	}

	pub fn parse_operation(&mut self) -> Result<AstOperation<'bump>, ParserError> {
		let token = self.peek();
		let mut op_token = token;
		let mut name = None;

		if token.kind == TokenKind::Name && token.value() == "query" {
			op_token = self.consume();
			if self.peek().kind == TokenKind::Name {
				name = Some(self.consume().fragment);
			}
		}

		let selections = self.parse_selection_set()?;

		Ok(AstOperation {
			token: op_token,
			name,
			selections,
		})
	}

	fn parse_selection_set(&mut self) -> Result<BumpVec<'bump, AstSelection<'bump>>, ParserError> {
		self.expect(TokenKind::BraceOpen)?;
		let mut selections = BumpVec::new_in(self.bump);

		while self.peek().kind != TokenKind::BraceClose && !self.is_eof() {
			selections.push(self.parse_selection()?);
		}

		self.expect(TokenKind::BraceClose)?;
		Ok(selections)
	}

	fn parse_selection(&mut self) -> Result<AstSelection<'bump>, ParserError> {
		// Only supporting Fields for now
		let field = self.parse_field()?;
		Ok(AstSelection::Field(BumpBox::new_in(field, self.bump)))
	}

	fn parse_field(&mut self) -> Result<AstField<'bump>, ParserError> {
		let token = self.expect(TokenKind::Name)?;
		let mut name = token.fragment;
		let mut alias = None;

		if self.peek().kind == TokenKind::Colon {
			self.consume(); // :
			alias = Some(name);
			name = self.expect(TokenKind::Name)?.fragment;
		}

		let arguments = if self.peek().kind == TokenKind::ParenOpen {
			Some(self.parse_arguments()?)
		} else {
			None
		};

		let selections = if self.peek().kind == TokenKind::BraceOpen {
			Some(self.parse_selection_set()?)
		} else {
			None
		};

		Ok(AstField {
			token,
			alias,
			name,
			arguments,
			selections,
		})
	}

	fn parse_arguments(&mut self) -> Result<BumpVec<'bump, AstArgument<'bump>>, ParserError> {
		self.expect(TokenKind::ParenOpen)?;
		let mut arguments = BumpVec::new_in(self.bump);

		while self.peek().kind != TokenKind::ParenClose && !self.is_eof() {
			let token = self.expect(TokenKind::Name)?;
			let name = token.fragment;
			self.expect(TokenKind::Colon)?;
			let value = self.parse_value()?;
			arguments.push(AstArgument {
				token,
				name,
				value,
			});
		}

		self.expect(TokenKind::ParenClose)?;
		Ok(arguments)
	}

	fn parse_value(&mut self) -> Result<AstValue<'bump>, ParserError> {
		let token = self.peek();
		match token.kind {
			TokenKind::Dollar => {
				self.consume(); // $
				let var = self.expect(TokenKind::Name)?;
				Ok(AstValue::Variable(var))
			}
			TokenKind::IntLiteral => Ok(AstValue::Int(self.consume())),
			TokenKind::FloatLiteral => Ok(AstValue::Float(self.consume())),
			TokenKind::StringLiteral => Ok(AstValue::String(self.consume())),
			TokenKind::BooleanLiteral => Ok(AstValue::Boolean(self.consume())),
			TokenKind::Name => {
				let t = self.consume();
				if t.value() == "null" {
					Ok(AstValue::Null(t))
				} else if t.value() == "true" || t.value() == "false" {
					Ok(AstValue::Boolean(t))
				} else {
					Ok(AstValue::Enum(t))
				}
			}
			TokenKind::BracketOpen => {
				self.consume();
				let mut values = BumpVec::new_in(self.bump);
				while self.peek().kind != TokenKind::BracketClose && !self.is_eof() {
					values.push(self.parse_value()?);
				}
				self.expect(TokenKind::BracketClose)?;
				Ok(AstValue::List(values))
			}
			TokenKind::BraceOpen => {
				self.consume();
				let mut fields = BumpVec::new_in(self.bump);
				while self.peek().kind != TokenKind::BraceClose && !self.is_eof() {
					let t = self.expect(TokenKind::Name)?;
					let name = t.fragment;
					self.expect(TokenKind::Colon)?;
					let value = self.parse_value()?;
					fields.push(AstObjectField {
						token: t,
						name,
						value,
					});
				}
				self.expect(TokenKind::BraceClose)?;
				Ok(AstValue::Object(fields))
			}
			_ => Err(ParserError::UnexpectedToken(
				token.kind,
				TokenKind::Name,
				token.fragment.line().0,
				token.fragment.column().0,
			)),
		}
	}

	fn expect(&mut self, kind: TokenKind) -> Result<Token<'bump>, ParserError> {
		let token = self.peek();
		if token.kind == kind {
			Ok(self.consume())
		} else {
			if self.is_eof() {
				Err(ParserError::UnexpectedEOF(
					kind,
					token.fragment.line().0,
					token.fragment.column().0,
				))
			} else {
				Err(ParserError::UnexpectedToken(
					token.kind,
					kind,
					token.fragment.line().0,
					token.fragment.column().0,
				))
			}
		}
	}

	fn consume(&mut self) -> Token<'bump> {
		let token = self.tokens[self.pos];
		self.pos += 1;
		token
	}

	fn peek(&self) -> Token<'bump> {
		if self.pos < self.tokens.len() {
			self.tokens[self.pos]
		} else {
			// Return EOF token with last known position
			let last_fragment = self.tokens.last().map(|t| t.fragment).unwrap_or_default();
			Token {
				kind: TokenKind::EOF,
				fragment: last_fragment,
			}
		}
	}

	fn is_eof(&self) -> bool {
		self.pos >= self.tokens.len()
	}
}
