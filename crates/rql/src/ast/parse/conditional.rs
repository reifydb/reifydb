// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{error::diagnostic::ast::unexpected_token_error, fragment::Fragment};

use crate::ast::{
	ast::{Ast, AstElseIf, AstIf, AstLiteral, AstLiteralUndefined},
	parse::{Parser, Precedence},
	tokenize::{
		keyword::Keyword,
		operator::Operator,
		token::{Literal, Token, TokenKind},
	},
};

impl Parser {
	pub(crate) fn parse_if(&mut self) -> crate::Result<AstIf> {
		let token = self.current()?.clone();

		// Consume 'if' keyword
		if !self.current()?.is_keyword(Keyword::If) {
			return Err(reifydb_type::error::Error(unexpected_token_error(
				"expected 'if'",
				self.current()?.fragment.clone(),
			)));
		}
		self.advance()?; // consume 'if'

		// Parse condition expression
		let condition = Box::new(self.parse_node(Precedence::None)?);

		// Expect opening brace '{'
		if !self.current()?.is_operator(Operator::OpenCurly) {
			return Err(reifydb_type::error::Error(unexpected_token_error(
				"expected '{' after if condition",
				self.current()?.fragment.clone(),
			)));
		}

		// Parse the then block - should be a single expression inside {}
		self.advance()?; // consume '{'

		// Check if the block is empty (next token is '}')
		let then_expr = if self.current()?.is_operator(Operator::CloseCurly) {
			// Empty block - return undefined literal
			Ast::Literal(AstLiteral::Undefined(AstLiteralUndefined(Token {
				kind: TokenKind::Literal(Literal::Undefined),
				fragment: Fragment::internal("undefined"),
			})))
		} else {
			self.parse_node(Precedence::None)?
		};

		// Expect closing brace '}'
		if !self.current()?.is_operator(Operator::CloseCurly) {
			return Err(reifydb_type::error::Error(unexpected_token_error(
				"expected '}' after then block",
				self.current()?.fragment.clone(),
			)));
		}
		self.advance()?; // consume '}'

		let then_block = Box::new(then_expr);

		// Parse any else if chains
		let else_ifs = self.parse_else_if_chain()?;

		// Parse optional else block
		let else_block = self.parse_else_block()?;

		Ok(AstIf {
			token,
			condition,
			then_block,
			else_ifs,
			else_block,
		})
	}

	fn parse_else_if_chain(&mut self) -> crate::Result<Vec<AstElseIf>> {
		let mut else_ifs = Vec::new();

		while !self.is_eof() {
			// Check for 'else' keyword
			if !self.current()?.is_keyword(Keyword::Else) {
				break;
			}

			// Peek ahead to see if this is 'else if' or just 'else'
			let next_pos = self.position + 1;
			if next_pos >= self.tokens.len() {
				// Just 'else' at end of tokens
				break;
			}

			let next_token = &self.tokens[next_pos];
			if !next_token.is_keyword(Keyword::If) {
				// Just 'else', not 'else if'
				break;
			}

			// Parse 'else if'
			let else_token = self.advance()?; // consume 'else'
			self.advance()?; // consume 'if'

			// Parse condition
			let condition = Box::new(self.parse_node(Precedence::None)?);

			// Expect opening brace '{'
			if !self.current()?.is_operator(Operator::OpenCurly) {
				return Err(reifydb_type::error::Error(unexpected_token_error(
					"expected '{' after else if condition",
					self.current()?.fragment.clone(),
				)));
			}

			// Parse the then block - should be a single expression inside {}
			self.advance()?; // consume '{'

			let then_expr = if self.current()?.is_operator(Operator::CloseCurly) {
				Ast::Literal(AstLiteral::Undefined(AstLiteralUndefined(Token {
					kind: TokenKind::Literal(Literal::Undefined),
					fragment: Fragment::internal("undefined"),
				})))
			} else {
				self.parse_node(Precedence::None)?
			};

			// Expect closing brace '}'
			if !self.current()?.is_operator(Operator::CloseCurly) {
				return Err(reifydb_type::error::Error(unexpected_token_error(
					"expected '}' after else if then block",
					self.current()?.fragment.clone(),
				)));
			}
			self.advance()?; // consume '}'

			let then_block = Box::new(then_expr);

			else_ifs.push(AstElseIf {
				token: else_token,
				condition,
				then_block,
			});
		}

		Ok(else_ifs)
	}

	fn parse_else_block(&mut self) -> crate::Result<Option<Box<Ast>>> {
		// Check if we have a final 'else' block
		if self.is_eof() || !self.current()?.is_keyword(Keyword::Else) {
			return Ok(None);
		}

		// Consume 'else'
		self.advance()?;

		// Expect opening brace '{'
		if !self.current()?.is_operator(Operator::OpenCurly) {
			return Err(reifydb_type::error::Error(unexpected_token_error(
				"expected '{' after else",
				self.current()?.fragment.clone(),
			)));
		}

		// Parse the else block - should be a single expression inside {}
		self.advance()?; // consume '{'

		let else_expr = if self.current()?.is_operator(Operator::CloseCurly) {
			Ast::Literal(AstLiteral::Undefined(AstLiteralUndefined(Token {
				kind: TokenKind::Literal(Literal::Undefined),
				fragment: Fragment::internal("undefined"),
			})))
		} else {
			self.parse_node(Precedence::None)?
		};

		// Expect closing brace '}'
		if !self.current()?.is_operator(Operator::CloseCurly) {
			return Err(reifydb_type::error::Error(unexpected_token_error(
				"expected '}' after else block",
				self.current()?.fragment.clone(),
			)));
		}
		self.advance()?; // consume '}'

		Ok(Some(Box::new(else_expr)))
	}
}
