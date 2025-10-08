// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::diagnostic::ast::unexpected_token_error;

use crate::ast::{
	Ast, AstElseIf, AstIf,
	parse::{Parser, Precedence},
	tokenize::{Keyword, Operator},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_if(&mut self) -> crate::Result<AstIf<'a>> {
		let token = self.current()?.clone();

		// Consume 'if' keyword
		if !self.current()?.is_keyword(Keyword::If) {
			return Err(reifydb_type::Error(unexpected_token_error(
				"expected 'if'",
				self.current()?.fragment.clone(),
			)));
		}
		self.advance()?; // consume 'if'

		// Parse condition expression
		let condition = Box::new(self.parse_node(Precedence::None)?);

		// Expect opening brace '{'
		if !self.current()?.is_operator(Operator::OpenCurly) {
			return Err(reifydb_type::Error(unexpected_token_error(
				"expected '{' after if condition",
				self.current()?.fragment.clone(),
			)));
		}

		// Parse the then block - should be a single expression inside {}
		self.advance()?; // consume '{'
		let then_expr = self.parse_node(Precedence::None)?;

		// Expect closing brace '}'
		if !self.current()?.is_operator(Operator::CloseCurly) {
			return Err(reifydb_type::Error(unexpected_token_error(
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

	fn parse_else_if_chain(&mut self) -> crate::Result<Vec<AstElseIf<'a>>> {
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
				return Err(reifydb_type::Error(unexpected_token_error(
					"expected '{' after else if condition",
					self.current()?.fragment.clone(),
				)));
			}

			// Parse the then block - should be a single expression inside {}
			self.advance()?; // consume '{'
			let then_expr = self.parse_node(Precedence::None)?;

			// Expect closing brace '}'
			if !self.current()?.is_operator(Operator::CloseCurly) {
				return Err(reifydb_type::Error(unexpected_token_error(
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

	fn parse_else_block(&mut self) -> crate::Result<Option<Box<Ast<'a>>>> {
		// Check if we have a final 'else' block
		if self.is_eof() || !self.current()?.is_keyword(Keyword::Else) {
			return Ok(None);
		}

		// Consume 'else'
		self.advance()?;

		// Expect opening brace '{'
		if !self.current()?.is_operator(Operator::OpenCurly) {
			return Err(reifydb_type::Error(unexpected_token_error(
				"expected '{' after else",
				self.current()?.fragment.clone(),
			)));
		}

		// Parse the else block - should be a single expression inside {}
		self.advance()?; // consume '{'
		let else_expr = self.parse_node(Precedence::None)?;

		// Expect closing brace '}'
		if !self.current()?.is_operator(Operator::CloseCurly) {
			return Err(reifydb_type::Error(unexpected_token_error(
				"expected '}' after else block",
				self.current()?.fragment.clone(),
			)));
		}
		self.advance()?; // consume '}'

		Ok(Some(Box::new(else_expr)))
	}
}
