// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::diagnostic::ast::unexpected_token_error;

use crate::{
	ast::{
		ast::{AstElseIf, AstIf},
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::keyword::Keyword,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_if(&mut self) -> crate::Result<AstIf<'bump>> {
		let token = self.current()?;

		// Consume 'if' keyword
		if !self.current()?.is_keyword(Keyword::If) {
			return Err(reifydb_type::error::Error(unexpected_token_error(
				"expected 'if'",
				self.current()?.fragment.to_owned(),
			)));
		}
		self.advance()?; // consume 'if'

		// Parse condition expression
		let condition = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());

		// Parse the then block
		let then_block = self.parse_block()?;

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

	fn parse_else_if_chain(&mut self) -> crate::Result<Vec<AstElseIf<'bump>>> {
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
			let condition = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());

			// Parse the then block
			let then_block = self.parse_block()?;

			else_ifs.push(AstElseIf {
				token: else_token,
				condition,
				then_block,
			});
		}

		Ok(else_ifs)
	}

	fn parse_else_block(&mut self) -> crate::Result<Option<crate::ast::ast::AstBlock<'bump>>> {
		// Check if we have a final 'else' block
		if self.is_eof() || !self.current()?.is_keyword(Keyword::Else) {
			return Ok(None);
		}

		// Consume 'else'
		self.advance()?;

		// Parse the else block
		let block = self.parse_block()?;

		Ok(Some(block))
	}
}
