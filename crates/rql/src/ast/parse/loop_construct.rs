// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{AstBreak, AstContinue, AstFor, AstLoop, AstVariable, AstWhile},
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{keyword::Keyword, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	/// Parse `LOOP { ... }`
	pub(crate) fn parse_loop(&mut self) -> crate::Result<AstLoop<'bump>> {
		let token = self.consume_keyword(Keyword::Loop)?;
		let body = self.parse_block()?;
		Ok(AstLoop {
			token,
			body,
		})
	}

	/// Parse `WHILE condition { ... }`
	pub(crate) fn parse_while(&mut self) -> crate::Result<AstWhile<'bump>> {
		let token = self.consume_keyword(Keyword::While)?;
		let condition = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());
		let body = self.parse_block()?;
		Ok(AstWhile {
			token,
			condition,
			body,
		})
	}

	/// Parse `FOR $var IN expr { ... }`
	pub(crate) fn parse_for(&mut self) -> crate::Result<AstFor<'bump>> {
		let token = self.consume_keyword(Keyword::For)?;

		// Parse variable
		let var_token = self.consume(TokenKind::Variable)?;
		let variable = AstVariable {
			token: var_token,
		};

		// Consume IN keyword
		self.consume_keyword(Keyword::In)?;

		// Parse iterable expression
		let iterable = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());

		// Parse body block
		let body = self.parse_block()?;

		Ok(AstFor {
			token,
			variable,
			iterable,
			body,
		})
	}

	/// Parse `BREAK`
	pub(crate) fn parse_break(&mut self) -> crate::Result<AstBreak<'bump>> {
		let token = self.consume_keyword(Keyword::Break)?;
		Ok(AstBreak {
			token,
		})
	}

	/// Parse `CONTINUE`
	pub(crate) fn parse_continue(&mut self) -> crate::Result<AstContinue<'bump>> {
		let token = self.consume_keyword(Keyword::Continue)?;
		Ok(AstContinue {
			token,
		})
	}
}
