// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{AstBreak, AstContinue, AstFor, AstLoop, AstVariable, AstWhile},
		parse::{Parser, Precedence},
	},
	token::{keyword::Keyword, token::TokenKind},
};

impl Parser {
	/// Parse `LOOP { ... }`
	pub(crate) fn parse_loop(&mut self) -> crate::Result<AstLoop> {
		let token = self.consume_keyword(Keyword::Loop)?;
		let body = self.parse_block()?;
		Ok(AstLoop {
			token,
			body,
		})
	}

	/// Parse `WHILE condition { ... }`
	pub(crate) fn parse_while(&mut self) -> crate::Result<AstWhile> {
		let token = self.consume_keyword(Keyword::While)?;
		let condition = Box::new(self.parse_node(Precedence::None)?);
		let body = self.parse_block()?;
		Ok(AstWhile {
			token,
			condition,
			body,
		})
	}

	/// Parse `FOR $var IN expr { ... }`
	pub(crate) fn parse_for(&mut self) -> crate::Result<AstFor> {
		let token = self.consume_keyword(Keyword::For)?;

		// Parse variable
		let var_token = self.consume(TokenKind::Variable)?;
		let variable = AstVariable {
			token: var_token,
		};

		// Consume IN keyword
		self.consume_keyword(Keyword::In)?;

		// Parse iterable expression
		let iterable = Box::new(self.parse_node(Precedence::None)?);

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
	pub(crate) fn parse_break(&mut self) -> crate::Result<AstBreak> {
		let token = self.consume_keyword(Keyword::Break)?;
		Ok(AstBreak {
			token,
		})
	}

	/// Parse `CONTINUE`
	pub(crate) fn parse_continue(&mut self) -> crate::Result<AstContinue> {
		let token = self.consume_keyword(Keyword::Continue)?;
		Ok(AstContinue {
			token,
		})
	}
}
