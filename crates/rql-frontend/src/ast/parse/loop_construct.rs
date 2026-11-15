// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::error::ValueError;

use crate::{
	ast::{
		ast::{AstBreak, AstContinue, AstFor, AstLoop, AstVariable, AstWhile},
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{keyword::Keyword, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_loop(&mut self) -> Result<AstLoop<'bump>, ValueError> {
		let token = self.consume_keyword(Keyword::Loop)?;
		let body = self.parse_block()?;
		Ok(AstLoop {
			token,
			body,
		})
	}

	pub(crate) fn parse_while(&mut self) -> Result<AstWhile<'bump>, ValueError> {
		let token = self.consume_keyword(Keyword::While)?;
		let condition = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());
		let body = self.parse_block()?;
		Ok(AstWhile {
			token,
			condition,
			body,
		})
	}

	pub(crate) fn parse_for(&mut self) -> Result<AstFor<'bump>, ValueError> {
		let token = self.consume_keyword(Keyword::For)?;

		let var_token = self.consume(TokenKind::Variable)?;
		let variable = AstVariable {
			token: var_token,
		};

		self.consume_keyword(Keyword::In)?;

		let iterable = self.parse_let_value()?;

		let body = self.parse_block()?;

		Ok(AstFor {
			token,
			variable,
			iterable,
			body,
		})
	}

	pub(crate) fn parse_break(&mut self) -> Result<AstBreak<'bump>, ValueError> {
		let token = self.consume_keyword(Keyword::Break)?;
		Ok(AstBreak {
			token,
		})
	}

	pub(crate) fn parse_continue(&mut self) -> Result<AstContinue<'bump>, ValueError> {
		let token = self.consume_keyword(Keyword::Continue)?;
		Ok(AstContinue {
			token,
		})
	}
}
