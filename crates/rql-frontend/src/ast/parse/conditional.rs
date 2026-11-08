// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::error::{AstErrorKind, Error, TypeError};

use crate::{
	Result,
	ast::{
		ast::{AstBlock, AstElseIf, AstIf},
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{keyword::Keyword, separator::Separator},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_if(&mut self) -> Result<AstIf<'bump>> {
		let token = self.current()?;

		if !self.current()?.is_keyword(Keyword::If) {
			let fragment = self.current()?.fragment.to_owned();
			return Err(Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "expected 'if'".to_string(),
				},
				message: format!(
					"Unexpected token: expected {}, got {}",
					"expected 'if'",
					fragment.text()
				),
				fragment,
			}));
		}
		self.advance()?;

		let condition = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());

		let then_block = self.parse_block()?;

		let else_ifs = self.parse_else_if_chain()?;

		let else_block = self.parse_else_block()?;

		Ok(AstIf {
			token,
			condition,
			then_block,
			else_ifs,
			else_block,
		})
	}

	fn skip_new_line_before_else(&mut self) {
		let mut position = self.position;
		while position < self.tokens.len() && self.tokens[position].is_separator(Separator::NewLine) {
			position += 1;
		}

		if position < self.tokens.len() && self.tokens[position].is_keyword(Keyword::Else) {
			self.position = position;
		}
	}

	fn parse_else_if_chain(&mut self) -> Result<Vec<AstElseIf<'bump>>> {
		let mut else_ifs = Vec::new();

		while !self.is_eof() {
			self.skip_new_line_before_else();

			if self.is_eof() || !self.current()?.is_keyword(Keyword::Else) {
				break;
			}

			let next_pos = self.position + 1;
			if next_pos >= self.tokens.len() {
				break;
			}

			let next_token = &self.tokens[next_pos];
			if !next_token.is_keyword(Keyword::If) {
				break;
			}

			let else_token = self.advance()?;
			self.advance()?;

			let condition = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());

			let then_block = self.parse_block()?;

			else_ifs.push(AstElseIf {
				token: else_token,
				condition,
				then_block,
			});
		}

		Ok(else_ifs)
	}

	fn parse_else_block(&mut self) -> Result<Option<AstBlock<'bump>>> {
		self.skip_new_line_before_else();

		if self.is_eof() || !self.current()?.is_keyword(Keyword::Else) {
			return Ok(None);
		}

		self.advance()?;

		let block = self.parse_block()?;

		Ok(Some(block))
	}
}
