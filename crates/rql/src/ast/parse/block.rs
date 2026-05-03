// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::{AstErrorKind, Error, TypeError};

use crate::{
	Result,
	ast::{
		ast::{AstBlock, AstStatement},
		parse::{Parser, Precedence},
	},
	token::{operator::Operator, separator::Separator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_block(&mut self) -> Result<AstBlock<'bump>> {
		let token = self.consume_operator(Operator::OpenCurly)?;

		let mut statements = Vec::new();
		loop {
			self.skip_new_line()?;

			if self.is_eof() {
				let fragment = token.fragment.to_owned();
				return Err(Error::from(TypeError::Ast {
					kind: AstErrorKind::UnexpectedToken {
						expected: "expected '}' to close block".to_string(),
					},
					message: format!(
						"Unexpected token: expected {}, got {}",
						"expected '}' to close block",
						fragment.text()
					),
					fragment,
				}));
			}

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let stmt = self.parse_block_statement()?;
			if !stmt.is_empty() {
				statements.push(stmt);
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstBlock {
			token,
			statements,
		})
	}

	fn parse_block_statement(&mut self) -> Result<AstStatement<'bump>> {
		let mut nodes = Vec::with_capacity(4);
		let mut has_pipes = false;

		loop {
			if self.is_eof() {
				break;
			}

			if let Ok(current) = self.current() {
				if current.is_operator(Operator::CloseCurly) {
					break;
				}
				if current.is_separator(Separator::Semicolon) {
					self.advance()?;
					break;
				}
			}

			let node = self.parse_node(Precedence::None)?;
			nodes.push(node);

			if !self.is_eof()
				&& let Ok(current) = self.current()
			{
				if current.is_operator(Operator::CloseCurly) {
					break;
				}
				if current.is_separator(Separator::Semicolon) {
					self.advance()?;
					break;
				}
				if current.is_operator(Operator::Pipe) {
					self.advance()?;
					has_pipes = true;
				} else {
					self.consume_if(TokenKind::Separator(Separator::NewLine))?;
				}
			}
		}

		Ok(AstStatement {
			nodes,
			has_pipes,
			is_output: false,
			rql: "",
		})
	}
}
