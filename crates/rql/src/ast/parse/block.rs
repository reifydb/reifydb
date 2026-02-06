// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::diagnostic::ast::unexpected_token_error;

use crate::{
	ast::{ast::AstBlock, parse::Parser},
	token::{operator::Operator, separator::Separator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	/// Parse a block: `{ stmt; stmt; ... }`
	/// Consumes the opening `{`, parses statements separated by `;` or newlines,
	/// and consumes the closing `}`.
	pub(crate) fn parse_block(&mut self) -> crate::Result<AstBlock<'bump>> {
		let token = self.consume_operator(Operator::OpenCurly)?;

		let mut statements = Vec::new();
		loop {
			// Skip newlines
			self.skip_new_line()?;

			if self.is_eof() {
				return Err(reifydb_type::error::Error(unexpected_token_error(
					"expected '}' to close block",
					token.fragment.to_owned(),
				)));
			}

			// Check for closing brace
			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			// Parse a statement
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

	/// Parse a single statement inside a block.
	/// Stops at `;`, `}`, or EOF without consuming `}`.
	fn parse_block_statement(&mut self) -> crate::Result<crate::ast::ast::AstStatement<'bump>> {
		let mut nodes = Vec::with_capacity(4);
		let mut has_pipes = false;

		loop {
			if self.is_eof() {
				break;
			}

			// Check for block-terminating tokens
			if let Ok(current) = self.current() {
				if current.is_operator(Operator::CloseCurly) {
					break;
				}
				if current.is_separator(Separator::Semicolon) {
					self.advance()?; // consume semicolon
					break;
				}
			}

			let node = self.parse_node(crate::ast::parse::Precedence::None)?;
			nodes.push(node);

			if !self.is_eof() {
				if let Ok(current) = self.current() {
					if current.is_operator(Operator::CloseCurly) {
						break;
					}
					if current.is_separator(Separator::Semicolon) {
						self.advance()?; // consume semicolon
						break;
					}
					if current.is_operator(Operator::Pipe) {
						self.advance()?; // consume pipe
						has_pipes = true;
					} else {
						self.consume_if(TokenKind::Separator(Separator::NewLine))?;
					}
				}
			}
		}

		Ok(crate::ast::ast::AstStatement {
			nodes,
			has_pipes,
			is_output: false,
		})
	}
}
