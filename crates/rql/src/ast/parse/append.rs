// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{Parser, Precedence};
use crate::{
	ast::ast::{Ast, AstAppend, AstAppendSource, AstFrom, AstStatement, AstVariable},
	token::{keyword::Keyword, operator::Operator, separator::Separator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_append(&mut self) -> crate::Result<AstAppend<'bump>> {
		let token = self.current()?;
		// Consume APPEND keyword
		self.advance()?;

		// Parse target variable ($name)
		let variable_token = self.current()?;
		if !matches!(variable_token.kind, TokenKind::Variable) {
			return Err(reifydb_type::error::Error(
				reifydb_type::error::diagnostic::ast::unexpected_token_error(
					"expected variable name starting with '$'",
					variable_token.fragment.to_owned(),
				),
			));
		}
		let var_token = self.advance()?;
		let target = AstVariable {
			token: var_token,
		};

		// Consume FROM keyword
		self.consume_keyword(Keyword::From)?;

		// Dispatch on next token
		let source = if !self.is_eof() && self.current()?.is_operator(Operator::OpenBracket) {
			// Inline: APPEND $x FROM [{...}]
			AstAppendSource::Inline(self.parse_list()?)
		} else if !self.is_eof() && matches!(self.current()?.kind, TokenKind::Variable) {
			// Variable source: always treat as frame source, then parse any pipe continuation
			let src_token = self.advance()?;
			let variable = AstVariable {
				token: src_token,
			};
			let first_node = Ast::From(AstFrom::Variable {
				token: src_token,
				variable,
			});

			let mut nodes = vec![first_node];
			let mut has_pipes = false;

			// Check for pipe continuation (e.g., $data | filter { ... })
			while !self.is_eof() {
				if let Ok(current) = self.current() {
					if current.is_separator(Separator::Semicolon) {
						break;
					}
				}
				if self.current()?.is_operator(Operator::Pipe) {
					self.advance()?; // consume the pipe
					has_pipes = true;
					nodes.push(self.parse_node(Precedence::None)?);
				} else {
					break;
				}
			}

			AstAppendSource::Statement(AstStatement {
				nodes,
				has_pipes,
				is_output: false,
			})
		} else {
			// Statement: APPEND $x FROM table | FILTER ...
			let statement = self.parse_statement_content()?;
			AstAppendSource::Statement(statement)
		};

		Ok(AstAppend {
			token,
			target,
			source,
		})
	}
}
