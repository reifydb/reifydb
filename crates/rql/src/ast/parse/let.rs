// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::error::{AstErrorKind, Error, TypeError};

use crate::{
	Result,
	ast::{
		ast::{AstLet, LetValue},
		identifier::UnqualifiedIdentifier,
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{keyword::Keyword, operator::Operator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_let(&mut self) -> Result<AstLet<'bump>> {
		let token = self.current()?;

		if !self.current()?.is_keyword(Keyword::Let) {
			let fragment = self.current()?.fragment.to_owned();
			return Err(Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "expected 'let'".to_string(),
				},
				message: format!(
					"Unexpected token: expected {}, got {}",
					"expected 'let'",
					fragment.text()
				),
				fragment,
			}));
		}
		self.advance()?;

		let variable_token = self.current()?;
		if !matches!(variable_token.kind, TokenKind::Variable) {
			let fragment = variable_token.fragment.to_owned();
			return Err(Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "expected variable name starting with '$'".to_string(),
				},
				message: format!(
					"Unexpected token: expected {}, got {}",
					"expected variable name starting with '$'",
					fragment.text()
				),
				fragment,
			}));
		}

		let var_token = self.advance()?;

		let name = UnqualifiedIdentifier::new(var_token);

		self.consume_operator(Operator::Equal)?;

		let value = self.parse_let_value()?;

		Ok(AstLet {
			token,
			name,
			value,
		})
	}

	pub(crate) fn parse_let_value(&mut self) -> Result<LetValue<'bump>> {
		if !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly) {
			self.advance()?;
			let statement = self.parse_block_statement()?;
			self.consume_operator(Operator::CloseCurly)?;
			Ok(LetValue::Statement(statement))
		} else if self.is_statement()? {
			let statement = self.parse_statement_content()?;
			Ok(LetValue::Statement(statement))
		} else {
			let expr = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());
			Ok(LetValue::Expression(expr))
		}
	}

	fn is_statement(&self) -> Result<bool> {
		if let Ok(token) = self.current() {
			Ok(matches!(
				token.kind,
				TokenKind::Keyword(Keyword::From)
					| TokenKind::Keyword(Keyword::Map) | TokenKind::Keyword(Keyword::Extend)
					| TokenKind::Keyword(Keyword::Assert)
			) || (matches!(token.kind, TokenKind::Variable) && self.has_pipe_ahead()))
		} else {
			Ok(false)
		}
	}
}
