// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::{AstErrorKind, Error, TypeError};

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

		// Expect 'let' keyword
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
		self.advance()?; // consume 'let'

		// Parse the variable name (must start with $)
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

		// Use the variable token directly but create an identifier with the '$' prefix
		// The UnqualifiedIdentifier will store the full token but we'll extract the name later
		let name = UnqualifiedIdentifier::new(var_token);

		// Consume the '=' operator
		self.consume_operator(Operator::Equal)?;

		// Check if the RHS is a statement or an expression
		let value = if self.is_statement()? {
			let statement = self.parse_statement_content()?;
			LetValue::Statement(statement)
		} else {
			let expr = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());
			LetValue::Expression(expr)
		};

		Ok(AstLet {
			token,
			name,
			value,
		})
	}

	/// Check if the current token starts a statement (FROM, MAP, EXTEND, etc.)
	/// Also checks for variables followed by pipes ($var | ...)
	fn is_statement(&self) -> Result<bool> {
		if let Ok(token) = self.current() {
			Ok(matches!(
				token.kind,
				TokenKind::Keyword(Keyword::From)
					| TokenKind::Keyword(Keyword::Map) | TokenKind::Keyword(Keyword::Extend)
			) || (matches!(token.kind, TokenKind::Variable) && self.has_pipe_ahead()))
		} else {
			Ok(false)
		}
	}
}
