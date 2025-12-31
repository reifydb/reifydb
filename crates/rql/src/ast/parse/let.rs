// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::diagnostic::ast::unexpected_token_error;

use crate::ast::{
	AstLet, LetValue,
	parse::{Parser, Precedence},
	tokenize::{Keyword, Operator, TokenKind},
};

impl Parser {
	pub(crate) fn parse_let(&mut self) -> crate::Result<AstLet> {
		let mut is_mut = false;
		let token = self.current()?.clone();

		// Handle both `let` and `mut` keywords
		if self.current()?.is_keyword(Keyword::Let) {
			self.advance()?; // consume 'let'

			// Check if the next token is 'mut'
			if self.current()?.is_keyword(Keyword::Mut) {
				is_mut = true;
				self.advance()?; // consume 'mut'
			}
		} else if self.current()?.is_keyword(Keyword::Mut) {
			is_mut = true;
			self.advance()?; // consume 'mut'
		} else {
			return Err(reifydb_type::Error(unexpected_token_error(
				"expected 'let' or 'mut'",
				self.current()?.fragment.clone(),
			)));
		};

		// Parse the variable name (must start with $)
		let variable_token = self.current()?;
		if !matches!(variable_token.kind, TokenKind::Variable) {
			return Err(reifydb_type::Error(unexpected_token_error(
				"expected variable name starting with '$'",
				variable_token.fragment.clone(),
			)));
		}

		let var_token = self.advance()?;

		// Use the variable token directly but create an identifier with the '$' prefix
		// The UnqualifiedIdentifier will store the full token but we'll extract the name later
		let name = crate::ast::identifier::UnqualifiedIdentifier::new(var_token);

		// Consume the ':=' operator
		self.consume_operator(Operator::ColonEqual)?;

		// Check if the RHS is a statement or an expression
		let value = if self.is_statement()? {
			let statement = self.parse_statement_content()?;
			LetValue::Statement(statement)
		} else {
			let expr = Box::new(self.parse_node(Precedence::None)?);
			LetValue::Expression(expr)
		};

		Ok(AstLet {
			token,
			name,
			value,
			mutable: is_mut,
		})
	}

	/// Check if the current token starts a statement (FROM, SELECT, MAP, EXTEND, etc.)
	/// Also checks for variables followed by pipes ($var | ...)
	fn is_statement(&self) -> crate::Result<bool> {
		if let Ok(token) = self.current() {
			Ok(matches!(
				token.kind,
				TokenKind::Keyword(Keyword::From)
					| TokenKind::Keyword(Keyword::Select)
					| TokenKind::Keyword(Keyword::Map) | TokenKind::Keyword(Keyword::Extend)
			) || (matches!(token.kind, TokenKind::Variable) && self.has_pipe_ahead()))
		} else {
			Ok(false)
		}
	}
}
