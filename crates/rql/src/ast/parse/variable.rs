// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	AstLet,
	parse::Parser,
	tokenize::{Keyword, Operator},
};

impl<'a> Parser<'a> {
	/// Parse a variable declaration: `let $name = expression` or `let mut $name = expression` or `mut $name =
	/// expression`
	pub(crate) fn parse_variable_declaration(&mut self) -> crate::Result<AstLet<'a>> {
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
			return Err(reifydb_type::Error(reifydb_type::diagnostic::ast::unexpected_token_error(
				"expected 'let' or 'mut'",
				self.current()?.fragment.clone(),
			)));
		};

		// Parse the variable name (must start with $)
		let variable_token = self.current()?;
		if !matches!(variable_token.kind, crate::ast::tokenize::TokenKind::Variable) {
			return Err(reifydb_type::Error(reifydb_type::diagnostic::ast::unexpected_token_error(
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

		// Parse the value expression
		let value = Box::new(self.parse_node(crate::ast::parse::Precedence::None)?);

		Ok(AstLet {
			token,
			name,
			value,
			mutable: is_mut,
		})
	}
}
