// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	AstLet,
	parse::Parser,
	tokenize::{Keyword, Operator},
};

impl<'a> Parser<'a> {
	/// Parse a variable declaration: `let name = expression` or `mut name = expression`
	pub(crate) fn parse_variable_declaration(&mut self) -> crate::Result<AstLet<'a>> {
		// Check if it's let or mut
		let is_mut = if self.current()?.is_keyword(Keyword::Mut) {
			true
		} else if self.current()?.is_keyword(Keyword::Let) {
			false
		} else {
			return Err(reifydb_type::Error(reifydb_type::diagnostic::ast::unexpected_token_error(
				"expected 'let' or 'mut'",
				self.current()?.fragment.clone(),
			)));
		};

		// Consume the let/mut keyword
		let token = self.advance()?;

		// Parse the variable name (identifier)
		let name = self.parse_as_identifier()?;

		// Consume the '=' operator
		self.consume_operator(Operator::Equal)?;

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
