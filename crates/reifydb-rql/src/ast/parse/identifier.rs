// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{ast::AstIdentifier, parse::Parser, tokenize::TokenKind};

impl Parser {
	pub(crate) fn parse_identifier(
		&mut self,
	) -> crate::Result<AstIdentifier> {
		let token = self.consume(TokenKind::Identifier)?;
		Ok(AstIdentifier(token))
	}

	pub(crate) fn parse_as_identifier(
		&mut self,
	) -> crate::Result<AstIdentifier> {
		let token = self.advance()?;
		debug_assert!(matches!(
			token.kind,
			TokenKind::Identifier | TokenKind::Keyword(_)
		));
		Ok(AstIdentifier(token))
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{
		Ast::Identifier, ast::AstIdentifier, parse::parse,
		tokenize::tokenize,
	};

	#[test]
	fn identifier() {
		let tokens = tokenize("x").unwrap();
		let mut result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Identifier(AstIdentifier(token)) =
			result.pop().unwrap().0.pop().unwrap()
		else {
			panic!()
		};
		assert_eq!(token.value(), "x");
	}

	#[test]
	fn identifier_with_underscore() {
		let tokens = tokenize("some_identifier").unwrap();
		let mut result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Identifier(AstIdentifier(token)) =
			result.pop().unwrap().0.pop().unwrap()
		else {
			panic!()
		};
		assert_eq!(token.value(), "some_identifier");
	}
}
