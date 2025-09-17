// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	identifier::{MaybeQualifiedColumnIdentifier, UnqualifiedIdentifier},
	parse::Parser,
	tokenize::{Operator, TokenKind},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_identifier(&mut self) -> crate::Result<UnqualifiedIdentifier<'a>> {
		let token = self.consume(TokenKind::Identifier)?;
		Ok(UnqualifiedIdentifier::new(token))
	}

	pub(crate) fn parse_as_identifier(&mut self) -> crate::Result<UnqualifiedIdentifier<'a>> {
		let token = self.advance()?;
		debug_assert!(matches!(token.kind, TokenKind::Identifier | TokenKind::Keyword(_)));
		Ok(UnqualifiedIdentifier::new(token))
	}

	/// Parse a potentially qualified column identifier
	/// Handles patterns like: column, table.column, namespace.table.column,
	/// alias.column
	pub(crate) fn parse_column_identifier(&mut self) -> crate::Result<MaybeQualifiedColumnIdentifier<'a>> {
		let first = self.consume(TokenKind::Identifier)?;

		// Check for qualification
		if !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
			self.consume_operator(Operator::Dot)?;
			let second = self.consume(TokenKind::Identifier)?;

			// Check for further qualification
			// (namespace.table.column)
			if !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
				self.consume_operator(Operator::Dot)?;
				let third = self.consume(TokenKind::Identifier)?;

				// namespace.table.column
				Ok(MaybeQualifiedColumnIdentifier::with_source(
					Some(first.fragment.clone()),
					second.fragment.clone(),
					third.fragment.clone(),
				))
			} else {
				// table.column or alias.column
				// At parse time, we don't know if first is a
				// table or alias The resolver will
				// determine this
				Ok(MaybeQualifiedColumnIdentifier::with_source(
					None,
					first.fragment.clone(),
					second.fragment.clone(),
				))
			}
		} else {
			// Unqualified column
			Ok(MaybeQualifiedColumnIdentifier::unqualified(first.fragment.clone()))
		}
	}

	/// Parse a column identifier, but also accept keywords as column names
	pub(crate) fn parse_column_identifier_or_keyword(
		&mut self,
	) -> crate::Result<MaybeQualifiedColumnIdentifier<'a>> {
		// For simple cases where keywords can be column names
		let first = self.advance()?;

		// Check for qualification
		if !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
			self.consume_operator(Operator::Dot)?;
			let second = self.advance()?;

			// Check for further qualification
			if !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
				self.consume_operator(Operator::Dot)?;
				let third = self.advance()?;

				// namespace.table.column
				Ok(MaybeQualifiedColumnIdentifier::with_source(
					Some(first.fragment.clone()),
					second.fragment.clone(),
					third.fragment.clone(),
				))
			} else {
				// table.column or alias.column
				Ok(MaybeQualifiedColumnIdentifier::with_source(
					None,
					first.fragment.clone(),
					second.fragment.clone(),
				))
			}
		} else {
			// Unqualified column
			Ok(MaybeQualifiedColumnIdentifier::unqualified(first.fragment.clone()))
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{Ast::Identifier, parse::parse, tokenize::tokenize};

	#[test]
	fn identifier() {
		let tokens = tokenize("x").unwrap();
		let mut result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Identifier(identifier) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};
		assert_eq!(identifier.text(), "x");
	}

	#[test]
	fn identifier_with_underscore() {
		let tokens = tokenize("some_identifier").unwrap();
		let mut result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Identifier(identifier) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};
		assert_eq!(identifier.text(), "some_identifier");
	}
}
