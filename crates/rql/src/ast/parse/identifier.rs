// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{Error, Fragment, OwnedFragment, diagnostic::ast::unexpected_token_error};

use crate::ast::{
	identifier::{MaybeQualifiedColumnIdentifier, UnqualifiedIdentifier},
	parse::Parser,
	tokenize::{Operator, Token, TokenKind},
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

	/// Parse an identifier that may contain hyphens, allowing keywords as the first token
	/// Consumes: (identifier|keyword) [-(identifier|keyword)]*
	/// Returns: UnqualifiedIdentifier with combined text
	pub(crate) fn parse_identifier_with_hyphens_allow_keyword(
		&mut self,
	) -> crate::Result<UnqualifiedIdentifier<'a>> {
		let first_token = self.advance()?;

		// Helper to check if a token can be used as an identifier part
		let is_identifier_like =
			|token: &Token| matches!(token.kind, TokenKind::Identifier | TokenKind::Keyword(_));

		// Check if next token is hyphen followed by identifier or keyword
		// If not, just return the first token
		if self.is_eof()
			|| self.current_expect_operator(Operator::Minus).is_err()
			|| self.position + 1 >= self.tokens.len()
			|| !is_identifier_like(&self.tokens[self.position + 1])
		{
			return Ok(UnqualifiedIdentifier::new(first_token));
		}

		// Build hyphenated identifier
		let mut parts = vec![first_token.fragment.text().to_string()];
		let start_line = first_token.fragment.line();
		let start_column = first_token.fragment.column();

		// Look for pattern: - (identifier | keyword)
		while !self.is_eof()
			&& self.current_expect_operator(Operator::Minus).is_ok()
			&& self.position + 1 < self.tokens.len()
			&& is_identifier_like(&self.tokens[self.position + 1])
		{
			self.consume_operator(Operator::Minus)?; // consume hyphen
			let next_token = self.advance()?; // consume identifier or keyword
			parts.push("-".to_string());
			parts.push(next_token.fragment.text().to_string());
		}

		let combined_text = parts.join("");

		// Validate: no consecutive hyphens
		if combined_text.contains("--") {
			return Err(Error(unexpected_token_error(
				"identifier without consecutive hyphens",
				first_token.fragment.clone(),
			)));
		}

		// Create OwnedFragment with combined text
		let fragment = Fragment::Owned(OwnedFragment::Statement {
			text: combined_text,
			line: start_line,
			column: start_column,
		});

		Ok(UnqualifiedIdentifier::from_fragment(fragment))
	}

	/// Parse an identifier that may contain hyphens
	/// Consumes: identifier [-(identifier|keyword)]*
	/// Returns: UnqualifiedIdentifier with combined text
	pub(crate) fn parse_identifier_with_hyphens(&mut self) -> crate::Result<UnqualifiedIdentifier<'a>> {
		let first_token = self.consume(TokenKind::Identifier)?;

		// Helper to check if a token can be used as an identifier part
		let is_identifier_like =
			|token: &Token| matches!(token.kind, TokenKind::Identifier | TokenKind::Keyword(_));

		// Check if next token is hyphen followed by identifier or keyword
		// If not, just return the first token
		if self.is_eof()
			|| self.current_expect_operator(Operator::Minus).is_err()
			|| self.position + 1 >= self.tokens.len()
			|| !is_identifier_like(&self.tokens[self.position + 1])
		{
			return Ok(UnqualifiedIdentifier::new(first_token));
		}

		// Build hyphenated identifier
		let mut parts = vec![first_token.fragment.text().to_string()];
		let start_line = first_token.fragment.line();
		let start_column = first_token.fragment.column();

		// Look for pattern: - (identifier | keyword)
		while !self.is_eof()
			&& self.current_expect_operator(Operator::Minus).is_ok()
			&& self.position + 1 < self.tokens.len()
			&& is_identifier_like(&self.tokens[self.position + 1])
		{
			self.consume_operator(Operator::Minus)?; // consume hyphen
			let next_token = self.advance()?; // consume identifier or keyword
			parts.push("-".to_string());
			parts.push(next_token.fragment.text().to_string());
		}

		let combined_text = parts.join("");

		// Validate: no consecutive hyphens
		if combined_text.contains("--") {
			return Err(Error(unexpected_token_error(
				"identifier without consecutive hyphens",
				first_token.fragment.clone(),
			)));
		}

		// Create OwnedFragment with combined text
		let fragment = Fragment::Owned(OwnedFragment::Statement {
			text: combined_text,
			line: start_line,
			column: start_column,
		});

		Ok(UnqualifiedIdentifier::from_fragment(fragment))
	}

	/// Parse a potentially qualified column identifier
	/// Handles patterns like: column, table.column, namespace.table.column,
	/// alias.column
	/// Supports hyphenated identifiers like: my-column, my-table.my-column
	pub(crate) fn parse_column_identifier(&mut self) -> crate::Result<MaybeQualifiedColumnIdentifier<'a>> {
		let first = self.parse_identifier_with_hyphens()?;

		// Check for qualification
		if !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
			self.consume_operator(Operator::Dot)?;
			let second = self.parse_identifier_with_hyphens()?;

			// Check for further qualification
			// (namespace.table.column)
			if !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
				self.consume_operator(Operator::Dot)?;
				let third = self.parse_identifier_with_hyphens()?;

				// namespace.table.column
				Ok(MaybeQualifiedColumnIdentifier::with_source(
					Some(first.into_fragment()),
					second.into_fragment(),
					third.into_fragment(),
				))
			} else {
				// table.column or alias.column
				// At parse time, we don't know if first is a
				// table or alias The resolve will
				// determine this
				Ok(MaybeQualifiedColumnIdentifier::with_source(
					None,
					first.into_fragment(),
					second.into_fragment(),
				))
			}
		} else {
			// Unqualified column
			Ok(MaybeQualifiedColumnIdentifier::unqualified(first.into_fragment()))
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
	use crate::ast::{
		Ast::{Create, Identifier},
		AstCreate::Namespace,
		parse::parse,
		tokenize::tokenize,
	};

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

	#[test]
	fn identifier_with_hyphen_context_aware() {
		// Test hyphenated identifier in CREATE NAMESPACE context
		let tokens = tokenize("CREATE NAMESPACE my-identifier").unwrap();
		let mut result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Create(create) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};

		if let Namespace(ns) = create {
			assert_eq!(ns.namespace.name.text(), "my-identifier");
		} else {
			panic!("Expected namespace creation");
		}
	}

	#[test]
	fn identifier_with_multiple_hyphens() {
		// Test identifier with multiple hyphens in CREATE NAMESPACE context
		let tokens = tokenize("CREATE NAMESPACE user-profile-data").unwrap();
		let mut result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Create(create) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};

		if let Namespace(ns) = create {
			assert_eq!(ns.namespace.name.text(), "user-profile-data");
		} else {
			panic!("Expected namespace creation");
		}
	}

	#[test]
	fn identifier_with_double_hyphens_should_fail() {
		// When using unquoted identifiers, double hyphens are tokenized as two minus operators
		// Input: "CREATE NAMESPACE name--space"
		// Tokens: [CREATE, NAMESPACE, name, -, -, space]
		//
		// The parser should:
		// 1. Parse "CREATE NAMESPACE name" successfully
		// 2. See trailing tokens "- - space"
		// 3. REJECT the trailing tokens as invalid after a CREATE statement
		//
		// Rationale: CREATE statements (and all DDL) should stand alone. Trailing tokens
		// are almost certainly a user error. If consecutive hyphens are intended, use backticks.

		let tokens = tokenize("CREATE NAMESPACE name--space").unwrap();
		assert_eq!(tokens.len(), 6); // CREATE, NAMESPACE, name, -, -, space

		let result = parse(tokens);

		// Parser should reject this with an error about unexpected trailing tokens
		assert!(result.is_err(), "Parser should reject trailing tokens after CREATE statement");

		// Verify error message is helpful
		if let Err(e) = result {
			let error_msg = format!("{:?}", e);
			assert!(
				error_msg.contains("unexpected") || error_msg.contains("DDL"),
				"Error should mention unexpected tokens or DDL: {}",
				error_msg
			);
		}
	}

	#[test]
	fn identifier_backtick_with_hyphen() {
		let tokens = tokenize("`my-identifier`").unwrap();
		let mut result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Identifier(identifier) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};
		assert_eq!(identifier.text(), "my-identifier");
	}

	#[test]
	fn identifier_backtick_without_hyphen() {
		// Test that backticks work for simple identifiers without special characters
		let tokens = tokenize("`myidentifier`").unwrap();
		let mut result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Identifier(identifier) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};
		assert_eq!(identifier.text(), "myidentifier");
	}

	#[test]
	fn identifier_backtick_with_underscore() {
		// Test that backticks work for identifiers with underscores
		let tokens = tokenize("`my_identifier`").unwrap();
		let mut result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Identifier(identifier) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};
		assert_eq!(identifier.text(), "my_identifier");
	}
}
