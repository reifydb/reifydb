// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::{Error, diagnostic::ast::unexpected_token_error};

use crate::{
	ast::{
		identifier::{MaybeQualifiedColumnIdentifier, UnqualifiedIdentifier},
		parse::Parser,
	},
	bump::BumpFragment,
	token::{
		operator::Operator,
		token::{Literal, Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_identifier(&mut self) -> crate::Result<UnqualifiedIdentifier<'bump>> {
		let token = self.consume(TokenKind::Identifier)?;
		Ok(UnqualifiedIdentifier::new(token))
	}

	/// Parse an identifier or keyword as an identifier (simple, no hyphen handling)
	/// Used in expression contexts where hyphens should remain as operators
	pub(crate) fn parse_as_identifier(&mut self) -> crate::Result<UnqualifiedIdentifier<'bump>> {
		let token = self.advance()?;
		debug_assert!(matches!(token.kind, TokenKind::Identifier | TokenKind::Keyword(_)));
		Ok(UnqualifiedIdentifier::new(token))
	}

	/// Parse an identifier that may contain hyphens, allowing keywords as the first token
	/// Used in DDL contexts where hyphenated identifiers are supported
	/// Consumes: (identifier|keyword) [-(identifier|keyword)]*
	/// Returns: UnqualifiedIdentifier with combined text
	pub(crate) fn parse_identifier_with_hyphens(&mut self) -> crate::Result<UnqualifiedIdentifier<'bump>> {
		let first_token = self.advance()?;

		// Helper to check if a token can be used as an identifier part
		let is_identifier_like = |token: &Token| {
			matches!(
				token.kind,
				TokenKind::Identifier | TokenKind::Keyword(_) | TokenKind::Literal(Literal::Number)
			)
		};

		// Helper to check if two tokens are adjacent (no space between them)
		let is_adjacent = |prev: &Token, next: &Token| {
			prev.fragment.line() == next.fragment.line()
				&& *prev.fragment.column() + prev.fragment.text().len() as u32
					== *next.fragment.column()
		};

		// Build hyphenated identifier - start with first token

		// Reject identifiers that start with a number
		if matches!(first_token.kind, TokenKind::Literal(Literal::Number)) {
			return Err(Error(unexpected_token_error(
				"identifier (identifiers cannot start with digits)",
				first_token.fragment.to_owned(),
			)));
		}
		let mut parts = vec![first_token.fragment.text().to_string()];
		let start_line = first_token.fragment.line();
		let start_column = first_token.fragment.column();
		let first_fragment = first_token.fragment;

		// Check if next token is hyphen followed by identifier or keyword
		// If not, return what we have so far
		if self.is_eof()
			|| self.current_expect_operator(Operator::Minus).is_err()
			|| self.position + 1 >= self.tokens.len()
			|| !is_identifier_like(&self.tokens[self.position + 1])
		{
			let combined_text = parts.join("");
			let text = self.bump().alloc_str(&combined_text);
			let fragment = BumpFragment::Statement {
				text,
				line: start_line,
				column: start_column,
			};
			return Ok(UnqualifiedIdentifier::from_fragment(fragment));
		}

		let mut last_token;
		// Look for pattern: - (identifier | keyword | number)
		// Also handle adjacent identifier after number (e.g., "10min" tokenizes as "10" + "min")
		while !self.is_eof()
			&& self.current_expect_operator(Operator::Minus).is_ok()
			&& self.position + 1 < self.tokens.len()
			&& is_identifier_like(&self.tokens[self.position + 1])
		{
			self.consume_operator(Operator::Minus)?; // consume hyphen
			let next_token = self.advance()?; // consume identifier or keyword or number
			parts.push("-".to_string());
			parts.push(next_token.fragment.text().to_string());
			last_token = next_token;

			// Special case: if we just consumed a number, check if next token is an identifier
			// that's adjacent (no space), e.g., "10" followed by "min" in "10min"
			if matches!(last_token.kind, TokenKind::Literal(Literal::Number))
				&& !self.is_eof() && matches!(self.tokens[self.position].kind, TokenKind::Identifier)
				&& is_adjacent(&last_token, &self.tokens[self.position])
			{
				let adjacent_identifier = self.advance()?;
				parts.push(adjacent_identifier.fragment.text().to_string());
			}
		}

		let combined_text = parts.join("");

		// Validate: no consecutive hyphens
		if combined_text.contains("--") {
			return Err(Error(unexpected_token_error(
				"identifier without consecutive hyphens",
				first_fragment.to_owned(),
			)));
		}

		// Create Fragment with combined text
		let text = self.bump().alloc_str(&combined_text);
		let fragment = BumpFragment::Statement {
			text,
			line: start_line,
			column: start_column,
		};

		Ok(UnqualifiedIdentifier::from_fragment(fragment))
	}

	/// Parse a potentially qualified column identifier
	/// Handles patterns like: column, table.column, namespace.table.column,
	/// alias.column
	/// Supports hyphenated identifiers like: my-column, my-table.my-column
	pub(crate) fn parse_column_identifier(&mut self) -> crate::Result<MaybeQualifiedColumnIdentifier<'bump>> {
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
				Ok(MaybeQualifiedColumnIdentifier::with_primitive(
					Some(first.into_fragment()),
					second.into_fragment(),
					third.into_fragment(),
				))
			} else {
				// table.column or alias.column
				// At parse time, we don't know if first is a
				// table or alias The resolve will
				// determine this
				Ok(MaybeQualifiedColumnIdentifier::with_primitive(
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
	) -> crate::Result<MaybeQualifiedColumnIdentifier<'bump>> {
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
				Ok(MaybeQualifiedColumnIdentifier::with_primitive(
					Some(first.fragment),
					second.fragment,
					third.fragment,
				))
			} else {
				// table.column or alias.column
				Ok(MaybeQualifiedColumnIdentifier::with_primitive(
					None,
					first.fragment,
					second.fragment,
				))
			}
		} else {
			// Unqualified column
			Ok(MaybeQualifiedColumnIdentifier::unqualified(first.fragment))
		}
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{
				Ast::{Create, Identifier},
				AstCreate::Namespace,
			},
			parse::parse,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn identifier() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "x").unwrap().into_iter().collect();
		let mut result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Identifier(identifier) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};
		assert_eq!(identifier.text(), "x");
	}

	#[test]
	fn identifier_with_underscore() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "some_identifier").unwrap().into_iter().collect();
		let mut result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Identifier(identifier) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};
		assert_eq!(identifier.text(), "some_identifier");
	}

	#[test]
	fn identifier_with_hyphen_context_aware() {
		let bump = Bump::new();
		// Test hyphenated identifier in CREATE NAMESPACE context
		let tokens = tokenize(&bump, "CREATE NAMESPACE my-identifier").unwrap().into_iter().collect();
		let mut result = parse(&bump, tokens).unwrap();
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
		let bump = Bump::new();
		// Test identifier with multiple hyphens in CREATE NAMESPACE context
		let tokens = tokenize(&bump, "CREATE NAMESPACE user-profile-data").unwrap().into_iter().collect();
		let mut result = parse(&bump, tokens).unwrap();
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
		let bump = Bump::new();
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

		let tokens: Vec<_> = tokenize(&bump, "CREATE NAMESPACE name--space").unwrap().into_iter().collect();
		assert_eq!(tokens.len(), 6); // CREATE, NAMESPACE, name, -, -, space

		let result = parse(&bump, tokens);

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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "`my-identifier`").unwrap().into_iter().collect();
		let mut result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Identifier(identifier) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};
		assert_eq!(identifier.text(), "my-identifier");
	}

	#[test]
	fn identifier_backtick_without_hyphen() {
		let bump = Bump::new();
		// Test that backticks work for simple identifiers without special characters
		let tokens = tokenize(&bump, "`myidentifier`").unwrap().into_iter().collect();
		let mut result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Identifier(identifier) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};
		assert_eq!(identifier.text(), "myidentifier");
	}

	#[test]
	fn identifier_backtick_with_underscore() {
		let bump = Bump::new();
		// Test that backticks work for identifiers with underscores
		let tokens = tokenize(&bump, "`my_identifier`").unwrap().into_iter().collect();
		let mut result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Identifier(identifier) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};
		assert_eq!(identifier.text(), "my_identifier");
	}

	#[test]
	fn identifier_with_hyphen_and_number_suffix() {
		let bump = Bump::new();
		// Number suffix is valid: twap-10min
		let tokens = tokenize(&bump, "CREATE NAMESPACE twap-10min").unwrap().into_iter().collect();
		let mut result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Create(create) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};

		if let Namespace(ns) = create {
			assert_eq!(ns.namespace.name.text(), "twap-10min");
		} else {
			panic!("Expected namespace creation");
		}
	}

	#[test]
	fn identifier_with_hyphen_and_number_middle() {
		let bump = Bump::new();
		// Number in middle is valid: avg-10min-window
		let tokens = tokenize(&bump, "CREATE NAMESPACE avg-10min-window").unwrap().into_iter().collect();
		let mut result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Create(create) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};

		if let Namespace(ns) = create {
			assert_eq!(ns.namespace.name.text(), "avg-10min-window");
		} else {
			panic!("Expected namespace creation");
		}
	}

	#[test]
	fn identifier_with_keyword_and_number() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE NAMESPACE create-2024-table").unwrap().into_iter().collect();
		let mut result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Create(create) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};

		if let Namespace(ns) = create {
			assert_eq!(ns.namespace.name.text(), "create-2024-table");
		} else {
			panic!("Expected namespace creation");
		}
	}
}
