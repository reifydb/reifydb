// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		identifier::{MaybeQualifiedColumnIdentifier, UnqualifiedIdentifier},
		parse::Parser,
	},
	bump::BumpFragment,
	diagnostic::AstError,
	token::{
		operator::Operator,
		token::{Literal, Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_identifier(&mut self) -> Result<UnqualifiedIdentifier<'bump>> {
		let token = self.consume(TokenKind::Identifier)?;
		Ok(UnqualifiedIdentifier::new(token))
	}

	pub(crate) fn parse_as_identifier(&mut self) -> Result<UnqualifiedIdentifier<'bump>> {
		let token = self.consume_name()?;
		Ok(UnqualifiedIdentifier::new(token))
	}

	pub(crate) fn parse_identifier_with_hyphens(&mut self) -> Result<UnqualifiedIdentifier<'bump>> {
		let first_token = self.advance()?;

		let is_identifier_like = |token: &Token| {
			matches!(
				token.kind,
				TokenKind::Identifier | TokenKind::Keyword(_) | TokenKind::Literal(Literal::Number)
			)
		};

		if matches!(first_token.kind, TokenKind::Literal(Literal::Number)) {
			let has_hyphen_continuation = !self.is_eof()
				&& self.current_expect_operator(Operator::Minus).is_ok()
				&& self.position + 1 < self.tokens.len()
				&& is_identifier_like(&self.tokens[self.position + 1]);
			if !has_hyphen_continuation {
				return Err(AstError::UnexpectedToken {
					expected: "identifier (identifiers cannot start with digits)".to_string(),
					fragment: first_token.fragment.to_owned(),
				}
				.into());
			}
		}
		let start_line = first_token.fragment.line();
		let start_column = first_token.fragment.column();
		let first_fragment = first_token.fragment;

		if self.is_eof()
			|| self.current_expect_operator(Operator::Minus).is_err()
			|| self.position + 1 >= self.tokens.len()
			|| !is_identifier_like(&self.tokens[self.position + 1])
		{
			let text = self.bump().alloc_str(first_token.fragment.text());
			let fragment = BumpFragment::Statement {
				text,
				offset: 0,
				source_end: 0,
				line: start_line,
				column: start_column,
			};
			return Ok(UnqualifiedIdentifier::from_fragment(fragment));
		}

		let mut combined = String::from(first_token.fragment.text());

		while !self.is_eof()
			&& self.current_expect_operator(Operator::Minus).is_ok()
			&& self.position + 1 < self.tokens.len()
			&& is_identifier_like(&self.tokens[self.position + 1])
		{
			self.consume_operator(Operator::Minus)?;
			let next_token = self.advance()?;
			combined.push('-');
			combined.push_str(next_token.fragment.text());
		}

		if combined.contains("--") {
			return Err(AstError::UnexpectedToken {
				expected: "identifier without consecutive hyphens".to_string(),
				fragment: first_fragment.to_owned(),
			}
			.into());
		}

		let text = self.bump().alloc_str(&combined);
		let fragment = BumpFragment::Statement {
			text,
			offset: 0,
			source_end: 0,
			line: start_line,
			column: start_column,
		};

		Ok(UnqualifiedIdentifier::from_fragment(fragment))
	}

	pub(crate) fn parse_double_colon_separated_identifiers(&mut self) -> Result<Vec<UnqualifiedIdentifier<'bump>>> {
		let mut segments = vec![self.parse_identifier_with_hyphens()?];
		while !self.is_eof() && self.current_expect_operator(Operator::DoubleColon).is_ok() {
			self.consume_operator(Operator::DoubleColon)?;
			segments.push(self.parse_identifier_with_hyphens()?);
		}
		Ok(segments)
	}

	pub(crate) fn parse_column_identifier(&mut self) -> Result<MaybeQualifiedColumnIdentifier<'bump>> {
		let mut ns_table_segments = self.parse_double_colon_separated_identifiers()?;

		if !self.is_eof() && self.current_expect_operator(Operator::Dot).is_ok() {
			self.consume_operator(Operator::Dot)?;
			let col = self.parse_identifier_with_hyphens()?;

			let table = ns_table_segments.pop().unwrap();
			let namespace: Vec<_> = ns_table_segments.into_iter().map(|s| s.into_fragment()).collect();

			Ok(MaybeQualifiedColumnIdentifier::with_shape(
				namespace,
				table.into_fragment(),
				col.into_fragment(),
			))
		} else {
			Self::segments_to_column_identifier(ns_table_segments)
		}
	}

	fn segments_to_column_identifier(
		mut segments: Vec<UnqualifiedIdentifier<'bump>>,
	) -> Result<MaybeQualifiedColumnIdentifier<'bump>> {
		match segments.len() {
			1 => {
				let col = segments.remove(0);
				Ok(MaybeQualifiedColumnIdentifier::unqualified(col.into_fragment()))
			}
			2 => {
				let table = segments.remove(0);
				let col = segments.remove(0);
				Ok(MaybeQualifiedColumnIdentifier::with_shape(
					vec![],
					table.into_fragment(),
					col.into_fragment(),
				))
			}
			_ => {
				let col = segments.pop().unwrap();
				let table = segments.pop().unwrap();
				let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
				Ok(MaybeQualifiedColumnIdentifier::with_shape(
					namespace,
					table.into_fragment(),
					col.into_fragment(),
				))
			}
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
		let source = "x";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Identifier(identifier) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};
		assert_eq!(identifier.text(), "x");
	}

	#[test]
	fn identifier_with_underscore() {
		let bump = Bump::new();
		let source = "some_identifier";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
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
		let source = "CREATE NAMESPACE my-identifier";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Create(create) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};

		if let Namespace(ns) = create {
			assert_eq!(ns.namespace.segments[0].text(), "my-identifier");
		} else {
			panic!("Expected namespace creation");
		}
	}

	#[test]
	fn identifier_with_multiple_hyphens() {
		let bump = Bump::new();
		// Test identifier with multiple hyphens in CREATE NAMESPACE context
		let source = "CREATE NAMESPACE user-profile-data";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Create(create) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};

		if let Namespace(ns) = create {
			assert_eq!(ns.namespace.segments[0].text(), "user-profile-data");
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

		let source = "CREATE NAMESPACE name--space";
		let tokens: Vec<_> = tokenize(&bump, source).unwrap().into_iter().collect();
		assert_eq!(tokens.len(), 6); // CREATE, NAMESPACE, name, -, -, space

		let result = parse(&bump, source, tokens);

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
		let source = "`my-identifier`";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
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
		let source = "`myidentifier`";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
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
		let source = "`my_identifier`";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
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
		let source = "CREATE NAMESPACE twap-10min";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Create(create) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};

		if let Namespace(ns) = create {
			assert_eq!(ns.namespace.segments[0].text(), "twap-10min");
		} else {
			panic!("Expected namespace creation");
		}
	}

	#[test]
	fn identifier_with_hyphen_and_number_middle() {
		let bump = Bump::new();
		// Number in middle is valid: avg-10min-window
		let source = "CREATE NAMESPACE avg-10min-window";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Create(create) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};

		if let Namespace(ns) = create {
			assert_eq!(ns.namespace.segments[0].text(), "avg-10min-window");
		} else {
			panic!("Expected namespace creation");
		}
	}

	#[test]
	fn identifier_with_keyword_and_number() {
		let bump = Bump::new();
		let source = "CREATE NAMESPACE create-2024-table";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Create(create) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};

		if let Namespace(ns) = create {
			assert_eq!(ns.namespace.segments[0].text(), "create-2024-table");
		} else {
			panic!("Expected namespace creation");
		}
	}

	#[test]
	fn identifier_digit_starting() {
		let bump = Bump::new();
		let source = "CREATE NAMESPACE 10min";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Create(create) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};

		if let Namespace(ns) = create {
			assert_eq!(ns.namespace.segments[0].text(), "10min");
		} else {
			panic!("Expected namespace creation");
		}
	}

	#[test]
	fn identifier_digit_starting_with_hyphen() {
		let bump = Bump::new();
		let source = "CREATE NAMESPACE 10min-window";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Create(create) = result.pop().unwrap().nodes.pop().unwrap() else {
			panic!()
		};

		if let Namespace(ns) = create {
			assert_eq!(ns.namespace.segments[0].text(), "10min-window");
		} else {
			panic!("Expected namespace creation");
		}
	}
}
