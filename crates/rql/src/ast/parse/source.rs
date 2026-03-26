// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{AstConfigPair, AstCreate, AstCreateSource},
		identifier::{MaybeQualifiedIdentifier, MaybeQualifiedSourceIdentifier},
		parse::{Parser, Precedence},
	},
	token::{
		keyword::Keyword,
		operator::Operator,
		separator::Separator,
		token::{Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	/// Parse CREATE SOURCE name AS { FROM connector { config } TO target }
	pub(crate) fn parse_source(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		// Parse source name: ns::name
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let source_ident = MaybeQualifiedSourceIdentifier::new(name).with_namespace(namespace);

		// Expect AS {
		self.consume_operator(Operator::As)?;
		self.consume_operator(Operator::OpenCurly)?;
		self.skip_new_line()?;

		// Expect FROM connector_name { config }
		self.consume_keyword(Keyword::From)?;
		let connector = self.consume(TokenKind::Identifier)?.fragment;

		let config = self.parse_config_block()?;

		self.skip_new_line()?;

		// Expect TO target
		self.consume_keyword(Keyword::To)?;
		let mut target_segments = self.parse_double_colon_separated_identifiers()?;
		let target_name = target_segments.pop().unwrap().into_fragment();
		let target_namespace: Vec<_> = target_segments.into_iter().map(|s| s.into_fragment()).collect();
		let target = MaybeQualifiedIdentifier::new(target_name).with_namespace(target_namespace);

		self.skip_new_line()?;

		// Expect closing }
		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstCreate::Source(AstCreateSource {
			token,
			name: source_ident,
			connector,
			config,
			target,
		}))
	}

	/// Parse a generic config block: { key: value, key: value, ... }
	pub(crate) fn parse_config_block(&mut self) -> Result<Vec<AstConfigPair<'bump>>> {
		self.consume_operator(Operator::OpenCurly)?;

		let mut pairs = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let key = self.consume(TokenKind::Identifier)?.fragment;
			self.consume_operator(Operator::Colon)?;

			let value = self.parse_node(Precedence::None)?;

			pairs.push(AstConfigPair {
				key,
				value,
			});

			self.skip_new_line()?;

			// Optional comma separator
			if self.consume_if(TokenKind::Separator(Separator::Comma))?.is_some() {
				continue;
			}

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(pairs)
	}
}

#[cfg(test)]
mod tests {
	use crate::{
		ast::{ast::AstCreate, parse::Parser},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_create_source_basic() {
		let bump = Bump::new();
		let input = r#"CREATE SOURCE shop::order_sync AS {
			FROM postgres {
				uri: "postgres://localhost/mydb",
				query: "SELECT * FROM orders"
			}
			TO shop::orders
		}"#;
		let tokens = tokenize(&bump, input).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, input, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();
		let AstCreate::Source(source) = create else {
			panic!("expected Source");
		};

		assert_eq!(source.name.namespace[0].text(), "shop");
		assert_eq!(source.name.name.text(), "order_sync");
		assert_eq!(source.connector.text(), "postgres");
		assert_eq!(source.config.len(), 2);
		assert_eq!(source.config[0].key.text(), "uri");
		assert_eq!(source.config[1].key.text(), "query");
		assert_eq!(source.target.namespace[0].text(), "shop");
		assert_eq!(source.target.name.text(), "orders");
	}

	#[test]
	fn test_create_source_no_namespace() {
		let bump = Bump::new();
		let input = r#"CREATE SOURCE my_source AS {
			FROM kafka {
				uri: "broker:9092",
				topic: "events"
			}
			TO events
		}"#;
		let tokens = tokenize(&bump, input).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, input, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();
		let AstCreate::Source(source) = create else {
			panic!("expected Source");
		};

		assert!(source.name.namespace.is_empty());
		assert_eq!(source.name.name.text(), "my_source");
		assert_eq!(source.connector.text(), "kafka");
		assert!(source.target.namespace.is_empty());
		assert_eq!(source.target.name.text(), "events");
	}
}
