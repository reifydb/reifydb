// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{AstCreate, AstCreateSink},
		identifier::{MaybeQualifiedIdentifier, MaybeQualifiedSinkIdentifier},
		parse::Parser,
	},
	token::{
		keyword::Keyword,
		operator::Operator,
		token::{Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	/// Parse CREATE SINK name AS { FROM source TO connector { config } }
	pub(crate) fn parse_sink(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		// Parse sink name: ns::name
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let sink_ident = MaybeQualifiedSinkIdentifier::new(name).with_namespace(namespace);

		// Expect AS {
		self.consume_operator(Operator::As)?;
		self.consume_operator(Operator::OpenCurly)?;
		self.skip_new_line()?;

		// Expect FROM source
		self.consume_keyword(Keyword::From)?;
		let mut source_segments = self.parse_double_colon_separated_identifiers()?;
		let source_name = source_segments.pop().unwrap().into_fragment();
		let source_namespace: Vec<_> = source_segments.into_iter().map(|s| s.into_fragment()).collect();
		let source = MaybeQualifiedIdentifier::new(source_name).with_namespace(source_namespace);

		self.skip_new_line()?;

		// Expect TO connector { config }
		self.consume_keyword(Keyword::To)?;
		let connector = self.consume(TokenKind::Identifier)?.fragment;

		let config = self.parse_config_block()?;

		self.skip_new_line()?;

		// Expect closing }
		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstCreate::Sink(AstCreateSink {
			token,
			name: sink_ident,
			source,
			connector,
			config,
		}))
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
	fn test_create_sink_basic() {
		let bump = Bump::new();
		let input = r#"CREATE SINK shop::order_export AS {
			FROM shop::orders
			TO postgres {
				uri: "postgres://warehouse/db",
				table: "exported_orders"
			}
		}"#;
		let tokens = tokenize(&bump, input).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, input, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();
		let AstCreate::Sink(sink) = create else {
			panic!("expected Sink");
		};

		assert_eq!(sink.name.namespace[0].text(), "shop");
		assert_eq!(sink.name.name.text(), "order_export");
		assert_eq!(sink.source.namespace[0].text(), "shop");
		assert_eq!(sink.source.name.text(), "orders");
		assert_eq!(sink.connector.text(), "postgres");
		assert_eq!(sink.config.len(), 2);
		assert_eq!(sink.config[0].key.text(), "uri");
		assert_eq!(sink.config[1].key.text(), "table");
	}

	#[test]
	fn test_create_sink_no_namespace() {
		let bump = Bump::new();
		let input = r#"CREATE SINK my_export AS {
			FROM events
			TO kafka {
				uri: "broker:9092",
				topic: "exported-events"
			}
		}"#;
		let tokens = tokenize(&bump, input).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, input, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();
		let AstCreate::Sink(sink) = create else {
			panic!("expected Sink");
		};

		assert!(sink.name.namespace.is_empty());
		assert_eq!(sink.name.name.text(), "my_export");
		assert!(sink.source.namespace.is_empty());
		assert_eq!(sink.source.name.text(), "events");
		assert_eq!(sink.connector.text(), "kafka");
	}
}
