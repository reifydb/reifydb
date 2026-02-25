// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::{AstErrorKind, Error, TypeError};

use crate::{
	ast::{
		ast::{
			AstDrop, AstDropDictionary, AstDropFlow, AstDropNamespace, AstDropRingBuffer, AstDropSeries,
			AstDropSubscription, AstDropSumType, AstDropTable, AstDropView,
		},
		identifier::{
			MaybeQualifiedDictionaryIdentifier, MaybeQualifiedFlowIdentifier,
			MaybeQualifiedNamespaceIdentifier, MaybeQualifiedRingBufferIdentifier,
			MaybeQualifiedSeriesIdentifier, MaybeQualifiedSumTypeIdentifier, MaybeQualifiedTableIdentifier,
			MaybeQualifiedViewIdentifier,
		},
		parse::Parser,
	},
	token::{
		keyword::Keyword,
		token::{Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_drop(&mut self) -> crate::Result<AstDrop<'bump>> {
		let token = self.consume_keyword(Keyword::Drop)?;

		// Check what we're dropping
		if (self.consume_if(TokenKind::Keyword(Keyword::Flow))?).is_some() {
			return self.parse_drop_flow(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Table))?).is_some() {
			return self.parse_drop_table(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::View))?).is_some() {
			return self.parse_drop_view(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Ringbuffer))?).is_some() {
			return self.parse_drop_ringbuffer(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Namespace))?).is_some() {
			return self.parse_drop_namespace(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Dictionary))?).is_some() {
			return self.parse_drop_dictionary(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Enum))?).is_some() {
			return self.parse_drop_enum(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Subscription))?).is_some() {
			return self.parse_drop_subscription(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Series))?).is_some() {
			return self.parse_drop_series(token);
		}

		let fragment = self.current()?.fragment.to_owned();
		Err(Error::from(TypeError::Ast {
			kind: AstErrorKind::UnexpectedToken {
				expected: "FLOW, TABLE, VIEW, RINGBUFFER, NAMESPACE, DICTIONARY, ENUM, SUBSCRIPTION, or SERIES"
					.to_string(),
			},
			message: format!(
				"Unexpected token: expected {}, got {}",
				"FLOW, TABLE, VIEW, RINGBUFFER, NAMESPACE, DICTIONARY, ENUM, SUBSCRIPTION, or SERIES",
				fragment.text()
			),
			fragment,
		}))
	}

	/// Parse IF EXISTS clause, returning true if present.
	fn parse_if_exists(&mut self) -> crate::Result<bool> {
		if (self.consume_if(TokenKind::Keyword(Keyword::If))?).is_some() {
			self.consume_keyword(Keyword::Exists)?;
			Ok(true)
		} else {
			Ok(false)
		}
	}

	/// Parse optional CASCADE or RESTRICT clause, defaulting to RESTRICT (false).
	fn parse_cascade(&mut self) -> crate::Result<bool> {
		if (self.consume_if(TokenKind::Keyword(Keyword::Cascade))?).is_some() {
			Ok(true)
		} else if (self.consume_if(TokenKind::Keyword(Keyword::Restrict))?).is_some() {
			Ok(false)
		} else {
			Ok(false)
		}
	}

	fn parse_drop_flow(&mut self, token: Token<'bump>) -> crate::Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let flow = if namespace.is_empty() {
			MaybeQualifiedFlowIdentifier::new(name)
		} else {
			MaybeQualifiedFlowIdentifier::new(name).with_namespace(namespace)
		};

		let cascade = self.parse_cascade()?;

		Ok(AstDrop::Flow(AstDropFlow {
			token,
			if_exists,
			flow,
			cascade,
		}))
	}

	fn parse_drop_table(&mut self, token: Token<'bump>) -> crate::Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let table = if namespace.is_empty() {
			MaybeQualifiedTableIdentifier::new(name)
		} else {
			MaybeQualifiedTableIdentifier::new(name).with_namespace(namespace)
		};

		let cascade = self.parse_cascade()?;

		Ok(AstDrop::Table(AstDropTable {
			token,
			if_exists,
			table,
			cascade,
		}))
	}

	fn parse_drop_view(&mut self, token: Token<'bump>) -> crate::Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let view = if namespace.is_empty() {
			MaybeQualifiedViewIdentifier::new(name)
		} else {
			MaybeQualifiedViewIdentifier::new(name).with_namespace(namespace)
		};

		let cascade = self.parse_cascade()?;

		Ok(AstDrop::View(AstDropView {
			token,
			if_exists,
			view,
			cascade,
		}))
	}

	fn parse_drop_ringbuffer(&mut self, token: Token<'bump>) -> crate::Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let ringbuffer = if namespace.is_empty() {
			MaybeQualifiedRingBufferIdentifier::new(name)
		} else {
			MaybeQualifiedRingBufferIdentifier::new(name).with_namespace(namespace)
		};

		let cascade = self.parse_cascade()?;

		Ok(AstDrop::RingBuffer(AstDropRingBuffer {
			token,
			if_exists,
			ringbuffer,
			cascade,
		}))
	}

	fn parse_drop_namespace(&mut self, token: Token<'bump>) -> crate::Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;

		let segments: Vec<_> = self
			.parse_double_colon_separated_identifiers()?
			.into_iter()
			.map(|s| s.into_fragment())
			.collect();
		let namespace = MaybeQualifiedNamespaceIdentifier::new(segments);

		let cascade = self.parse_cascade()?;

		Ok(AstDrop::Namespace(AstDropNamespace {
			token,
			if_exists,
			namespace,
			cascade,
		}))
	}

	fn parse_drop_dictionary(&mut self, token: Token<'bump>) -> crate::Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let dictionary = if namespace.is_empty() {
			MaybeQualifiedDictionaryIdentifier::new(name)
		} else {
			MaybeQualifiedDictionaryIdentifier::new(name).with_namespace(namespace)
		};

		let cascade = self.parse_cascade()?;

		Ok(AstDrop::Dictionary(AstDropDictionary {
			token,
			if_exists,
			dictionary,
			cascade,
		}))
	}

	fn parse_drop_enum(&mut self, token: Token<'bump>) -> crate::Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let sumtype = if namespace.is_empty() {
			MaybeQualifiedSumTypeIdentifier::new(name)
		} else {
			MaybeQualifiedSumTypeIdentifier::new(name).with_namespace(namespace)
		};

		let cascade = self.parse_cascade()?;

		Ok(AstDrop::Enum(AstDropSumType {
			token,
			if_exists,
			sumtype,
			cascade,
		}))
	}

	fn parse_drop_series(&mut self, token: Token<'bump>) -> crate::Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let series = if namespace.is_empty() {
			MaybeQualifiedSeriesIdentifier::new(name)
		} else {
			MaybeQualifiedSeriesIdentifier::new(name).with_namespace(namespace)
		};

		let cascade = self.parse_cascade()?;

		Ok(AstDrop::Series(AstDropSeries {
			token,
			if_exists,
			series,
			cascade,
		}))
	}

	fn parse_drop_subscription(&mut self, token: Token<'bump>) -> crate::Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;

		let identifier = self.parse_identifier_with_hyphens()?.into_fragment();

		let cascade = self.parse_cascade()?;

		Ok(AstDrop::Subscription(AstDropSubscription {
			token,
			if_exists,
			identifier,
			cascade,
		}))
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{ast::parse::Parser, bump::Bump, token::tokenize};

	#[test]
	fn test_drop_flow_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP FLOW my_flow").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Flow(drop) = result else {
			panic!("expected Flow")
		};
		assert!(!drop.if_exists);
		assert_eq!(drop.flow.name.text(), "my_flow");
		assert!(drop.flow.namespace.is_empty());
		assert!(!drop.cascade);
	}

	#[test]
	fn test_drop_flow_if_exists() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP FLOW IF EXISTS my_flow").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Flow(drop) = result else {
			panic!("expected Flow")
		};
		assert!(drop.if_exists);
		assert_eq!(drop.flow.name.text(), "my_flow");
	}

	#[test]
	fn test_drop_flow_qualified() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP FLOW analytics::sales_flow").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Flow(drop) = result else {
			panic!("expected Flow")
		};
		assert_eq!(drop.flow.namespace[0].text(), "analytics");
		assert_eq!(drop.flow.name.text(), "sales_flow");
	}

	#[test]
	fn test_drop_flow_cascade() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP FLOW my_flow CASCADE").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Flow(drop) = result else {
			panic!("expected Flow")
		};
		assert_eq!(drop.flow.name.text(), "my_flow");
		assert!(drop.cascade);
	}

	#[test]
	fn test_drop_flow_restrict() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP FLOW my_flow RESTRICT").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Flow(drop) = result else {
			panic!("expected Flow")
		};
		assert_eq!(drop.flow.name.text(), "my_flow");
		assert!(!drop.cascade);
	}

	#[test]
	fn test_drop_flow_if_exists_cascade() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "DROP FLOW IF EXISTS test::my_flow CASCADE").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Flow(drop) = result else {
			panic!("expected Flow")
		};
		assert!(drop.if_exists);
		assert_eq!(drop.flow.namespace[0].text(), "test");
		assert_eq!(drop.flow.name.text(), "my_flow");
		assert!(drop.cascade);
	}

	#[test]
	fn test_drop_table_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP TABLE users").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Table(drop) = result else {
			panic!("expected Table")
		};
		assert!(!drop.if_exists);
		assert_eq!(drop.table.name.text(), "users");
		assert!(drop.table.namespace.is_empty());
		assert!(!drop.cascade);
	}

	#[test]
	fn test_drop_table_if_exists_qualified() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "DROP TABLE IF EXISTS analytics::users CASCADE").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Table(drop) = result else {
			panic!("expected Table")
		};
		assert!(drop.if_exists);
		assert_eq!(drop.table.namespace[0].text(), "analytics");
		assert_eq!(drop.table.name.text(), "users");
		assert!(drop.cascade);
	}

	#[test]
	fn test_drop_view_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP VIEW my_view").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::View(drop) = result else {
			panic!("expected View")
		};
		assert!(!drop.if_exists);
		assert_eq!(drop.view.name.text(), "my_view");
		assert!(drop.view.namespace.is_empty());
		assert!(!drop.cascade);
	}

	#[test]
	fn test_drop_view_if_exists_qualified() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP VIEW IF EXISTS ns::my_view CASCADE").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::View(drop) = result else {
			panic!("expected View")
		};
		assert!(drop.if_exists);
		assert_eq!(drop.view.namespace[0].text(), "ns");
		assert_eq!(drop.view.name.text(), "my_view");
		assert!(drop.cascade);
	}

	#[test]
	fn test_drop_ringbuffer_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP RINGBUFFER my_buffer").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::RingBuffer(drop) = result else {
			panic!("expected RingBuffer")
		};
		assert!(!drop.if_exists);
		assert_eq!(drop.ringbuffer.name.text(), "my_buffer");
		assert!(drop.ringbuffer.namespace.is_empty());
		assert!(!drop.cascade);
	}

	#[test]
	fn test_drop_ringbuffer_if_exists_qualified() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP RINGBUFFER IF EXISTS ns::my_buffer").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::RingBuffer(drop) = result else {
			panic!("expected RingBuffer")
		};
		assert!(drop.if_exists);
		assert_eq!(drop.ringbuffer.namespace[0].text(), "ns");
		assert_eq!(drop.ringbuffer.name.text(), "my_buffer");
	}

	#[test]
	fn test_drop_namespace_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP NAMESPACE analytics").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Namespace(drop) = result else {
			panic!("expected Namespace")
		};
		assert!(!drop.if_exists);
		assert_eq!(drop.namespace.segments.len(), 1);
		assert_eq!(drop.namespace.segments[0].text(), "analytics");
		assert!(!drop.cascade);
	}

	#[test]
	fn test_drop_namespace_if_exists_cascade() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "DROP NAMESPACE IF EXISTS analytics CASCADE").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Namespace(drop) = result else {
			panic!("expected Namespace")
		};
		assert!(drop.if_exists);
		assert_eq!(drop.namespace.segments[0].text(), "analytics");
		assert!(drop.cascade);
	}

	#[test]
	fn test_drop_dictionary_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP DICTIONARY my_dict").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Dictionary(drop) = result else {
			panic!("expected Dictionary")
		};
		assert!(!drop.if_exists);
		assert_eq!(drop.dictionary.name.text(), "my_dict");
		assert!(drop.dictionary.namespace.is_empty());
		assert!(!drop.cascade);
	}

	#[test]
	fn test_drop_dictionary_if_exists_qualified() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "DROP DICTIONARY IF EXISTS ns::my_dict CASCADE").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Dictionary(drop) = result else {
			panic!("expected Dictionary")
		};
		assert!(drop.if_exists);
		assert_eq!(drop.dictionary.namespace[0].text(), "ns");
		assert_eq!(drop.dictionary.name.text(), "my_dict");
		assert!(drop.cascade);
	}

	#[test]
	fn test_drop_enum_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP ENUM my_enum").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Enum(drop) = result else {
			panic!("expected Enum")
		};
		assert!(!drop.if_exists);
		assert_eq!(drop.sumtype.name.text(), "my_enum");
		assert!(drop.sumtype.namespace.is_empty());
		assert!(!drop.cascade);
	}

	#[test]
	fn test_drop_enum_if_exists_qualified() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP ENUM IF EXISTS ns::my_enum CASCADE").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Enum(drop) = result else {
			panic!("expected Enum")
		};
		assert!(drop.if_exists);
		assert_eq!(drop.sumtype.namespace[0].text(), "ns");
		assert_eq!(drop.sumtype.name.text(), "my_enum");
		assert!(drop.cascade);
	}

	#[test]
	fn test_drop_subscription_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP SUBSCRIPTION sub_123").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Subscription(drop) = result else {
			panic!("expected Subscription")
		};
		assert!(!drop.if_exists);
		assert_eq!(drop.identifier.text(), "sub_123");
		assert!(!drop.cascade);
	}

	#[test]
	fn test_drop_subscription_if_exists() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "DROP SUBSCRIPTION IF EXISTS sub_123").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Subscription(drop) = result else {
			panic!("expected Subscription")
		};
		assert!(drop.if_exists);
		assert_eq!(drop.identifier.text(), "sub_123");
		assert!(!drop.cascade);
	}
}
