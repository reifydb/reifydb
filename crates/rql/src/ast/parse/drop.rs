// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::{AstErrorKind, Error, TypeError};

use crate::{
	Result,
	ast::{
		ast::{
			AstDrop, AstDropDictionary, AstDropHandler, AstDropNamespace, AstDropProcedure,
			AstDropRingBuffer, AstDropSeries, AstDropSink, AstDropSource, AstDropSubscription,
			AstDropSumType, AstDropTable, AstDropTest, AstDropView, AstPolicyTargetType,
		},
		identifier::{
			MaybeQualifiedDictionaryIdentifier, MaybeQualifiedHandlerIdentifier,
			MaybeQualifiedNamespaceIdentifier, MaybeQualifiedProcedureIdentifier,
			MaybeQualifiedRingBufferIdentifier, MaybeQualifiedSeriesIdentifier,
			MaybeQualifiedSinkIdentifier, MaybeQualifiedSourceIdentifier, MaybeQualifiedSumTypeIdentifier,
			MaybeQualifiedTableIdentifier, MaybeQualifiedTestIdentifier, MaybeQualifiedViewIdentifier,
		},
		parse::Parser,
	},
	bump::BumpFragment,
	token::{
		keyword::Keyword,
		token::{Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_drop(&mut self) -> Result<AstDrop<'bump>> {
		let token = self.consume_keyword(Keyword::Drop)?;

		// Check what we're dropping
		if (self.consume_if(TokenKind::Keyword(Keyword::Table))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_drop_policy(token, AstPolicyTargetType::Table);
			}
			return self.parse_drop_table(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::View))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_drop_policy(token, AstPolicyTargetType::View);
			}
			return self.parse_drop_view(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Ringbuffer))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_drop_policy(token, AstPolicyTargetType::RingBuffer);
			}
			return self.parse_drop_ringbuffer(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Namespace))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_drop_policy(token, AstPolicyTargetType::Namespace);
			}
			return self.parse_drop_namespace(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Dictionary))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_drop_policy(token, AstPolicyTargetType::Dictionary);
			}
			return self.parse_drop_dictionary(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Enum))?).is_some() {
			return self.parse_drop_enum(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Subscription))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_drop_policy(token, AstPolicyTargetType::Subscription);
			}
			return self.parse_drop_subscription(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Series))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_drop_policy(token, AstPolicyTargetType::Series);
			}
			return self.parse_drop_series(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Authentication))?).is_some() {
			return self.parse_drop_authentication(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::User))?).is_some() {
			return self.parse_drop_identity(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Role))?).is_some() {
			return self.parse_drop_role(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Session))?).is_some() {
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_drop_policy(token, AstPolicyTargetType::Session);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Feature))?).is_some() {
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_drop_policy(token, AstPolicyTargetType::Feature);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Function))?).is_some() {
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_drop_policy(token, AstPolicyTargetType::Function);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Procedure))?).is_some() {
			// `DROP PROCEDURE POLICY ...` vs `DROP PROCEDURE [IF EXISTS] ns::name`
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_drop_policy(token, AstPolicyTargetType::Procedure);
			}
			return self.parse_drop_procedure(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Source))?).is_some() {
			return self.parse_drop_source(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Sink))?).is_some() {
			return self.parse_drop_sink(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Handler))?).is_some() {
			return self.parse_drop_handler(token);
		}
		if (self.consume_if(TokenKind::Keyword(Keyword::Test))?).is_some() {
			return self.parse_drop_test(token);
		}

		let fragment = self.current()?.fragment.to_owned();
		Err(Error::from(TypeError::Ast {
			kind: AstErrorKind::UnexpectedToken {
				expected: "AUTHENTICATION, TABLE, VIEW, RINGBUFFER, NAMESPACE, DICTIONARY, ENUM, SUBSCRIPTION, SERIES, SOURCE, SINK, HANDLER, or TEST"
					.to_string(),
			},
			message: format!(
				"Unexpected token: expected {}, got {}",
				"AUTHENTICATION, TABLE, VIEW, RINGBUFFER, NAMESPACE, DICTIONARY, ENUM, SUBSCRIPTION, SERIES, SOURCE, SINK, HANDLER, or TEST",
				fragment.text()
			),
			fragment,
		}))
	}

	/// Parse IF EXISTS clause, returning true if present.
	pub(crate) fn parse_if_exists(&mut self) -> Result<bool> {
		if (self.consume_if(TokenKind::Keyword(Keyword::If))?).is_some() {
			self.consume_keyword(Keyword::Exists)?;
			Ok(true)
		} else {
			Ok(false)
		}
	}

	/// Parse optional CASCADE or RESTRICT clause, defaulting to RESTRICT (false).
	fn parse_cascade(&mut self) -> Result<bool> {
		if (self.consume_if(TokenKind::Keyword(Keyword::Cascade))?).is_some() {
			Ok(true)
		} else {
			// Consume optional RESTRICT keyword (it's the default behavior)
			let _ = self.consume_if(TokenKind::Keyword(Keyword::Restrict))?;
			Ok(false)
		}
	}
	/// Parse a standard DROP entity: IF EXISTS, qualified identifier, CASCADE.
	/// Calls `make_identifier` with (name, namespace) and `wrap` to produce the AstDrop variant.
	fn parse_drop_qualified<I>(
		&mut self,
		token: Token<'bump>,
		make_identifier: impl FnOnce(BumpFragment<'bump>, Vec<BumpFragment<'bump>>) -> I,
		wrap: impl FnOnce(Token<'bump>, bool, I, bool) -> AstDrop<'bump>,
	) -> Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let identifier = make_identifier(name, namespace);
		let cascade = self.parse_cascade()?;
		Ok(wrap(token, if_exists, identifier, cascade))
	}

	fn parse_drop_table(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		self.parse_drop_qualified(
			token,
			|name, ns| {
				if ns.is_empty() {
					MaybeQualifiedTableIdentifier::new(name)
				} else {
					MaybeQualifiedTableIdentifier::new(name).with_namespace(ns)
				}
			},
			|token, if_exists, table, cascade| {
				AstDrop::Table(AstDropTable {
					token,
					if_exists,
					table,
					cascade,
				})
			},
		)
	}

	fn parse_drop_view(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		self.parse_drop_qualified(
			token,
			|name, ns| {
				if ns.is_empty() {
					MaybeQualifiedViewIdentifier::new(name)
				} else {
					MaybeQualifiedViewIdentifier::new(name).with_namespace(ns)
				}
			},
			|token, if_exists, view, cascade| {
				AstDrop::View(AstDropView {
					token,
					if_exists,
					view,
					cascade,
				})
			},
		)
	}

	fn parse_drop_ringbuffer(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		self.parse_drop_qualified(
			token,
			|name, ns| {
				if ns.is_empty() {
					MaybeQualifiedRingBufferIdentifier::new(name)
				} else {
					MaybeQualifiedRingBufferIdentifier::new(name).with_namespace(ns)
				}
			},
			|token, if_exists, ringbuffer, cascade| {
				AstDrop::RingBuffer(AstDropRingBuffer {
					token,
					if_exists,
					ringbuffer,
					cascade,
				})
			},
		)
	}

	fn parse_drop_namespace(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
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

	fn parse_drop_dictionary(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		self.parse_drop_qualified(
			token,
			|name, ns| {
				if ns.is_empty() {
					MaybeQualifiedDictionaryIdentifier::new(name)
				} else {
					MaybeQualifiedDictionaryIdentifier::new(name).with_namespace(ns)
				}
			},
			|token, if_exists, dictionary, cascade| {
				AstDrop::Dictionary(AstDropDictionary {
					token,
					if_exists,
					dictionary,
					cascade,
				})
			},
		)
	}

	fn parse_drop_enum(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		self.parse_drop_qualified(
			token,
			|name, ns| {
				if ns.is_empty() {
					MaybeQualifiedSumTypeIdentifier::new(name)
				} else {
					MaybeQualifiedSumTypeIdentifier::new(name).with_namespace(ns)
				}
			},
			|token, if_exists, sumtype, cascade| {
				AstDrop::Enum(AstDropSumType {
					token,
					if_exists,
					sumtype,
					cascade,
				})
			},
		)
	}

	fn parse_drop_series(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		self.parse_drop_qualified(
			token,
			|name, ns| {
				if ns.is_empty() {
					MaybeQualifiedSeriesIdentifier::new(name)
				} else {
					MaybeQualifiedSeriesIdentifier::new(name).with_namespace(ns)
				}
			},
			|token, if_exists, series, cascade| {
				AstDrop::Series(AstDropSeries {
					token,
					if_exists,
					series,
					cascade,
				})
			},
		)
	}

	fn parse_drop_subscription(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
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

	fn parse_drop_source(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		self.parse_drop_qualified(
			token,
			|name, ns| {
				if ns.is_empty() {
					MaybeQualifiedSourceIdentifier::new(name)
				} else {
					MaybeQualifiedSourceIdentifier::new(name).with_namespace(ns)
				}
			},
			|token, if_exists, source, cascade| {
				AstDrop::Source(AstDropSource {
					token,
					if_exists,
					source,
					cascade,
				})
			},
		)
	}

	fn parse_drop_procedure(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let procedure = if namespace.is_empty() {
			MaybeQualifiedProcedureIdentifier::new(name)
		} else {
			MaybeQualifiedProcedureIdentifier::new(name).with_namespace(namespace)
		};
		Ok(AstDrop::Procedure(AstDropProcedure {
			token,
			if_exists,
			procedure,
		}))
	}

	fn parse_drop_sink(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		self.parse_drop_qualified(
			token,
			|name, ns| {
				if ns.is_empty() {
					MaybeQualifiedSinkIdentifier::new(name)
				} else {
					MaybeQualifiedSinkIdentifier::new(name).with_namespace(ns)
				}
			},
			|token, if_exists, sink, cascade| {
				AstDrop::Sink(AstDropSink {
					token,
					if_exists,
					sink,
					cascade,
				})
			},
		)
	}

	fn parse_drop_handler(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let handler = if namespace.is_empty() {
			MaybeQualifiedHandlerIdentifier::new(name)
		} else {
			MaybeQualifiedHandlerIdentifier::new(name).with_namespace(namespace)
		};
		Ok(AstDrop::Handler(AstDropHandler {
			token,
			if_exists,
			handler,
		}))
	}

	fn parse_drop_test(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		let if_exists = self.parse_if_exists()?;
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let test = if namespace.is_empty() {
			MaybeQualifiedTestIdentifier::new(name)
		} else {
			MaybeQualifiedTestIdentifier::new(name).with_namespace(namespace)
		};
		Ok(AstDrop::Test(AstDropTest {
			token,
			if_exists,
			test,
		}))
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{ast::parse::Parser, bump::Bump, token::tokenize};

	#[test]
	fn test_drop_table_basic() {
		let bump = Bump::new();
		let source = "DROP TABLE users";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = "DROP TABLE IF EXISTS analytics::users CASCADE";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = "DROP VIEW my_view";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = "DROP VIEW IF EXISTS ns::my_view CASCADE";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = "DROP RINGBUFFER my_buffer";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = "DROP RINGBUFFER IF EXISTS ns::my_buffer";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = "DROP NAMESPACE analytics";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = "DROP NAMESPACE IF EXISTS analytics CASCADE";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = "DROP DICTIONARY my_dict";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = "DROP DICTIONARY IF EXISTS ns::my_dict CASCADE";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = "DROP ENUM my_enum";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = "DROP ENUM IF EXISTS ns::my_enum CASCADE";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = "DROP SUBSCRIPTION sub_123";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
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
		let source = "DROP SUBSCRIPTION IF EXISTS sub_123";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse_drop().unwrap();

		let AstDrop::Subscription(drop) = result else {
			panic!("expected Subscription")
		};
		assert!(drop.if_exists);
		assert_eq!(drop.identifier.text(), "sub_123");
		assert!(!drop.cascade);
	}
}
