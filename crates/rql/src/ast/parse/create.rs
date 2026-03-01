// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::sort::SortDirection;
use reifydb_type::{
	error::{AstErrorKind, Error, TypeError},
	fragment::Fragment,
};

use crate::{
	Result,
	ast::{
		ast::{
			AstColumnProperty, AstColumnPropertyEntry, AstColumnPropertyKind, AstColumnToCreate, AstCreate,
			AstCreateColumnProperty, AstCreateDeferredView, AstCreateDictionary, AstCreateEvent,
			AstCreateHandler, AstCreateMigration, AstCreateNamespace, AstCreatePrimaryKey,
			AstCreateProcedure, AstCreateRingBuffer, AstCreateSeries, AstCreateSubscription,
			AstCreateSumType, AstCreateTable, AstCreateTag, AstCreateTransactionalView, AstIndexColumn,
			AstPolicyTargetType, AstPrimaryKeyDef, AstProcedureParam, AstStatement, AstTimestampPrecision,
			AstType, AstVariantDef,
		},
		identifier::{
			MaybeQualifiedDeferredViewIdentifier, MaybeQualifiedDictionaryIdentifier,
			MaybeQualifiedNamespaceIdentifier, MaybeQualifiedProcedureIdentifier,
			MaybeQualifiedRingBufferIdentifier, MaybeQualifiedSeriesIdentifier,
			MaybeQualifiedSumTypeIdentifier, MaybeQualifiedTableIdentifier,
			MaybeQualifiedTransactionalViewIdentifier,
		},
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{
		keyword::{
			Keyword,
			Keyword::{
				Create, Deferred, Dictionary, Exists, For, If, Namespace, Replace, Ringbuffer, Series,
				Subscription, Table, Tag, Transactional, View,
			},
		},
		operator::{
			Operator,
			Operator::{Colon, Not, Or},
		},
		separator::{Separator, Separator::Comma},
		token::{Literal, Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_create(&mut self) -> Result<AstCreate<'bump>> {
		let token = self.consume_keyword(Create)?;

		// Check for CREATE OR REPLACE
		let or_replace = if (self.consume_if(TokenKind::Operator(Or))?).is_some() {
			self.consume_keyword(Replace)?;
			true
		} else {
			false
		};

		// CREATE OR REPLACE is only valid for FLOW currently
		if or_replace {
			let fragment = self.current()?.fragment.to_owned();
			return Err(Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "FLOW after CREATE OR REPLACE".to_string(),
				},
				message: format!(
					"Unexpected token: expected {}, got {}",
					"FLOW after CREATE OR REPLACE",
					fragment.text()
				),
				fragment,
			}));
		}

		if (self.consume_if(TokenKind::Keyword(Namespace))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_create_policy(token, AstPolicyTargetType::Namespace);
			}
			return self.parse_namespace(token);
		}

		// CREATE VIEW / CREATE VIEW POLICY
		if (self.consume_if(TokenKind::Keyword(View))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_create_policy(token, AstPolicyTargetType::View);
			}
			return self.parse_transactional_view(token);
		}

		if (self.consume_if(TokenKind::Keyword(Deferred))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(View))?).is_some() {
				return self.parse_deferred_view(token);
			}
			unimplemented!()
		}

		if (self.consume_if(TokenKind::Keyword(Transactional))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(View))?).is_some() {
				return self.parse_transactional_view(token);
			}
			unimplemented!()
		}

		if (self.consume_if(TokenKind::Keyword(Table))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_create_policy(token, AstPolicyTargetType::Table);
			}
			return self.parse_table(token);
		}

		if (self.consume_if(TokenKind::Keyword(Ringbuffer))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_create_policy(token, AstPolicyTargetType::RingBuffer);
			}
			return self.parse_ringbuffer(token);
		}

		if (self.consume_if(TokenKind::Keyword(Dictionary))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_create_policy(token, AstPolicyTargetType::Dictionary);
			}
			return self.parse_dictionary(token);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Enum))?).is_some() {
			return self.parse_enum(token);
		}

		if (self.consume_if(TokenKind::Keyword(Series))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_create_policy(token, AstPolicyTargetType::Series);
			}
			return self.parse_series(token);
		}

		if (self.consume_if(TokenKind::Keyword(Subscription))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_create_policy(token, AstPolicyTargetType::Subscription);
			}
			return self.parse_subscription(token);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Primary))?).is_some() {
			self.consume_keyword(Keyword::Key)?;
			return self.parse_create_primary_key(token);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Column))?).is_some() {
			self.consume_keyword(Keyword::Property)?;
			return self.parse_create_column_property(token);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Procedure))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_create_policy(token, AstPolicyTargetType::Procedure);
			}
			return self.parse_procedure(token);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Event))?).is_some() {
			return self.parse_event(token);
		}

		if (self.consume_if(TokenKind::Keyword(Tag))?).is_some() {
			return self.parse_tag(token);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Handler))?).is_some() {
			return self.parse_handler(token);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Authentication))?).is_some() {
			return self.parse_create_authentication(token);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::User))?).is_some() {
			return self.parse_create_user(token);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Role))?).is_some() {
			return self.parse_create_role(token);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Session))?).is_some() {
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_create_policy(token, AstPolicyTargetType::Session);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Feature))?).is_some() {
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_create_policy(token, AstPolicyTargetType::Feature);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Function))?).is_some() {
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_create_policy(token, AstPolicyTargetType::Function);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Migration))?).is_some() {
			return self.parse_migration(token);
		}

		if self.peek_is_index_creation()? {
			return self.parse_create_index(token);
		}

		unimplemented!();
	}

	fn parse_procedure(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		// Parse dot-separated name: ns.procedure_name
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();

		let proc_ident = MaybeQualifiedProcedureIdentifier::new(name).with_namespace(namespace);

		// Parse optional parameter block: { param: Type, ... }
		let params = if self.current()?.is_operator(Operator::OpenCurly) {
			self.parse_procedure_params()?
		} else {
			Vec::new()
		};

		// Consume AS keyword
		self.consume_operator(Operator::As)?;

		// Parse body block: { statements... }
		self.consume_operator(Operator::OpenCurly)?;

		// Track token position for body source reconstruction
		let body_start_pos = self.position;

		let mut body = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.is_eof() || self.current()?.kind == TokenKind::Operator(Operator::CloseCurly) {
				break;
			}

			let node = self.parse_node(Precedence::None)?;
			body.push(node);

			// Try to consume separator
			self.consume_if(TokenKind::Separator(Separator::NewLine))?;
			self.consume_if(TokenKind::Separator(Separator::Semicolon))?;
		}

		// Capture body source by slicing the original source between { and }
		let body_end_pos = self.position;
		let body_source = if body_start_pos < body_end_pos {
			let start = self.tokens[body_start_pos].fragment.offset();
			let end = self.tokens[body_end_pos - 1].fragment.offset()
				+ self.tokens[body_end_pos - 1].fragment.text().len();
			self.source[start..end].trim().to_string()
		} else {
			String::new()
		};

		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstCreate::Procedure(AstCreateProcedure {
			token,
			name: proc_ident,
			params,
			body,
			body_source,
		}))
	}

	fn parse_procedure_params(&mut self) -> Result<Vec<AstProcedureParam<'bump>>> {
		let mut params = Vec::new();
		self.consume_operator(Operator::OpenCurly)?;

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let name = self.parse_identifier_with_hyphens()?.into_fragment();
			self.consume_operator(Colon)?;
			let param_type = self.parse_type()?;

			params.push(AstProcedureParam {
				name,
				param_type,
			});

			self.skip_new_line()?;

			if self.consume_if(TokenKind::Separator(Comma))?.is_some() {
				continue;
			}

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;
		Ok(params)
	}

	fn parse_namespace(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		// Check for IF NOT EXISTS BEFORE identifier
		let mut if_not_exists = if (self.consume_if(TokenKind::Keyword(If))?).is_some() {
			self.consume_operator(Not)?;
			self.consume_keyword(Exists)?;
			true
		} else {
			false
		};

		let segments = self.parse_double_colon_separated_identifiers()?;

		// Check for IF NOT EXISTS AFTER identifier (alternate syntax)
		if !if_not_exists && (self.consume_if(TokenKind::Keyword(If))?).is_some() {
			self.consume_operator(Not)?;
			self.consume_keyword(Exists)?;
			if_not_exists = true;
		}

		let namespace = MaybeQualifiedNamespaceIdentifier::new(
			segments.into_iter().map(|s| s.into_fragment()).collect(),
		);
		Ok(AstCreate::Namespace(AstCreateNamespace {
			token,
			namespace,
			if_not_exists,
		}))
	}

	fn parse_series(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let columns = self.parse_columns()?;

		let series = MaybeQualifiedSeriesIdentifier::new(name).with_namespace(namespace);

		// Parse optional WITH block
		let mut tag = None;
		let mut precision = None;

		if self.consume_if(TokenKind::Keyword(Keyword::With))?.is_some() {
			self.consume_operator(Operator::OpenCurly)?;

			loop {
				self.skip_new_line()?;

				if self.current()?.is_operator(Operator::CloseCurly) {
					break;
				}

				let key = {
					let current = self.current()?;
					match current.kind {
						TokenKind::Identifier => self.consume(TokenKind::Identifier)?,
						TokenKind::Keyword(Keyword::Tag) => {
							let token = self.advance()?;
							Token {
								kind: TokenKind::Identifier,
								..token
							}
						}
						_ => {
							return Err(Error::from(TypeError::Ast {
								kind: AstErrorKind::UnexpectedToken {
									expected: "'tag' or 'precision'".to_string(),
								},
								message: format!(
									"expected 'tag' or 'precision', found `{}`",
									current.fragment.text()
								),
								fragment: current.fragment.to_owned(),
							}));
						}
					}
				};
				self.consume_operator(Operator::Colon)?;

				match key.fragment.text() {
					"tag" => {
						let mut tag_segments =
							self.parse_double_colon_separated_identifiers()?;
						let tag_name = tag_segments.pop().unwrap().into_fragment();
						let tag_namespace: Vec<_> =
							tag_segments.into_iter().map(|s| s.into_fragment()).collect();
						tag = Some(MaybeQualifiedSumTypeIdentifier::new(tag_name)
							.with_namespace(tag_namespace));
					}
					"precision" => {
						let prec_token = self.consume(TokenKind::Identifier)?;
						precision = Some(match prec_token.fragment.text() {
							"millisecond" => AstTimestampPrecision::Millisecond,
							"microsecond" => AstTimestampPrecision::Microsecond,
							"nanosecond" => AstTimestampPrecision::Nanosecond,
							_ => {
								let fragment = prec_token.fragment.to_owned();
								return Err(Error::from(TypeError::Ast {
									kind: AstErrorKind::UnexpectedToken {
										expected: "'millisecond', 'microsecond', or 'nanosecond'"
											.to_string(),
									},
									message: format!(
										"Unexpected token: expected {}, got {}",
										"'millisecond', 'microsecond', or 'nanosecond'",
										fragment.text()
									),
									fragment,
								}));
							}
						});
					}
					_other => {
						let fragment = key.fragment.to_owned();
						return Err(Error::from(TypeError::Ast {
							kind: AstErrorKind::UnexpectedToken {
								expected: "'tag' or 'precision'".to_string(),
							},
							message: format!(
								"Unexpected token: expected {}, got {}",
								"'tag' or 'precision'",
								fragment.text()
							),
							fragment,
						}));
					}
				}

				self.skip_new_line()?;

				if self.consume_if(TokenKind::Separator(Comma))?.is_some() {
					continue;
				}

				if self.current()?.is_operator(Operator::CloseCurly) {
					break;
				}
			}

			self.consume_operator(Operator::CloseCurly)?;
		}

		Ok(AstCreate::Series(AstCreateSeries {
			token,
			series,
			columns,
			tag,
			precision,
		}))
	}

	fn parse_subscription(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		// Subscriptions don't have names - they're identified only by UUID v7
		// Syntax: CREATE SUBSCRIPTION { columns... } AS { query }
		// Or schema-less: CREATE SUBSCRIPTION AS { query }

		// Check if we have columns or go straight to AS
		let columns = if self.current()?.is_operator(Operator::As) {
			// Schema-less: no columns, will be inferred from query
			Vec::new()
		} else if self.current()?.is_operator(Operator::OpenCurly) {
			// Has column definitions
			self.parse_columns()?
		} else {
			let fragment = self.current()?.fragment.to_owned();
			return Err(Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "'{' or 'AS'".to_string(),
				},
				message: format!(
					"Unexpected token: expected {}, got {}",
					"'{' or 'AS'",
					fragment.text()
				),
				fragment,
			}));
		};

		// Parse optional AS clause
		let as_clause = if self.consume_if(TokenKind::Operator(Operator::As))?.is_some() {
			// Expect opening curly brace
			self.consume_operator(Operator::OpenCurly)?;

			// Parse the query nodes inside the AS clause
			let mut query_nodes = Vec::new();
			let mut has_pipes = false;

			// Parse statements until we hit the closing brace
			loop {
				if self.is_eof() || self.current()?.kind == TokenKind::Operator(Operator::CloseCurly) {
					break;
				}

				let node = self.parse_node(Precedence::None)?;
				query_nodes.push(node);

				// Check for pipe operator or newline as separator between nodes
				if !self.is_eof() && self.current()?.is_operator(Operator::Pipe) {
					self.advance()?; // consume the pipe
					has_pipes = true;
				} else {
					// Try to consume a newline if present (optional)
					self.consume_if(TokenKind::Separator(Separator::NewLine))?;
				}
			}

			// Expect closing curly brace
			self.consume_operator(Operator::CloseCurly)?;

			Some(AstStatement {
				nodes: query_nodes,
				has_pipes,
				is_output: false,
			})
		} else {
			None
		};

		// Validation: schema-less subscriptions require AS clause
		if columns.is_empty() && as_clause.is_none() {
			let fragment = self
				.current()
				.ok()
				.map(|t| t.fragment.to_owned())
				.unwrap_or_else(|| Fragment::internal("end of input"));
			return Err(Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "AS clause (schema-less CREATE SUBSCRIPTION requires AS clause)"
						.to_string(),
				},
				message: format!(
					"Unexpected token: expected {}, got {}",
					"AS clause (schema-less CREATE SUBSCRIPTION requires AS clause)",
					fragment.text()
				),
				fragment,
			}));
		}

		Ok(AstCreate::Subscription(AstCreateSubscription {
			token,
			columns,
			as_clause,
		}))
	}

	fn parse_deferred_view(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let columns = self.parse_columns()?;

		let view = MaybeQualifiedDeferredViewIdentifier::new(name).with_namespace(namespace);

		// Parse optional AS clause
		let as_clause = if self.consume_if(TokenKind::Operator(Operator::As))?.is_some() {
			// Expect opening curly brace
			self.consume_operator(Operator::OpenCurly)?;

			// Parse the query nodes inside the AS clause
			let mut query_nodes = Vec::new();
			let mut has_pipes = false;

			// Parse statements until we hit the closing brace
			loop {
				if self.is_eof() || self.current()?.kind == TokenKind::Operator(Operator::CloseCurly) {
					break;
				}

				let node = self.parse_node(Precedence::None)?;
				query_nodes.push(node);

				// Check for pipe operator or newline as separator between nodes
				if !self.is_eof() && self.current()?.is_operator(Operator::Pipe) {
					self.advance()?; // consume the pipe
					has_pipes = true;
				} else {
					// Try to consume a newline if present (optional)
					self.consume_if(TokenKind::Separator(Separator::NewLine))?;
				}
			}

			// Expect closing curly brace
			self.consume_operator(Operator::CloseCurly)?;

			Some(AstStatement {
				nodes: query_nodes,
				has_pipes,
				is_output: false,
			})
		} else {
			None
		};

		Ok(AstCreate::DeferredView(AstCreateDeferredView {
			token,
			view,
			columns,
			as_clause,
		}))
	}

	fn parse_transactional_view(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let columns = self.parse_columns()?;

		let view = MaybeQualifiedTransactionalViewIdentifier::new(name).with_namespace(namespace);

		// Parse optional AS clause
		let as_clause = if self.consume_if(TokenKind::Operator(Operator::As))?.is_some() {
			// Expect opening curly brace
			self.consume_operator(Operator::OpenCurly)?;

			// Parse the query nodes inside the AS clause
			let mut query_nodes = Vec::new();
			let mut has_pipes = false;

			// Parse statements until we hit the closing brace
			loop {
				if self.is_eof() || self.current()?.kind == TokenKind::Operator(Operator::CloseCurly) {
					break;
				}

				let node = self.parse_node(Precedence::None)?;
				query_nodes.push(node);

				// Check for pipe operator or newline as separator between nodes
				if !self.is_eof() && self.current()?.is_operator(Operator::Pipe) {
					self.advance()?; // consume the pipe
					has_pipes = true;
				} else {
					// Try to consume a newline if present (optional)
					self.consume_if(TokenKind::Separator(Separator::NewLine))?;
				}
			}

			// Expect closing curly brace
			self.consume_operator(Operator::CloseCurly)?;

			Some(AstStatement {
				nodes: query_nodes,
				has_pipes,
				is_output: false,
			})
		} else {
			None
		};

		Ok(AstCreate::TransactionalView(AstCreateTransactionalView {
			token,
			view,
			columns,
			as_clause,
		}))
	}

	fn parse_table(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let columns = self.parse_columns()?;

		let table = MaybeQualifiedTableIdentifier::new(name).with_namespace(namespace);

		Ok(AstCreate::Table(AstCreateTable {
			token,
			table,
			columns,
		}))
	}

	fn parse_ringbuffer(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let columns = self.parse_columns()?;

		// Parse WITH block (required for ringbuffer - must have capacity)
		self.consume_keyword(Keyword::With)?;
		self.consume_operator(Operator::OpenCurly)?;

		let mut capacity: Option<u64> = None;

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let key = {
				let current = self.current()?;
				match current.kind {
					TokenKind::Identifier => self.consume(TokenKind::Identifier)?,
					TokenKind::Keyword(Keyword::Tag) => {
						let token = self.advance()?;
						Token {
							kind: TokenKind::Identifier,
							..token
						}
					}
					_ => {
						return Err(Error::from(TypeError::Ast {
							kind: AstErrorKind::UnexpectedToken {
								expected: "'tag' or 'precision'".to_string(),
							},
							message: format!(
								"expected 'tag' or 'precision', found `{}`",
								current.fragment.text()
							),
							fragment: current.fragment.to_owned(),
						}));
					}
				}
			};
			self.consume_operator(Operator::Colon)?;

			match key.fragment.text() {
				"capacity" => {
					let capacity_token = self.consume(TokenKind::Literal(Literal::Number))?;
					capacity =
						Some(capacity_token.fragment.text().parse::<u64>().map_err(|_| {
							let fragment = capacity_token.fragment.to_owned();
							Error::from(TypeError::Ast {
								kind: AstErrorKind::UnexpectedToken {
									expected: "valid capacity number".to_string(),
								},
								message: format!(
									"Unexpected token: expected {}, got {}",
									"valid capacity number",
									fragment.text()
								),
								fragment,
							})
						})?);
				}
				_other => {
					let fragment = key.fragment.to_owned();
					return Err(Error::from(TypeError::Ast {
						kind: AstErrorKind::UnexpectedToken {
							expected: "'capacity'".to_string(),
						},
						message: format!(
							"Unexpected token: expected {}, got {}",
							"'capacity'",
							fragment.text()
						),
						fragment,
					}));
				}
			}

			self.skip_new_line()?;

			if self.consume_if(TokenKind::Separator(Comma))?.is_some() {
				continue;
			}

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		let capacity = capacity.ok_or_else(|| {
			let fragment = self
				.current()
				.ok()
				.map(|t| t.fragment.to_owned())
				.unwrap_or_else(|| Fragment::internal("end of input"));
			Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "'capacity' is required for RINGBUFFER".to_string(),
				},
				message: format!(
					"Unexpected token: expected {}, got {}",
					"'capacity' is required for RINGBUFFER",
					fragment.text()
				),
				fragment,
			})
		})?;

		let ringbuffer = MaybeQualifiedRingBufferIdentifier::new(name).with_namespace(namespace);

		Ok(AstCreate::RingBuffer(AstCreateRingBuffer {
			token,
			ringbuffer,
			columns,
			capacity,
		}))
	}

	/// Parse primary key definition: {col1: DESC, col2: ASC}
	/// Defaults to DESC when sort order is not specified
	fn parse_primary_key_definition(&mut self) -> Result<AstPrimaryKeyDef<'bump>> {
		let mut columns = Vec::new();

		self.consume_operator(Operator::OpenCurly)?;

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let column = self.parse_column_identifier()?;

			// Check for optional sort direction (default is DESC)
			let sort_direction = if self.current()?.is_operator(Operator::Colon) {
				self.consume_operator(Operator::Colon)?;

				if self.current()?.is_keyword(Keyword::Asc) {
					self.consume_keyword(Keyword::Asc)?;
					SortDirection::Asc
				} else if self.current()?.is_keyword(Keyword::Desc) {
					self.consume_keyword(Keyword::Desc)?;
					SortDirection::Desc
				} else {
					// If colon present but invalid keyword, default to DESC
					SortDirection::Desc
				}
			} else {
				// No colon, default to DESC
				SortDirection::Desc
			};

			columns.push(AstIndexColumn {
				column,
				order: Some(sort_direction),
			});

			self.skip_new_line()?;

			if self.consume_if(TokenKind::Separator(Separator::Comma))?.is_some() {
				continue;
			}

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		if columns.is_empty() {
			let fragment = self.current()?.fragment.to_owned();
			return Err(Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "at least one column in primary key".to_string(),
				},
				message: format!(
					"Unexpected token: expected {}, got {}",
					"at least one column in primary key",
					fragment.text()
				),
				fragment,
			}));
		}

		Ok(AstPrimaryKeyDef {
			columns,
		})
	}

	/// Parse CREATE PRIMARY KEY ON ns.table { col1, col2: desc }
	fn parse_create_primary_key(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		self.consume_keyword(Keyword::On)?;

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let table = MaybeQualifiedTableIdentifier::new(name).with_namespace(namespace);

		let pk_def = self.parse_primary_key_definition()?;

		Ok(AstCreate::PrimaryKey(AstCreatePrimaryKey {
			token,
			table,
			columns: pk_def.columns,
		}))
	}

	/// Parse CREATE COLUMN POLICY ON ns.table.column { saturation: error, default: 0 }
	fn parse_create_column_property(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		self.consume_keyword(Keyword::On)?;

		let column = self.parse_column_identifier()?;

		self.consume_operator(Operator::OpenCurly)?;

		let mut properties = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			// Parse property kind
			let kind_token = self.consume(TokenKind::Identifier)?;
			let kind = match kind_token.fragment.text() {
				"saturation" => AstColumnPropertyKind::Saturation,
				"default" => AstColumnPropertyKind::Default,
				_ => {
					let fragment = kind_token.fragment.to_owned();
					return Err(Error::from(TypeError::Ast {
						kind: AstErrorKind::InvalidPolicy,
						message: format!("Invalid property token: {}", fragment.text()),
						fragment,
					}));
				}
			};

			// Consume colon separator
			self.consume_operator(Operator::Colon)?;

			// Parse property value
			let value = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());

			properties.push(AstColumnPropertyEntry {
				kind,
				value,
			});

			self.skip_new_line()?;

			if self.consume_if(TokenKind::Separator(Comma))?.is_some() {
				continue;
			}

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstCreate::ColumnProperty(AstCreateColumnProperty {
			token,
			column,
			properties,
		}))
	}

	fn parse_dictionary(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		// Check for IF NOT EXISTS
		let if_not_exists = if (self.consume_if(TokenKind::Keyword(If))?).is_some() {
			self.consume_operator(Not)?;
			self.consume_keyword(Exists)?;
			true
		} else {
			false
		};

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let dictionary = if namespace.is_empty() {
			MaybeQualifiedDictionaryIdentifier::new(name)
		} else {
			MaybeQualifiedDictionaryIdentifier::new(name).with_namespace(namespace)
		};

		// Parse FOR <value_type>
		self.consume_keyword(For)?;
		let value_type = self.parse_type()?;

		// Parse AS <id_type>
		self.consume_operator(Operator::As)?;
		let id_type = self.parse_type()?;

		Ok(AstCreate::Dictionary(AstCreateDictionary {
			token,
			if_not_exists,
			dictionary,
			value_type,
			id_type,
		}))
	}

	fn parse_enum(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		let if_not_exists = if (self.consume_if(TokenKind::Keyword(If))?).is_some() {
			self.consume_operator(Not)?;
			self.consume_keyword(Exists)?;
			true
		} else {
			false
		};

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name_frag = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let sumtype_ident = if namespace.is_empty() {
			MaybeQualifiedSumTypeIdentifier::new(name_frag)
		} else {
			MaybeQualifiedSumTypeIdentifier::new(name_frag).with_namespace(namespace)
		};

		self.consume_operator(Operator::OpenCurly)?;
		let mut variants = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let variant_name = self.parse_identifier_with_hyphens()?.into_fragment();

			let columns = if !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly) {
				self.parse_columns()?
			} else {
				Vec::new()
			};

			variants.push(AstVariantDef {
				name: variant_name,
				columns,
			});

			self.skip_new_line()?;
			if self.consume_if(TokenKind::Separator(Comma))?.is_none() {
				self.skip_new_line()?;
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstCreate::Enum(AstCreateSumType {
			token,
			if_not_exists,
			name: sumtype_ident,
			variants,
		}))
	}

	fn parse_type(&mut self) -> Result<AstType<'bump>> {
		let ty_token = self.consume(TokenKind::Identifier)?;

		// Check for Option(T) syntax
		if ty_token.fragment.text().eq_ignore_ascii_case("option") {
			self.consume_operator(Operator::OpenParen)?;
			let inner = self.parse_type()?;
			self.consume_operator(Operator::CloseParen)?;
			return Ok(AstType::Optional(Box::new(inner)));
		}

		if !self.is_eof() && self.current()?.is_operator(Operator::DoubleColon) {
			self.consume_operator(Operator::DoubleColon)?;
			let name_token = self.consume(TokenKind::Identifier)?;
			return Ok(AstType::Qualified {
				namespace: ty_token.fragment,
				name: name_token.fragment,
			});
		}

		// Check for type with parameters like DECIMAL(10,2)
		if !self.is_eof() && self.current()?.is_operator(Operator::OpenParen) {
			self.consume_operator(Operator::OpenParen)?;
			let mut params = Vec::new();

			// Parse first parameter
			params.push(self.parse_literal_number()?);

			// Parse additional parameters if comma-separated
			while self.consume_if(TokenKind::Separator(Comma))?.is_some() {
				params.push(self.parse_literal_number()?);
			}

			self.consume_operator(Operator::CloseParen)?;

			Ok(AstType::Constrained {
				name: ty_token.fragment,
				params,
			})
		} else {
			Ok(AstType::Unconstrained(ty_token.fragment))
		}
	}

	fn parse_columns(&mut self) -> Result<Vec<AstColumnToCreate<'bump>>> {
		let mut result = Vec::new();

		self.consume_operator(Operator::OpenCurly)?;
		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}
			result.push(self.parse_column()?);

			self.skip_new_line()?;
			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			if self.consume_if(TokenKind::Separator(Comma))?.is_none() {
				break;
			};
		}
		self.consume_operator(Operator::CloseCurly)?;
		Ok(result)
	}

	pub(crate) fn parse_column(&mut self) -> Result<AstColumnToCreate<'bump>> {
		let name_identifier = self.parse_identifier_with_hyphens()?;
		self.consume_operator(Colon)?;
		let ty_token = self.consume(TokenKind::Identifier)?;

		let name = name_identifier.into_fragment();

		// Parse type with optional parameters
		let ty = if ty_token.fragment.text().eq_ignore_ascii_case("option") {
			// Option(T) syntax
			self.consume_operator(Operator::OpenParen)?;
			let inner = self.parse_type()?;
			self.consume_operator(Operator::CloseParen)?;
			AstType::Optional(Box::new(inner))
		} else if !self.is_eof() && self.current()?.is_operator(Operator::DoubleColon) {
			self.consume_operator(Operator::DoubleColon)?;
			let name_token = self.consume(TokenKind::Identifier)?;
			AstType::Qualified {
				namespace: ty_token.fragment,
				name: name_token.fragment,
			}
		} else if !self.is_eof() && self.current()?.is_operator(Operator::OpenParen) {
			// Type with parameters like UTF8(50) or DECIMAL(10,2)
			self.consume_operator(Operator::OpenParen)?;
			let mut params = Vec::new();

			// Parse first parameter - for type constraints we
			// expect numbers
			params.push(self.parse_literal_number()?);

			// Parse additional parameters if comma-separated
			while self.consume_if(TokenKind::Separator(Comma))?.is_some() {
				params.push(self.parse_literal_number()?);
			}

			self.consume_operator(Operator::CloseParen)?;

			AstType::Constrained {
				name: ty_token.fragment,
				params,
			}
		} else {
			// Simple type without parameters
			AstType::Unconstrained(ty_token.fragment)
		};

		let properties = if !self.is_eof() && self.current()?.is_keyword(Keyword::With) {
			self.parse_column_properties()?
		} else {
			vec![]
		};

		Ok(AstColumnToCreate {
			name,
			ty,
			properties,
		})
	}

	fn parse_column_properties(&mut self) -> Result<Vec<AstColumnProperty<'bump>>> {
		self.consume_keyword(Keyword::With)?;
		self.consume_operator(Operator::OpenCurly)?;

		let mut properties = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			// Property key can be an Identifier or the Dictionary keyword
			let key_token = {
				let current = self.current()?;
				match current.kind {
					TokenKind::Identifier => self.consume(TokenKind::Identifier)?,
					TokenKind::Keyword(Keyword::Dictionary) => {
						let token = self.advance()?;
						Token {
							kind: TokenKind::Identifier,
							..token
						}
					}
					_ => {
						let fragment = current.fragment.to_owned();
						return Err(Error::from(TypeError::Ast {
							kind: AstErrorKind::InvalidColumnProperty,
							message: format!(
								"Invalid column property: {}",
								fragment.text()
							),
							fragment,
						}));
					}
				}
			};

			let key = key_token.fragment.text();

			let property = match key {
				"auto_increment" => AstColumnProperty::AutoIncrement,
				"dictionary" => {
					self.consume_operator(Colon)?;
					let mut segments = self.parse_double_colon_separated_identifiers()?;
					let name = segments.pop().unwrap().into_fragment();
					let namespace: Vec<_> =
						segments.into_iter().map(|s| s.into_fragment()).collect();
					let dict_ident = if namespace.is_empty() {
						MaybeQualifiedDictionaryIdentifier::new(name)
					} else {
						MaybeQualifiedDictionaryIdentifier::new(name).with_namespace(namespace)
					};
					AstColumnProperty::Dictionary(dict_ident)
				}
				"saturation" => {
					self.consume_operator(Colon)?;
					let value = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());
					AstColumnProperty::Saturation(value)
				}
				"default" => {
					self.consume_operator(Colon)?;
					let value = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());
					AstColumnProperty::Default(value)
				}
				_ => {
					let fragment = key_token.fragment.to_owned();
					return Err(Error::from(TypeError::Ast {
						kind: AstErrorKind::InvalidColumnProperty,
						message: format!("Invalid column property: {}", fragment.text()),
						fragment,
					}));
				}
			};

			properties.push(property);

			self.skip_new_line()?;

			if self.consume_if(TokenKind::Separator(Comma))?.is_some() {
				continue;
			}

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(properties)
	}

	pub(crate) fn parse_event(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name_frag = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let sumtype_ident = if namespace.is_empty() {
			MaybeQualifiedSumTypeIdentifier::new(name_frag)
		} else {
			MaybeQualifiedSumTypeIdentifier::new(name_frag).with_namespace(namespace)
		};

		self.consume_operator(Operator::OpenCurly)?;
		let mut variants = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let variant_name = self.parse_identifier_with_hyphens()?.into_fragment();

			let columns = if !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly) {
				self.parse_columns()?
			} else {
				Vec::new()
			};

			variants.push(AstVariantDef {
				name: variant_name,
				columns,
			});

			self.skip_new_line()?;
			if self.consume_if(TokenKind::Separator(Comma))?.is_none() {
				self.skip_new_line()?;
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstCreate::Event(AstCreateEvent {
			token,
			name: sumtype_ident,
			variants,
		}))
	}

	pub(crate) fn parse_tag(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name_frag = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let sumtype_ident = if namespace.is_empty() {
			MaybeQualifiedSumTypeIdentifier::new(name_frag)
		} else {
			MaybeQualifiedSumTypeIdentifier::new(name_frag).with_namespace(namespace)
		};

		self.consume_operator(Operator::OpenCurly)?;
		let mut variants = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let variant_name = self.parse_identifier_with_hyphens()?.into_fragment();

			let columns = if !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly) {
				self.parse_columns()?
			} else {
				Vec::new()
			};

			variants.push(AstVariantDef {
				name: variant_name,
				columns,
			});

			self.skip_new_line()?;
			if self.consume_if(TokenKind::Separator(Comma))?.is_none() {
				self.skip_new_line()?;
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstCreate::Tag(AstCreateTag {
			token,
			name: sumtype_ident,
			variants,
		}))
	}

	pub(crate) fn parse_handler(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		// Parse handler name: ns.handler_name
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name_frag = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let handler_name = MaybeQualifiedTableIdentifier::new(name_frag).with_namespace(namespace);

		// ON event_type::VariantName AS alias
		self.consume_keyword(Keyword::On)?;

		// Parse event_namespace::event_type::variant as a single chain of :: separated identifiers
		let mut event_segments = self.parse_double_colon_separated_identifiers()?;
		// Last segment is the variant name
		let on_variant = event_segments.pop().unwrap().into_fragment();
		// Second-to-last is the event type name
		let event_name_frag = event_segments.pop().unwrap().into_fragment();
		// Remaining segments are the namespace
		let event_namespace: Vec<_> = event_segments.into_iter().map(|s| s.into_fragment()).collect();
		let on_event = if event_namespace.is_empty() {
			MaybeQualifiedSumTypeIdentifier::new(event_name_frag)
		} else {
			MaybeQualifiedSumTypeIdentifier::new(event_name_frag).with_namespace(event_namespace)
		};

		// Body: { statements... }
		self.consume_operator(Operator::OpenCurly)?;

		let body_start_pos = self.position;
		let mut body = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.is_eof() || self.current()?.kind == TokenKind::Operator(Operator::CloseCurly) {
				break;
			}

			let node = self.parse_node(Precedence::None)?;
			body.push(node);

			self.consume_if(TokenKind::Separator(Separator::NewLine))?;
			self.consume_if(TokenKind::Separator(Separator::Semicolon))?;
		}

		let body_end_pos = self.position;
		let body_source = if body_start_pos < body_end_pos {
			let start = self.tokens[body_start_pos].fragment.offset();
			let end = self.tokens[body_end_pos - 1].fragment.offset()
				+ self.tokens[body_end_pos - 1].fragment.text().len();
			self.source[start..end].trim().to_string()
		} else {
			String::new()
		};

		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstCreate::Handler(AstCreateHandler {
			token,
			name: handler_name,
			on_event,
			on_variant,
			body,
			body_source,
		}))
	}

	fn parse_migration(&mut self, token: Token<'bump>) -> Result<AstCreate<'bump>> {
		// Parse migration name as a string literal: CREATE MIGRATION 'name'
		let name = match &self.current()?.kind {
			TokenKind::Literal(Literal::Text) => {
				let text = self.current()?.fragment.text().to_string();
				self.advance()?;
				text
			}
			_ => {
				let fragment = self.current()?.fragment.to_owned();
				return Err(Error::from(TypeError::Ast {
					kind: AstErrorKind::UnexpectedToken {
						expected: "migration name as string literal".to_string(),
					},
					message: format!(
						"Expected migration name as string literal, got {}",
						fragment.text()
					),
					fragment,
				}));
			}
		};

		// Parse body: { statements... }
		self.consume_operator(Operator::OpenCurly)?;
		let body_start_pos = self.position;

		// Skip over body tokens, counting brace depth
		let mut depth = 1u32;
		while depth > 0 {
			if self.is_eof() {
				let fragment = self.tokens[body_start_pos].fragment.to_owned();
				return Err(Error::from(TypeError::Ast {
					kind: AstErrorKind::UnexpectedToken {
						expected: "closing '}'".to_string(),
					},
					message: "Unexpected end of input while parsing migration body".to_string(),
					fragment,
				}));
			}
			match self.current()?.kind {
				TokenKind::Operator(Operator::OpenCurly) => {
					depth += 1;
					self.advance()?;
				}
				TokenKind::Operator(Operator::CloseCurly) => {
					depth -= 1;
					if depth > 0 {
						self.advance()?;
					}
				}
				_ => {
					self.advance()?;
				}
			}
		}

		let body_end_pos = self.position;
		let body_source = if body_start_pos < body_end_pos {
			let start = self.tokens[body_start_pos].fragment.offset();
			let end = self.tokens[body_end_pos - 1].fragment.offset()
				+ self.tokens[body_end_pos - 1].fragment.text().len();
			self.source[start..end].trim().to_string()
		} else {
			String::new()
		};

		self.consume_operator(Operator::CloseCurly)?;

		// Parse optional ROLLBACK { ... }
		let rollback_body_source = if (self.consume_if(TokenKind::Keyword(Keyword::Rollback))?).is_some() {
			self.consume_operator(Operator::OpenCurly)?;
			let rb_start_pos = self.position;

			let mut depth = 1u32;
			while depth > 0 {
				if self.is_eof() {
					let fragment = self.tokens[rb_start_pos].fragment.to_owned();
					return Err(Error::from(TypeError::Ast {
						kind: AstErrorKind::UnexpectedToken {
							expected: "closing '}'".to_string(),
						},
						message: "Unexpected end of input while parsing rollback body"
							.to_string(),
						fragment,
					}));
				}
				match self.current()?.kind {
					TokenKind::Operator(Operator::OpenCurly) => {
						depth += 1;
						self.advance()?;
					}
					TokenKind::Operator(Operator::CloseCurly) => {
						depth -= 1;
						if depth > 0 {
							self.advance()?;
						}
					}
					_ => {
						self.advance()?;
					}
				}
			}

			let rb_end_pos = self.position;
			let rb_source = if rb_start_pos < rb_end_pos {
				let start = self.tokens[rb_start_pos].fragment.offset();
				let end = self.tokens[rb_end_pos - 1].fragment.offset()
					+ self.tokens[rb_end_pos - 1].fragment.text().len();
				self.source[start..end].trim().to_string()
			} else {
				String::new()
			};

			self.consume_operator(Operator::CloseCurly)?;
			Some(rb_source)
		} else {
			None
		};

		Ok(AstCreate::Migration(AstCreateMigration {
			token,
			name,
			body_source,
			rollback_body_source,
		}))
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{
				Ast, AstColumnProperty, AstCreate, AstCreateDeferredView, AstCreateDictionary,
				AstCreateNamespace, AstCreateRingBuffer, AstCreateSeries, AstCreateSubscription,
				AstCreateSumType, AstCreateTable, AstCreateTransactionalView, AstType,
			},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_create_namespace() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE NAMESPACE REIFYDB").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Namespace(AstCreateNamespace {
				namespace,
				if_not_exists,
				..
			}) => {
				assert_eq!(namespace.segments[0].text(), "REIFYDB");
				assert!(!if_not_exists);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_namespace_with_hyphen() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE NAMESPACE my-namespace").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Namespace(AstCreateNamespace {
				namespace,
				if_not_exists,
				..
			}) => {
				assert_eq!(namespace.segments[0].text(), "my-namespace");
				assert!(!if_not_exists);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_namespace_if_not_exists() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "CREATE NAMESPACE IF NOT EXISTS my_namespace").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Namespace(AstCreateNamespace {
				namespace,
				if_not_exists,
				..
			}) => {
				assert_eq!(namespace.segments[0].text(), "my_namespace");
				assert!(if_not_exists);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_namespace_if_not_exists_with_hyphen() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE NAMESPACE IF NOT EXISTS my-test-namespace")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Namespace(AstCreateNamespace {
				namespace,
				if_not_exists,
				..
			}) => {
				assert_eq!(namespace.segments[0].text(), "my-test-namespace");
				assert!(if_not_exists);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_namespace_if_not_exists_with_backtick() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "CREATE NAMESPACE IF NOT EXISTS `my-namespace`").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Namespace(AstCreateNamespace {
				namespace,
				if_not_exists,
				..
			}) => {
				assert_eq!(namespace.segments[0].text(), "my-namespace");
				assert!(if_not_exists);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_namespace_name_if_not_exists() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "CREATE NAMESPACE my_namespace IF NOT EXISTS").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Namespace(AstCreateNamespace {
				namespace,
				if_not_exists,
				..
			}) => {
				assert_eq!(namespace.segments[0].text(), "my_namespace");
				assert!(if_not_exists);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_namespace_name_if_not_exists_with_hyphen() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE NAMESPACE my-test-namespace IF NOT EXISTS")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Namespace(AstCreateNamespace {
				namespace,
				if_not_exists,
				..
			}) => {
				assert_eq!(namespace.segments[0].text(), "my-test-namespace");
				assert!(if_not_exists);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_namespace_name_if_not_exists_with_backtick() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "CREATE NAMESPACE `my-namespace` IF NOT EXISTS").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Namespace(AstCreateNamespace {
				namespace,
				if_not_exists,
				..
			}) => {
				assert_eq!(namespace.segments[0].text(), "my-namespace");
				assert!(if_not_exists);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_table_with_hyphen() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "CREATE TABLE my-schema::my-table { id: Int4 }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Table(AstCreateTable {
				table,
				..
			}) => {
				assert_eq!(table.namespace[0].text(), "my-schema");
				assert_eq!(table.name.text(), "my-table");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_ringbuffer_with_hyphen() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE RINGBUFFER my-ns::my-buffer { id: Int4 } WITH { capacity: 100 }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::RingBuffer(AstCreateRingBuffer {
				ringbuffer,
				capacity,
				..
			}) => {
				assert_eq!(ringbuffer.namespace[0].text(), "my-ns");
				assert_eq!(ringbuffer.name.text(), "my-buffer");
				assert_eq!(*capacity, 100);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_dictionary_with_hyphen() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "CREATE DICTIONARY my-dict FOR Text AS Int4").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Dictionary(AstCreateDictionary {
				dictionary,
				..
			}) => {
				assert_eq!(dictionary.name.text(), "my-dict");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_table_with_hyphenated_columns() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE TABLE test::user-data { user-id: Int4, user-name: Text }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Table(AstCreateTable {
				table,
				columns,
				..
			}) => {
				assert_eq!(table.name.text(), "user-data");
				assert_eq!(columns.len(), 2);
				assert_eq!(columns[0].name.text(), "user-id");
				assert_eq!(columns[1].name.text(), "user-name");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_namespace_with_backtick() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE NAMESPACE `my-namespace`").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Namespace(AstCreateNamespace {
				namespace,
				..
			}) => {
				assert_eq!(namespace.segments[0].text(), "my-namespace");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_series() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
            create series test::metrics{value: Int2}
        "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Series(AstCreateSeries {
				series,
				columns,
				..
			}) => {
				assert_eq!(series.namespace[0].text(), "test");
				assert_eq!(series.name.text(), "metrics");

				assert_eq!(columns.len(), 1);

				assert_eq!(columns[0].name.text(), "value");
				match &columns[0].ty {
					AstType::Unconstrained(ident) => {
						assert_eq!(ident.text(), "Int2")
					}
					_ => panic!("Expected simple type"),
				}
				assert!(columns[0].properties.is_empty());
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_table() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        create table test::users{id: int2, name: text, is_premium: bool}
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Table(AstCreateTable {
				table,
				columns,
				..
			}) => {
				assert_eq!(table.namespace[0].text(), "test");
				assert_eq!(table.name.text(), "users");
				assert_eq!(columns.len(), 3);

				{
					let col = &columns[0];
					assert_eq!(col.name.text(), "id");
					match &col.ty {
						AstType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "int2")
						}
						_ => panic!("Expected simple type"),
					}
					assert!(col.properties.is_empty());
				}

				{
					let col = &columns[1];
					assert_eq!(col.name.text(), "name");
					match &col.ty {
						AstType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "text")
						}
						_ => panic!("Expected simple type"),
					}
					assert!(col.properties.is_empty());
				}

				{
					let col = &columns[2];
					assert_eq!(col.name.text(), "is_premium");
					match &col.ty {
						AstType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "bool")
						}
						_ => panic!("Expected simple type"),
					}
					assert!(col.properties.is_empty());
				}
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_table_with_auto_increment() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        create table test::users { id: int4 with { auto_increment }, name: utf8 }
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Table(AstCreateTable {
				table,
				columns,
				..
			}) => {
				assert_eq!(table.namespace[0].text(), "test");
				assert_eq!(table.name.text(), "users");
				assert_eq!(columns.len(), 2);

				{
					let col = &columns[0];
					assert_eq!(col.name.text(), "id");
					match &col.ty {
						AstType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "int4")
						}
						_ => panic!("Expected simple type"),
					}
					assert_eq!(col.properties.len(), 1);
					assert!(matches!(col.properties[0], AstColumnProperty::AutoIncrement));
				}

				{
					let col = &columns[1];
					assert_eq!(col.name.text(), "name");
					match &col.ty {
						AstType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "utf8")
						}
						_ => panic!("Expected simple type"),
					}
					assert!(col.properties.is_empty());
				}
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_deferred_view() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        create deferred view test::views{field: int2}
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();
		match create {
			AstCreate::DeferredView(AstCreateDeferredView {
				view,
				columns,
				..
			}) => {
				assert_eq!(view.namespace[0].text(), "test");
				assert_eq!(view.name.text(), "views");

				assert_eq!(columns.len(), 1);

				let col = &columns[0];
				assert_eq!(col.name.text(), "field");
				match &col.ty {
					AstType::Unconstrained(ident) => {
						assert_eq!(ident.text(), "int2")
					}
					_ => panic!("Expected simple type"),
				}
				assert!(col.properties.is_empty());
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_transactional_view() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        create transactional view test::myview{id: int4, name: utf8}
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();
		match create {
			AstCreate::TransactionalView(AstCreateTransactionalView {
				view,
				columns,
				..
			}) => {
				assert_eq!(view.namespace[0].text(), "test");
				assert_eq!(view.name.text(), "myview");

				assert_eq!(columns.len(), 2);

				{
					let col = &columns[0];
					assert_eq!(col.name.text(), "id");
					match &col.ty {
						AstType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "int4")
						}
						_ => panic!("Expected simple type"),
					}
					assert!(col.properties.is_empty());
				}

				{
					let col = &columns[1];
					assert_eq!(col.name.text(), "name");
					match &col.ty {
						AstType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "utf8")
						}
						_ => panic!("Expected simple type"),
					}
					assert!(col.properties.is_empty());
				}
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_ringbuffer() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        create ringbuffer test::events { id: int4, data: utf8 } with { capacity: 10 }
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::RingBuffer(AstCreateRingBuffer {
				ringbuffer,
				columns,
				capacity,
				..
			}) => {
				assert_eq!(ringbuffer.namespace[0].text(), "test");
				assert_eq!(ringbuffer.name.text(), "events");
				assert_eq!(*capacity, 10);
				assert_eq!(columns.len(), 2);

				{
					let col = &columns[0];
					assert_eq!(col.name.text(), "id");
					match &col.ty {
						AstType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "int4")
						}
						_ => panic!("Expected simple type"),
					}
				}

				{
					let col = &columns[1];
					assert_eq!(col.name.text(), "data");
					match &col.ty {
						AstType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "utf8")
						}
						_ => panic!("Expected simple type"),
					}
				}
			}
			_ => unreachable!("Expected ring buffer create"),
		}
	}

	#[test]
	fn test_create_transactional_view_with_query() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
        create transactional view test::myview{id: int4, name: utf8} as {
            from test::users
            where age > 18
        }
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();
		match create {
			AstCreate::TransactionalView(AstCreateTransactionalView {
				view,
				columns,
				as_clause,
				..
			}) => {
				assert_eq!(view.namespace[0].text(), "test");
				assert_eq!(view.name.text(), "myview");
				assert_eq!(columns.len(), 2);
				assert!(as_clause.is_some());

				if let Some(as_statement) = as_clause {
					// The AS clause should have the query
					// nodes
					assert!(as_statement.len() > 0);
				}
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_dictionary_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE DICTIONARY token_mints FOR Utf8 AS Uint2")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Dictionary(dict) => {
				assert!(dict.dictionary.namespace.is_empty());
				assert_eq!(dict.dictionary.name.text(), "token_mints");
				match &dict.value_type {
					AstType::Unconstrained(ty) => assert_eq!(ty.text(), "Utf8"),
					_ => panic!("Expected unconstrained type"),
				}
				match &dict.id_type {
					AstType::Unconstrained(ty) => assert_eq!(ty.text(), "Uint2"),
					_ => panic!("Expected unconstrained type"),
				}
			}
			_ => unreachable!("Expected Dictionary create"),
		}
	}

	#[test]
	fn test_create_dictionary_qualified() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE DICTIONARY analytics::token_mints FOR Utf8 AS Uint4")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Dictionary(dict) => {
				assert_eq!(dict.dictionary.namespace[0].text(), "analytics");
				assert_eq!(dict.dictionary.name.text(), "token_mints");
				match &dict.value_type {
					AstType::Unconstrained(ty) => assert_eq!(ty.text(), "Utf8"),
					_ => panic!("Expected unconstrained type"),
				}
				match &dict.id_type {
					AstType::Unconstrained(ty) => assert_eq!(ty.text(), "Uint4"),
					_ => panic!("Expected unconstrained type"),
				}
			}
			_ => unreachable!("Expected Dictionary create"),
		}
	}

	#[test]
	fn test_create_dictionary_blob_value() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "CREATE DICTIONARY hashes FOR Blob AS Uint8").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Dictionary(dict) => {
				assert_eq!(dict.dictionary.name.text(), "hashes");
				match &dict.value_type {
					AstType::Unconstrained(ty) => assert_eq!(ty.text(), "Blob"),
					_ => panic!("Expected unconstrained type"),
				}
				match &dict.id_type {
					AstType::Unconstrained(ty) => assert_eq!(ty.text(), "Uint8"),
					_ => panic!("Expected unconstrained type"),
				}
			}
			_ => unreachable!("Expected Dictionary create"),
		}
	}

	#[test]
	fn test_create_dictionary_if_not_exists() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE DICTIONARY IF NOT EXISTS token_mints FOR Utf8 AS Uint4")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Dictionary(dict) => {
				assert!(dict.if_not_exists);
				assert!(dict.dictionary.namespace.is_empty());
				assert_eq!(dict.dictionary.name.text(), "token_mints");
				match &dict.value_type {
					AstType::Unconstrained(ty) => assert_eq!(ty.text(), "Utf8"),
					_ => panic!("Expected unconstrained type"),
				}
				match &dict.id_type {
					AstType::Unconstrained(ty) => assert_eq!(ty.text(), "Uint4"),
					_ => panic!("Expected unconstrained type"),
				}
			}
			_ => unreachable!("Expected Dictionary create"),
		}
	}

	#[test]
	fn test_create_enum_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE ENUM Status { Active, Inactive, Pending }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Enum(AstCreateSumType {
				if_not_exists,
				name,
				variants,
				..
			}) => {
				assert!(!if_not_exists);
				assert!(name.namespace.is_empty());
				assert_eq!(name.name.text(), "Status");
				assert_eq!(variants.len(), 3);
				assert_eq!(variants[0].name.text(), "Active");
				assert_eq!(variants[1].name.text(), "Inactive");
				assert_eq!(variants[2].name.text(), "Pending");
				assert!(variants[0].columns.is_empty());
				assert!(variants[1].columns.is_empty());
				assert!(variants[2].columns.is_empty());
			}
			_ => unreachable!("Expected Enum create"),
		}
	}

	#[test]
	fn test_create_enum_with_fields() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			"CREATE ENUM Shape { Circle { radius: Float8 }, Rectangle { width: Float8, height: Float8 } }",
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Enum(AstCreateSumType {
				name,
				variants,
				..
			}) => {
				assert_eq!(name.name.text(), "Shape");
				assert_eq!(variants.len(), 2);

				assert_eq!(variants[0].name.text(), "Circle");
				assert_eq!(variants[0].columns.len(), 1);
				assert_eq!(variants[0].columns[0].name.text(), "radius");

				assert_eq!(variants[1].name.text(), "Rectangle");
				assert_eq!(variants[1].columns.len(), 2);
				assert_eq!(variants[1].columns[0].name.text(), "width");
				assert_eq!(variants[1].columns[1].name.text(), "height");
			}
			_ => unreachable!("Expected Enum create"),
		}
	}

	#[test]
	fn test_create_enum_qualified_name() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE ENUM analytics::Status { Active, Inactive }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Enum(AstCreateSumType {
				name,
				variants,
				..
			}) => {
				assert_eq!(name.namespace[0].text(), "analytics");
				assert_eq!(name.name.text(), "Status");
				assert_eq!(variants.len(), 2);
			}
			_ => unreachable!("Expected Enum create"),
		}
	}

	#[test]
	fn test_create_enum_if_not_exists() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "CREATE ENUM IF NOT EXISTS Status { Active }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Enum(AstCreateSumType {
				if_not_exists,
				name,
				variants,
				..
			}) => {
				assert!(if_not_exists);
				assert!(name.namespace.is_empty());
				assert_eq!(name.name.text(), "Status");
				assert_eq!(variants.len(), 1);
				assert_eq!(variants[0].name.text(), "Active");
			}
			_ => unreachable!("Expected Enum create"),
		}
	}

	#[test]
	fn test_create_subscription_basic() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "CREATE SUBSCRIPTION { id: Int4, name: Utf8 }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Subscription(AstCreateSubscription {
				columns,
				..
			}) => {
				assert_eq!(columns.len(), 2);

				{
					let col = &columns[0];
					assert_eq!(col.name.text(), "id");
					match &col.ty {
						AstType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "Int4")
						}
						_ => panic!("Expected simple type"),
					}
				}

				{
					let col = &columns[1];
					assert_eq!(col.name.text(), "name");
					match &col.ty {
						AstType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "Utf8")
						}
						_ => panic!("Expected simple type"),
					}
				}
			}
			_ => unreachable!("Expected Subscription create"),
		}
	}

	#[test]
	fn test_create_subscription_single_column() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE SUBSCRIPTION { value: Float8 }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Subscription(AstCreateSubscription {
				columns,
				..
			}) => {
				assert_eq!(columns.len(), 1);
				assert_eq!(columns[0].name.text(), "value");
				match &columns[0].ty {
					AstType::Unconstrained(ident) => {
						assert_eq!(ident.text(), "Float8")
					}
					_ => panic!("Expected simple type"),
				}
			}
			_ => unreachable!("Expected Subscription create"),
		}
	}

	#[test]
	fn test_create_subscription_with_simple_query() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE SUBSCRIPTION { id: Int4, name: Utf8 } AS { from test::products }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Subscription(AstCreateSubscription {
				columns,
				as_clause,
				..
			}) => {
				assert_eq!(columns.len(), 2);
				assert_eq!(columns[0].name.text(), "id");
				assert_eq!(columns[1].name.text(), "name");

				assert!(as_clause.is_some(), "AS clause should be present");
				let as_clause = as_clause.as_ref().unwrap();
				assert_eq!(as_clause.nodes.len(), 1, "Should have one FROM node");

				// Validate that the node is a FROM node
				match &as_clause.nodes[0] {
					Ast::From(_) => {
						// Expected: FROM node
					}
					_ => panic!("Expected FROM node in AS clause"),
				}
			}
			_ => unreachable!("Expected Subscription create"),
		}
	}

	#[test]
	fn test_create_subscription_with_piped_query() {
		let bump = Bump::new();
		let tokens = tokenize(&bump,"CREATE SUBSCRIPTION { id: Int4, price: Float8 } AS { from test::products | filter {price > 50} | filter {stock > 0} }",
		).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Subscription(AstCreateSubscription {
				columns,
				as_clause,
				..
			}) => {
				assert_eq!(columns.len(), 2);

				assert!(as_clause.is_some(), "AS clause should be present");
				let as_clause = as_clause.as_ref().unwrap();

				assert!(as_clause.nodes.len() >= 1, "Should have at least FROM node");

				match &as_clause.nodes[0] {
					Ast::From(_) => {}
					_ => panic!("Expected FROM node as first node in AS clause"),
				}
			}
			_ => unreachable!("Expected Subscription create"),
		}
	}

	#[test]
	fn test_create_subscription_without_as_clause() {
		let bump = Bump::new();
		// Ensure subscriptions without AS clause still work (backwards compatibility)
		let tokens = tokenize(&bump, "CREATE SUBSCRIPTION { value: Float8 }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Subscription(AstCreateSubscription {
				as_clause,
				..
			}) => {
				assert!(as_clause.is_none(), "AS clause should not be present");
			}
			_ => unreachable!("Expected Subscription create"),
		}
	}

	#[test]
	fn test_create_subscription_schemaless() {
		let bump = Bump::new();
		// Test schema-less subscription: CREATE SUBSCRIPTION AS { FROM demo::events }
		let tokens =
			tokenize(&bump, "CREATE SUBSCRIPTION AS { FROM demo::events }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Subscription(AstCreateSubscription {
				columns,
				as_clause,
				..
			}) => {
				assert_eq!(columns.len(), 0, "Schema-less should have no columns");
				assert!(as_clause.is_some(), "AS clause should be present");

				let as_clause = as_clause.as_ref().unwrap();
				assert_eq!(as_clause.nodes.len(), 1, "Should have one FROM node");

				match &as_clause.nodes[0] {
					Ast::From(_) => {
						// Expected: FROM node
					}
					_ => panic!("Expected FROM node in AS clause"),
				}
			}
			_ => unreachable!("Expected Subscription create"),
		}
	}

	#[test]
	fn test_create_subscription_schemaless_with_filter() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "CREATE SUBSCRIPTION AS { FROM demo::events | FILTER {id > 1 and id < 3} }")
				.unwrap()
				.into_iter()
				.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Subscription(AstCreateSubscription {
				columns,
				as_clause,
				..
			}) => {
				assert_eq!(columns.len(), 0, "Schema-less should have no columns");
				assert!(as_clause.is_some(), "AS clause should be present");

				let as_clause = as_clause.as_ref().unwrap();
				assert!(as_clause.nodes.len() >= 1, "Should have at least FROM node");
				assert!(as_clause.has_pipes, "Should have pipes");

				match &as_clause.nodes[0] {
					Ast::From(_) => {}
					_ => panic!("Expected FROM node as first node"),
				}
			}
			_ => unreachable!("Expected Subscription create"),
		}
	}

	#[test]
	fn test_create_subscription_schemaless_missing_as_fails() {
		let bump = Bump::new();
		// Test that schema-less subscription without AS clause fails
		let tokens = tokenize(&bump, "CREATE SUBSCRIPTION").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse();

		// Should fail with an error
		assert!(result.is_err(), "Schema-less subscription without AS should fail");
	}

	#[test]
	fn test_create_subscription_backward_compat_with_columns() {
		let bump = Bump::new();
		// Test backward compatibility: subscriptions with columns and AS still work
		let tokens = tokenize(&bump, "CREATE SUBSCRIPTION { id: Int4 } AS { FROM demo::events }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Subscription(AstCreateSubscription {
				columns,
				as_clause,
				..
			}) => {
				assert_eq!(columns.len(), 1, "Should have one column");
				assert_eq!(columns[0].name.text(), "id");
				assert!(as_clause.is_some(), "AS clause should be present");
			}
			_ => unreachable!("Expected Subscription create"),
		}
	}
}
