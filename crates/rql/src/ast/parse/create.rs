// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::sort::SortDirection;

use crate::{
	ast::{
		ast::{
			AstColumnToCreate, AstCreate, AstCreateDeferredView, AstCreateDictionary, AstCreateNamespace,
			AstCreatePolicy, AstCreatePrimaryKey, AstCreateRingBuffer, AstCreateSeries,
			AstCreateSubscription, AstCreateSumType, AstCreateTable, AstCreateTransactionalView,
			AstIndexColumn, AstPolicyEntry, AstPolicyKind, AstPrimaryKeyDef, AstType, AstVariantDef,
		},
		identifier::{
			MaybeQualifiedDictionaryIdentifier, MaybeQualifiedNamespaceIdentifier,
			MaybeQualifiedSequenceIdentifier, MaybeQualifiedSumTypeIdentifier,
			MaybeQualifiedTableIdentifier,
		},
		parse::Parser,
	},
	bump::BumpBox,
	token::{
		keyword::{
			Keyword,
			Keyword::{
				Create, Deferred, Dictionary, Exists, Flow, For, If, Namespace, Replace, Ringbuffer,
				Series, Subscription, Table, Transactional, View,
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
	pub(crate) fn parse_create(&mut self) -> crate::Result<AstCreate<'bump>> {
		let token = self.consume_keyword(Create)?;

		// Check for CREATE OR REPLACE
		let or_replace = if (self.consume_if(TokenKind::Operator(Or))?).is_some() {
			self.consume_keyword(Replace)?;
			true
		} else {
			false
		};

		// Check for CREATE FLOW
		if (self.consume_if(TokenKind::Keyword(Flow))?).is_some() {
			return self.parse_flow(token, or_replace);
		}

		// CREATE OR REPLACE is only valid for FLOW currently
		if or_replace {
			return Err(reifydb_type::error::Error(
				reifydb_type::error::diagnostic::ast::unexpected_token_error(
					"FLOW after CREATE OR REPLACE",
					self.current()?.fragment.to_owned(),
				),
			));
		}

		if (self.consume_if(TokenKind::Keyword(Namespace))?).is_some() {
			return self.parse_namespace(token);
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
			return self.parse_table(token);
		}

		if (self.consume_if(TokenKind::Keyword(Ringbuffer))?).is_some() {
			return self.parse_ringbuffer(token);
		}

		if (self.consume_if(TokenKind::Keyword(Dictionary))?).is_some() {
			return self.parse_dictionary(token);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Enum))?).is_some() {
			return self.parse_enum(token);
		}

		if (self.consume_if(TokenKind::Keyword(Series))?).is_some() {
			return self.parse_series(token);
		}

		if (self.consume_if(TokenKind::Keyword(Subscription))?).is_some() {
			return self.parse_subscription(token);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Primary))?).is_some() {
			self.consume_keyword(Keyword::Key)?;
			return self.parse_create_primary_key(token);
		}

		if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
			return self.parse_create_policy(token);
		}

		if self.peek_is_index_creation()? {
			return self.parse_create_index(token);
		}

		unimplemented!();
	}

	fn parse_namespace(&mut self, token: Token<'bump>) -> crate::Result<AstCreate<'bump>> {
		// Check for IF NOT EXISTS BEFORE identifier
		let mut if_not_exists = if (self.consume_if(TokenKind::Keyword(If))?).is_some() {
			self.consume_operator(Not)?;
			self.consume_keyword(Exists)?;
			true
		} else {
			false
		};

		let segments = self.parse_dot_separated_identifiers()?;

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

	fn parse_series(&mut self, token: Token<'bump>) -> crate::Result<AstCreate<'bump>> {
		let mut segments = self.parse_dot_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let columns = self.parse_columns()?;

		let sequence = MaybeQualifiedSequenceIdentifier::new(name).with_namespace(namespace);

		Ok(AstCreate::Series(AstCreateSeries {
			token,
			sequence,
			columns,
		}))
	}

	fn parse_subscription(&mut self, token: Token<'bump>) -> crate::Result<AstCreate<'bump>> {
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
			return Err(reifydb_type::error::Error(
				reifydb_type::error::diagnostic::ast::unexpected_token_error(
					"'{' or 'AS'",
					self.current()?.fragment.to_owned(),
				),
			));
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

				let node = self.parse_node(crate::ast::parse::Precedence::None)?;
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

			Some(crate::ast::ast::AstStatement {
				nodes: query_nodes,
				has_pipes,
				is_output: false,
			})
		} else {
			None
		};

		// Validation: schema-less subscriptions require AS clause
		if columns.is_empty() && as_clause.is_none() {
			return Err(reifydb_type::error::Error(
				reifydb_type::error::diagnostic::ast::unexpected_token_error(
					"AS clause (schema-less CREATE SUBSCRIPTION requires AS clause)",
					self.current().ok().map(|t| t.fragment.to_owned()).unwrap_or_else(|| {
						reifydb_type::fragment::Fragment::internal("end of input")
					}),
				),
			));
		}

		Ok(AstCreate::Subscription(AstCreateSubscription {
			token,
			columns,
			as_clause,
		}))
	}

	fn parse_deferred_view(&mut self, token: Token<'bump>) -> crate::Result<AstCreate<'bump>> {
		let mut segments = self.parse_dot_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let columns = self.parse_columns()?;

		use crate::ast::identifier::MaybeQualifiedDeferredViewIdentifier;

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

				let node = self.parse_node(crate::ast::parse::Precedence::None)?;
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

			Some(crate::ast::ast::AstStatement {
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

	fn parse_transactional_view(&mut self, token: Token<'bump>) -> crate::Result<AstCreate<'bump>> {
		let mut segments = self.parse_dot_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let columns = self.parse_columns()?;

		use crate::ast::identifier::MaybeQualifiedTransactionalViewIdentifier;

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

				let node = self.parse_node(crate::ast::parse::Precedence::None)?;
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

			Some(crate::ast::ast::AstStatement {
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

	fn parse_table(&mut self, token: Token<'bump>) -> crate::Result<AstCreate<'bump>> {
		let mut segments = self.parse_dot_separated_identifiers()?;
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

	fn parse_ringbuffer(&mut self, token: Token<'bump>) -> crate::Result<AstCreate<'bump>> {
		let mut segments = self.parse_dot_separated_identifiers()?;
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

			let key = self.consume(TokenKind::Identifier)?;
			self.consume_operator(Operator::Colon)?;

			match key.fragment.text() {
				"capacity" => {
					let capacity_token = self.consume(TokenKind::Literal(Literal::Number))?;
					capacity =
						Some(capacity_token.fragment.text().parse::<u64>().map_err(|_| {
							reifydb_type::error::Error(
								reifydb_type::error::diagnostic::ast::unexpected_token_error(
									"valid capacity number",
									capacity_token.fragment.to_owned(),
								),
							)
						})?);
				}
				_other => {
					return Err(reifydb_type::error::Error(
						reifydb_type::error::diagnostic::ast::unexpected_token_error(
							"'capacity'",
							key.fragment.to_owned(),
						),
					));
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
			reifydb_type::error::Error(reifydb_type::error::diagnostic::ast::unexpected_token_error(
				"'capacity' is required for RINGBUFFER",
				self.current()
					.ok()
					.map(|t| t.fragment.to_owned())
					.unwrap_or_else(|| reifydb_type::fragment::Fragment::internal("end of input")),
			))
		})?;

		use crate::ast::identifier::MaybeQualifiedRingBufferIdentifier;

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
	fn parse_primary_key_definition(&mut self) -> crate::Result<AstPrimaryKeyDef<'bump>> {
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
			return Err(reifydb_type::error::Error(
				reifydb_type::error::diagnostic::ast::unexpected_token_error(
					"at least one column in primary key",
					self.current()?.fragment.to_owned(),
				),
			));
		}

		Ok(AstPrimaryKeyDef {
			columns,
		})
	}

	/// Parse CREATE PRIMARY KEY ON ns.table { col1, col2: desc }
	fn parse_create_primary_key(&mut self, token: Token<'bump>) -> crate::Result<AstCreate<'bump>> {
		self.consume_keyword(Keyword::On)?;

		let mut segments = self.parse_dot_separated_identifiers()?;
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

	/// Parse CREATE POLICY ON ns.table.column { saturation: error, default: 0 }
	fn parse_create_policy(&mut self, token: Token<'bump>) -> crate::Result<AstCreate<'bump>> {
		self.consume_keyword(Keyword::On)?;

		let column = self.parse_column_identifier()?;

		self.consume_operator(Operator::OpenCurly)?;

		let mut policies = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			// Parse policy kind
			let kind_token = self.consume(TokenKind::Identifier)?;
			let kind = match kind_token.fragment.text() {
				"saturation" => AstPolicyKind::Saturation,
				"default" => AstPolicyKind::Default,
				_ => {
					return Err(reifydb_type::error::Error(
						reifydb_type::error::diagnostic::ast::invalid_policy_error(
							kind_token.fragment.to_owned(),
						),
					));
				}
			};

			// Consume colon separator
			self.consume_operator(Operator::Colon)?;

			// Parse policy value
			let value = BumpBox::new_in(self.parse_node(crate::ast::parse::Precedence::None)?, self.bump());

			policies.push(AstPolicyEntry {
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

		Ok(AstCreate::Policy(AstCreatePolicy {
			token,
			column,
			policies,
		}))
	}

	fn parse_dictionary(&mut self, token: Token<'bump>) -> crate::Result<AstCreate<'bump>> {
		// Check for IF NOT EXISTS
		let if_not_exists = if (self.consume_if(TokenKind::Keyword(If))?).is_some() {
			self.consume_operator(Not)?;
			self.consume_keyword(Exists)?;
			true
		} else {
			false
		};

		let mut segments = self.parse_dot_separated_identifiers()?;
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

	fn parse_enum(&mut self, token: Token<'bump>) -> crate::Result<AstCreate<'bump>> {
		let if_not_exists = if (self.consume_if(TokenKind::Keyword(If))?).is_some() {
			self.consume_operator(Not)?;
			self.consume_keyword(Exists)?;
			true
		} else {
			false
		};

		let mut segments = self.parse_dot_separated_identifiers()?;
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

	fn parse_type(&mut self) -> crate::Result<AstType<'bump>> {
		let ty_token = self.consume(TokenKind::Identifier)?;

		// Check for Option(T) syntax
		if ty_token.fragment.text().eq_ignore_ascii_case("option") {
			self.consume_operator(Operator::OpenParen)?;
			let inner = self.parse_type()?;
			self.consume_operator(Operator::CloseParen)?;
			return Ok(AstType::Optional(Box::new(inner)));
		}

		if !self.is_eof() && self.current()?.is_operator(Operator::Dot) {
			self.consume_operator(Operator::Dot)?;
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

	fn parse_columns(&mut self) -> crate::Result<Vec<AstColumnToCreate<'bump>>> {
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

	fn parse_column(&mut self) -> crate::Result<AstColumnToCreate<'bump>> {
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
		} else if !self.is_eof() && self.current()?.is_operator(Operator::Dot) {
			self.consume_operator(Operator::Dot)?;
			let name_token = self.consume(TokenKind::Identifier)?;
			AstType::Qualified {
				namespace: ty_token.fragment,
				name: name_token.fragment,
			}
		} else if self.current()?.is_operator(Operator::OpenParen) {
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

		let auto_increment = if self.current()?.is_keyword(Keyword::Auto) {
			self.consume_keyword(Keyword::Auto)?;
			self.consume_keyword(Keyword::Increment)?;
			true
		} else {
			false
		};

		// Parse optional DICTIONARY clause
		let dictionary = if self.current()?.is_keyword(Keyword::Dictionary) {
			self.consume_keyword(Keyword::Dictionary)?;
			let mut segments = self.parse_dot_separated_identifiers()?;
			let name = segments.pop().unwrap().into_fragment();
			let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
			let dict_ident = if namespace.is_empty() {
				MaybeQualifiedDictionaryIdentifier::new(name)
			} else {
				MaybeQualifiedDictionaryIdentifier::new(name).with_namespace(namespace)
			};
			Some(dict_ident)
		} else {
			None
		};

		Ok(AstColumnToCreate {
			name,
			ty,
			auto_increment,
			dictionary,
		})
	}

	fn parse_flow(&mut self, token: Token<'bump>, or_replace: bool) -> crate::Result<AstCreate<'bump>> {
		use crate::ast::identifier::MaybeQualifiedFlowIdentifier;

		// Check for IF NOT EXISTS
		let if_not_exists = if (self.consume_if(TokenKind::Keyword(If))?).is_some() {
			self.consume_operator(Not)?;
			self.consume_keyword(Exists)?;
			true
		} else {
			false
		};

		let mut segments = self.parse_dot_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let flow = if namespace.is_empty() {
			MaybeQualifiedFlowIdentifier::new(name)
		} else {
			MaybeQualifiedFlowIdentifier::new(name).with_namespace(namespace)
		};

		// Parse required AS clause
		self.consume_operator(Operator::As)?;

		// The AS clause can be either:
		// 1. Curly brace syntax: AS { FROM ... }
		// 2. Direct syntax: AS FROM ...
		let as_clause = if self.current()?.kind == TokenKind::Operator(Operator::OpenCurly) {
			// Curly brace syntax
			self.consume_operator(Operator::OpenCurly)?;

			let mut query_nodes = Vec::new();
			let mut has_pipes = false;

			// Parse statements until we hit the closing brace
			loop {
				if self.is_eof() || self.current()?.kind == TokenKind::Operator(Operator::CloseCurly) {
					break;
				}

				let node = self.parse_node(crate::ast::parse::Precedence::None)?;
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

			self.consume_operator(Operator::CloseCurly)?;

			crate::ast::ast::AstStatement {
				nodes: query_nodes,
				has_pipes,
				is_output: false,
			}
		} else {
			// Direct syntax - parse until semicolon or EOF
			let mut query_nodes = Vec::new();
			let mut has_pipes = false;

			// Parse nodes until we hit a terminator
			loop {
				if self.is_eof() {
					break;
				}

				// Check for statement terminators
				if self.current()?.kind == TokenKind::Separator(Separator::Semicolon) {
					break;
				}

				let node = self.parse_node(crate::ast::parse::Precedence::None)?;
				query_nodes.push(node);

				if !self.is_eof() {
					// Check for pipe operator or newline as separator
					if self.current()?.is_operator(Operator::Pipe) {
						self.advance()?; // consume the pipe
						has_pipes = true;
					} else {
						// Try to consume a newline if present (optional)
						self.consume_if(TokenKind::Separator(Separator::NewLine))?;
					}
				}
			}

			crate::ast::ast::AstStatement {
				nodes: query_nodes,
				has_pipes,
				is_output: false,
			}
		};

		use crate::ast::ast::AstCreateFlow;
		Ok(AstCreate::Flow(AstCreateFlow {
			token,
			or_replace,
			if_not_exists,
			flow,
			as_clause,
		}))
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{
				Ast, AstCreate, AstCreateDeferredView, AstCreateDictionary, AstCreateNamespace,
				AstCreateRingBuffer, AstCreateSeries, AstCreateSubscription, AstCreateSumType,
				AstCreateTable, AstCreateTransactionalView, AstType,
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
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
			tokenize(&bump, "CREATE TABLE my-schema.my-table { id: Int4 }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let tokens = tokenize(&bump, "CREATE RINGBUFFER my-ns.my-buffer { id: Int4 } WITH { capacity: 100 }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let tokens = tokenize(&bump, "CREATE TABLE test.user-data { user-id: Int4, user-name: Text }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
            create series test.metrics{value: Int2}
        "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Series(AstCreateSeries {
				sequence,
				columns,
				..
			}) => {
				assert_eq!(sequence.namespace[0].text(), "test");
				assert_eq!(sequence.name.text(), "metrics");

				assert_eq!(columns.len(), 1);

				assert_eq!(columns[0].name.text(), "value");
				match &columns[0].ty {
					AstType::Unconstrained(ident) => {
						assert_eq!(ident.text(), "Int2")
					}
					_ => panic!("Expected simple type"),
				}
				assert_eq!(columns[0].auto_increment, false);
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
        create table test.users{id: int2, name: text, is_premium: bool}
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
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
					assert_eq!(col.auto_increment, false);
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
					assert_eq!(col.auto_increment, false);
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
					assert_eq!(col.auto_increment, false);
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
        create table test.users { id: int4 AUTO INCREMENT, name: utf8 }
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
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
					assert_eq!(col.auto_increment, true);
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
					assert_eq!(col.auto_increment, false);
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
        create deferred view test.views{field: int2}
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
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
				assert_eq!(col.auto_increment, false);
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
        create transactional view test.myview{id: int4, name: utf8}
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
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
					assert_eq!(col.auto_increment, false);
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
					assert_eq!(col.auto_increment, false);
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
        create ringbuffer test.events { id: int4, data: utf8 } with { capacity: 10 }
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		use crate::ast::ast::AstCreateRingBuffer;

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
        create transactional view test.myview{id: int4, name: utf8} as {
            from test.users
            where age > 18
        }
    "#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
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
	fn test_create_flow_basic() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE FLOW my_flow AS FROM orders WHERE status = 'pending'")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Flow(flow) => {
				assert!(!flow.or_replace);
				assert!(!flow.if_not_exists);
				assert_eq!(flow.flow.name.text(), "my_flow");
				assert!(flow.flow.namespace.is_empty());
				assert!(flow.as_clause.len() > 0);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_flow_or_replace() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "CREATE OR REPLACE FLOW my_flow AS FROM orders").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Flow(flow) => {
				assert!(flow.or_replace);
				assert!(!flow.if_not_exists);
				assert_eq!(flow.flow.name.text(), "my_flow");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_flow_if_not_exists() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE FLOW IF NOT EXISTS my_flow AS FROM orders")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Flow(flow) => {
				assert!(!flow.or_replace);
				assert!(flow.if_not_exists);
				assert_eq!(flow.flow.name.text(), "my_flow");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_flow_qualified_name() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE FLOW analytics.sales_flow AS FROM sales.orders")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Flow(flow) => {
				assert_eq!(flow.flow.namespace[0].text(), "analytics");
				assert_eq!(flow.flow.name.text(), "sales_flow");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_flow_complex_query() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
			CREATE FLOW aggregated AS {
				FROM raw_events
				FILTER {event_type = 'purchase'}
				AGGREGATE BY {user_id}
				SELECT { user_id, total: SUM(amount) }
			}
		"#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Flow(flow) => {
				assert_eq!(flow.flow.name.text(), "aggregated");
				assert!(flow.as_clause.len() >= 4);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_or_replace_flow_if_not_exists() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "CREATE OR REPLACE FLOW IF NOT EXISTS test.my_flow AS FROM orders")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Flow(flow) => {
				assert!(flow.or_replace);
				assert!(flow.if_not_exists);
				assert_eq!(flow.flow.namespace[0].text(), "test");
				assert_eq!(flow.flow.name.text(), "my_flow");
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
		let mut parser = Parser::new(&bump, tokens);
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
		let tokens = tokenize(&bump, "CREATE DICTIONARY analytics.token_mints FOR Utf8 AS Uint4")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let tokens = tokenize(&bump, "CREATE ENUM analytics.Status { Active, Inactive }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		let tokens = tokenize(&bump, "CREATE SUBSCRIPTION { id: Int4, name: Utf8 } AS { from test.products }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let tokens = tokenize(&bump,"CREATE SUBSCRIPTION { id: Int4, price: Float8 } AS { from test.products | filter {price > 50} | filter {stock > 0} }",
		).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
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
		// Test schema-less subscription: CREATE SUBSCRIPTION AS { FROM demo.events }
		let tokens =
			tokenize(&bump, "CREATE SUBSCRIPTION AS { FROM demo.events }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
			tokenize(&bump, "CREATE SUBSCRIPTION AS { FROM demo.events | FILTER {id > 1 and id < 3} }")
				.unwrap()
				.into_iter()
				.collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse();

		// Should fail with an error
		assert!(result.is_err(), "Schema-less subscription without AS should fail");
	}

	#[test]
	fn test_create_subscription_backward_compat_with_columns() {
		let bump = Bump::new();
		// Test backward compatibility: subscriptions with columns and AS still work
		let tokens = tokenize(&bump, "CREATE SUBSCRIPTION { id: Int4 } AS { FROM demo.events }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
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
