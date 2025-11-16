// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use Keyword::{Create, Exists, Flow, If, Namespace, Replace};
use Operator::Colon;

use crate::ast::{
	AstColumnToCreate, AstCreate, AstCreateDeferredView, AstCreateNamespace, AstCreateRingBuffer, AstCreateSeries,
	AstCreateTable, AstCreateTransactionalView, AstDataType,
	identifier::{MaybeQualifiedNamespaceIdentifier, MaybeQualifiedSequenceIdentifier},
	parse::Parser,
	tokenize::{
		Keyword,
		Keyword::{Buffer, Deferred, Ring, Series, Table, Transactional, View},
		Operator,
		Operator::{Not, Or},
		Separator,
		Separator::Comma,
		Token, TokenKind,
	},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_create(&mut self) -> crate::Result<AstCreate<'a>> {
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
			return Err(reifydb_type::Error(reifydb_type::diagnostic::ast::unexpected_token_error(
				"FLOW after CREATE OR REPLACE",
				self.current()?.fragment.clone(),
			)));
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

		if (self.consume_if(TokenKind::Keyword(Ring))?).is_some() {
			self.consume_keyword(Buffer)?;
			return self.parse_ring_buffer(token);
		}

		if (self.consume_if(TokenKind::Keyword(Series))?).is_some() {
			return self.parse_series(token);
		}

		if self.peek_is_index_creation()? {
			return self.parse_create_index(token);
		}

		unimplemented!();
	}

	fn parse_namespace(&mut self, token: Token<'a>) -> crate::Result<AstCreate<'a>> {
		let name_token = self.consume(crate::ast::tokenize::TokenKind::Identifier)?;
		let namespace = MaybeQualifiedNamespaceIdentifier::new(name_token.fragment.clone());
		Ok(AstCreate::Namespace(AstCreateNamespace {
			token,
			namespace,
		}))
	}

	fn parse_series(&mut self, token: Token<'a>) -> crate::Result<AstCreate<'a>> {
		let schema_token = self.consume(TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let name_token = self.consume(TokenKind::Identifier)?;
		let columns = self.parse_columns()?;

		let sequence = MaybeQualifiedSequenceIdentifier::new(name_token.fragment.clone())
			.with_namespace(schema_token.fragment.clone());

		Ok(AstCreate::Series(AstCreateSeries {
			token,
			sequence,
			columns,
		}))
	}

	fn parse_deferred_view(&mut self, token: Token<'a>) -> crate::Result<AstCreate<'a>> {
		let schema_token = self.consume(TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let name_token = self.consume(TokenKind::Identifier)?;
		let columns = self.parse_columns()?;

		use crate::ast::identifier::MaybeQualifiedDeferredViewIdentifier;

		let view = MaybeQualifiedDeferredViewIdentifier::new(name_token.fragment.clone())
			.with_namespace(schema_token.fragment.clone());

		// Parse optional AS clause
		let as_clause = if self.consume_if(TokenKind::Operator(Operator::As))?.is_some() {
			// Expect opening curly brace
			self.consume_operator(Operator::OpenCurly)?;

			// Parse the query nodes inside the AS clause
			let mut query_nodes = Vec::new();

			// Parse statements until we hit the closing brace
			loop {
				if self.is_eof() || self.current()?.kind == TokenKind::Operator(Operator::CloseCurly) {
					break;
				}

				let node = self.parse_node(crate::ast::parse::Precedence::None)?;
				query_nodes.push(node);
			}

			// Expect closing curly brace
			self.consume_operator(Operator::CloseCurly)?;

			Some(crate::ast::AstStatement {
				nodes: query_nodes,
				has_pipes: false,
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

	fn parse_transactional_view(&mut self, token: Token<'a>) -> crate::Result<AstCreate<'a>> {
		let schema_token = self.consume(TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let name_token = self.consume(TokenKind::Identifier)?;
		let columns = self.parse_columns()?;

		// Create MaybeQualifiedSourceIdentifier for transactional view
		use crate::ast::identifier::MaybeQualifiedTransactionalViewIdentifier;

		let view = MaybeQualifiedTransactionalViewIdentifier::new(name_token.fragment.clone())
			.with_namespace(schema_token.fragment.clone());

		// Parse optional AS clause
		let as_clause = if self.consume_if(TokenKind::Operator(Operator::As))?.is_some() {
			// Expect opening curly brace
			self.consume_operator(Operator::OpenCurly)?;

			// Parse the query nodes inside the AS clause
			let mut query_nodes = Vec::new();

			// Parse statements until we hit the closing brace
			loop {
				if self.is_eof() || self.current()?.kind == TokenKind::Operator(Operator::CloseCurly) {
					break;
				}

				let node = self.parse_node(crate::ast::parse::Precedence::None)?;
				query_nodes.push(node);
			}

			// Expect closing curly brace
			self.consume_operator(Operator::CloseCurly)?;

			Some(crate::ast::AstStatement {
				nodes: query_nodes,
				has_pipes: false,
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

	fn parse_table(&mut self, token: Token<'a>) -> crate::Result<AstCreate<'a>> {
		let schema_token = self.consume(TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let name_token = self.advance()?;
		let columns = self.parse_columns()?;

		use crate::ast::identifier::MaybeQualifiedTableIdentifier;

		let table = MaybeQualifiedTableIdentifier::new(name_token.fragment.clone())
			.with_namespace(schema_token.fragment.clone());

		Ok(AstCreate::Table(AstCreateTable {
			token,
			table,
			columns,
		}))
	}

	fn parse_ring_buffer(&mut self, token: Token<'a>) -> crate::Result<AstCreate<'a>> {
		let schema_token = self.consume(TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let name_token = self.advance()?;
		let columns = self.parse_columns()?;

		// Parse WITH clause for capacity
		self.consume_keyword(Keyword::With)?;

		// Parse capacity option
		let capacity_ident = self.consume(TokenKind::Identifier)?;
		if capacity_ident.fragment.text() != "capacity" {
			return Err(reifydb_type::Error(reifydb_type::diagnostic::ast::unexpected_token_error(
				"capacity",
				capacity_ident.fragment.clone(),
			)));
		}

		self.consume_operator(Operator::Equal)?;
		let capacity_token = self.consume(TokenKind::Literal(crate::ast::tokenize::Literal::Number))?;

		// Parse capacity value
		let capacity = match capacity_token.fragment.text().parse::<u64>() {
			Ok(val) => val,
			Err(_) => {
				return Err(reifydb_type::Error(
					reifydb_type::diagnostic::ast::unexpected_token_error(
						"valid capacity number",
						capacity_token.fragment.clone(),
					),
				));
			}
		};

		use crate::ast::identifier::MaybeQualifiedRingBufferIdentifier;

		let ring_buffer = MaybeQualifiedRingBufferIdentifier::new(name_token.fragment.clone())
			.with_namespace(schema_token.fragment.clone());

		Ok(AstCreate::RingBuffer(AstCreateRingBuffer {
			token,
			ring_buffer,
			columns,
			capacity,
		}))
	}

	fn parse_columns(&mut self) -> crate::Result<Vec<AstColumnToCreate<'a>>> {
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

	fn parse_column(&mut self) -> crate::Result<AstColumnToCreate<'a>> {
		let name_token = self.advance()?;
		self.consume_operator(Colon)?;
		let ty_token = self.consume(TokenKind::Identifier)?;

		let name = name_token.fragment;

		// Parse type with optional parameters
		let ty = if self.current()?.is_operator(Operator::OpenParen) {
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

			AstDataType::Constrained {
				name: ty_token.fragment,
				params,
			}
		} else {
			// Simple type without parameters
			AstDataType::Unconstrained(ty_token.fragment)
		};

		let auto_increment = if self.current()?.is_keyword(Keyword::Auto) {
			self.consume_keyword(Keyword::Auto)?;
			self.consume_keyword(Keyword::Increment)?;
			true
		} else {
			false
		};

		let policies = if self.current()?.is_keyword(Keyword::Policy) {
			Some(self.parse_policy_block()?)
		} else {
			None
		};

		Ok(AstColumnToCreate {
			name,
			ty,
			policies,
			auto_increment,
		})
	}

	fn parse_flow(&mut self, token: Token<'a>, or_replace: bool) -> crate::Result<AstCreate<'a>> {
		use crate::ast::identifier::MaybeQualifiedFlowIdentifier;

		// Check for IF NOT EXISTS
		let if_not_exists = if (self.consume_if(TokenKind::Keyword(If))?).is_some() {
			self.consume_operator(Not)?;
			self.consume_keyword(Exists)?;
			true
		} else {
			false
		};

		// Parse the flow identifier (namespace.name or just name)
		let first_token = self.consume(TokenKind::Identifier)?;

		let flow = if (self.consume_if(TokenKind::Operator(Operator::Dot))?).is_some() {
			// namespace.name format
			let second_token = self.consume(TokenKind::Identifier)?;
			MaybeQualifiedFlowIdentifier::new(second_token.fragment.clone())
				.with_namespace(first_token.fragment.clone())
		} else {
			// just name format
			MaybeQualifiedFlowIdentifier::new(first_token.fragment.clone())
		};

		// Parse optional column definitions
		let columns = if self.current()?.kind == TokenKind::Operator(Operator::OpenCurly) {
			Some(self.parse_columns()?)
		} else {
			None
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

			// Parse statements until we hit the closing brace
			loop {
				if self.is_eof() || self.current()?.kind == TokenKind::Operator(Operator::CloseCurly) {
					break;
				}

				let node = self.parse_node(crate::ast::parse::Precedence::None)?;
				query_nodes.push(node);
			}

			self.consume_operator(Operator::CloseCurly)?;

			crate::ast::AstStatement {
				nodes: query_nodes,
				has_pipes: false,
			}
		} else {
			// Direct syntax - parse until semicolon or EOF
			let mut query_nodes = Vec::new();

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

				// Check if we've consumed everything up to a terminator
				if self.is_eof() || self.current()?.kind == TokenKind::Separator(Separator::Semicolon) {
					break;
				}
			}

			crate::ast::AstStatement {
				nodes: query_nodes,
				has_pipes: false,
			}
		};

		use crate::ast::ast::AstCreateFlow;
		Ok(AstCreate::Flow(AstCreateFlow {
			token,
			or_replace,
			if_not_exists,
			flow,
			columns,
			as_clause,
		}))
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{
		AstCreate, AstCreateDeferredView, AstCreateNamespace, AstCreateSeries, AstCreateTable,
		AstCreateTransactionalView, AstDataType, AstPolicyKind, parse::Parser, tokenize,
	};

	#[test]
	fn test_create_namespace() {
		let tokens = tokenize("CREATE NAMESPACE REIFYDB").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Namespace(AstCreateNamespace {
				namespace,
				..
			}) => {
				assert_eq!(namespace.name.text(), "REIFYDB");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_series() {
		let tokens = tokenize(
			r#"
            create series test.metrics{value: Int2}
        "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
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
				assert_eq!(sequence.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(sequence.name.text(), "metrics");

				assert_eq!(columns.len(), 1);

				assert_eq!(columns[0].name.text(), "value");
				match &columns[0].ty {
					AstDataType::Unconstrained(ident) => {
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
		let tokens = tokenize(
			r#"
        create table test.users{id: int2, name: text, is_premium: bool}
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
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
				assert_eq!(table.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(table.name.text(), "users");
				assert_eq!(columns.len(), 3);

				{
					let col = &columns[0];
					assert_eq!(col.name.text(), "id");
					match &col.ty {
						AstDataType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "int2")
						}
						_ => panic!("Expected simple type"),
					}
					assert_eq!(col.auto_increment, false);
					assert!(col.policies.is_none());
				}

				{
					let col = &columns[1];
					assert_eq!(col.name.text(), "name");
					match &col.ty {
						AstDataType::Unconstrained(ident) => {
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
						AstDataType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "bool")
						}
						_ => panic!("Expected simple type"),
					}
					assert_eq!(col.auto_increment, false);
					assert!(col.policies.is_none());
				}
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_table_with_saturation_policy() {
		let tokens = tokenize(
			r#"
        create table test.items{field: int2 policy {saturation error} }
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
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
				assert_eq!(table.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(table.name.text(), "items");

				assert_eq!(columns.len(), 1);

				let col = &columns[0];
				assert_eq!(col.name.text(), "field");
				match &col.ty {
					AstDataType::Unconstrained(ident) => {
						assert_eq!(ident.text(), "int2")
					}
					_ => panic!("Expected simple type"),
				}
				assert_eq!(col.auto_increment, false);

				let policies = &col.policies.as_ref().unwrap().policies;
				assert_eq!(policies.len(), 1);
				let policy = &policies[0];
				assert!(matches!(policy.policy, AstPolicyKind::Saturation));
				assert_eq!(policy.value.as_identifier().text(), "error");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_table_with_auto_increment() {
		let tokens = tokenize(
			r#"
        create table test.users { id: int4 AUTO INCREMENT, name: utf8 }
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
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
				assert_eq!(table.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(table.name.text(), "users");
				assert_eq!(columns.len(), 2);

				{
					let col = &columns[0];
					assert_eq!(col.name.text(), "id");
					match &col.ty {
						AstDataType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "int4")
						}
						_ => panic!("Expected simple type"),
					}
					assert_eq!(col.auto_increment, true);
					assert!(col.policies.is_none());
				}

				{
					let col = &columns[1];
					assert_eq!(col.name.text(), "name");
					match &col.ty {
						AstDataType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "utf8")
						}
						_ => panic!("Expected simple type"),
					}
					assert_eq!(col.auto_increment, false);
					assert!(col.policies.is_none());
				}
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_deferred_view() {
		let tokens = tokenize(
			r#"
        create deferred view test.views{field: int2 policy { saturation error} }
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
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
				assert_eq!(view.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(view.name.text(), "views");

				assert_eq!(columns.len(), 1);

				let col = &columns[0];
				assert_eq!(col.name.text(), "field");
				match &col.ty {
					AstDataType::Unconstrained(ident) => {
						assert_eq!(ident.text(), "int2")
					}
					_ => panic!("Expected simple type"),
				}
				assert_eq!(col.auto_increment, false);

				let policies = &col.policies.as_ref().unwrap().policies;
				assert_eq!(policies.len(), 1);
				let policy = &policies[0];
				assert!(matches!(policy.policy, AstPolicyKind::Saturation));
				assert_eq!(policy.value.as_identifier().text(), "error");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_transactional_view() {
		let tokens = tokenize(
			r#"
        create transactional view test.myview{id: int4, name: utf8}
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
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
				assert_eq!(view.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(view.name.text(), "myview");

				assert_eq!(columns.len(), 2);

				{
					let col = &columns[0];
					assert_eq!(col.name.text(), "id");
					match &col.ty {
						AstDataType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "int4")
						}
						_ => panic!("Expected simple type"),
					}
					assert_eq!(col.auto_increment, false);
					assert!(col.policies.is_none());
				}

				{
					let col = &columns[1];
					assert_eq!(col.name.text(), "name");
					match &col.ty {
						AstDataType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "utf8")
						}
						_ => panic!("Expected simple type"),
					}
					assert_eq!(col.auto_increment, false);
					assert!(col.policies.is_none());
				}
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_ring_buffer() {
		let tokens = tokenize(
			r#"
        create ring buffer test.events { id: int4, data: utf8 } with capacity = 10
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		use crate::ast::AstCreateRingBuffer;

		match create {
			AstCreate::RingBuffer(AstCreateRingBuffer {
				ring_buffer,
				columns,
				capacity,
				..
			}) => {
				assert_eq!(ring_buffer.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(ring_buffer.name.text(), "events");
				assert_eq!(*capacity, 10);
				assert_eq!(columns.len(), 2);

				{
					let col = &columns[0];
					assert_eq!(col.name.text(), "id");
					match &col.ty {
						AstDataType::Unconstrained(ident) => {
							assert_eq!(ident.text(), "int4")
						}
						_ => panic!("Expected simple type"),
					}
				}

				{
					let col = &columns[1];
					assert_eq!(col.name.text(), "data");
					match &col.ty {
						AstDataType::Unconstrained(ident) => {
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
		let tokens = tokenize(
			r#"
        create transactional view test.myview{id: int4, name: utf8} as {
            from test.users
            where age > 18
        }
    "#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
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
				assert_eq!(view.namespace.as_ref().unwrap().text(), "test");
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
		let tokens = tokenize("CREATE FLOW my_flow AS FROM orders WHERE status = 'pending'").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Flow(flow) => {
				assert!(!flow.or_replace);
				assert!(!flow.if_not_exists);
				assert_eq!(flow.flow.name.text(), "my_flow");
				assert!(flow.flow.namespace.is_none());
				assert!(flow.columns.is_none());
				assert!(flow.as_clause.len() > 0);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_flow_with_schema() {
		let tokens =
			tokenize(r#"CREATE FLOW my_flow {customer_id: int8, total: float8} AS FROM orders"#).unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Flow(flow) => {
				assert_eq!(flow.flow.name.text(), "my_flow");
				assert!(flow.columns.is_some());
				let columns = flow.columns.as_ref().unwrap();
				assert_eq!(columns.len(), 2);
				assert_eq!(columns[0].name.text(), "customer_id");
				assert_eq!(columns[1].name.text(), "total");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_flow_or_replace() {
		let tokens = tokenize("CREATE OR REPLACE FLOW my_flow AS FROM orders").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("CREATE FLOW IF NOT EXISTS my_flow AS FROM orders").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("CREATE FLOW analytics.sales_flow AS FROM sales.orders").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Flow(flow) => {
				assert_eq!(flow.flow.namespace.as_ref().unwrap().text(), "analytics");
				assert_eq!(flow.flow.name.text(), "sales_flow");
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_flow_complex_query() {
		let tokens = tokenize(
			r#"
			CREATE FLOW aggregated AS {
				FROM raw_events
				WHERE event_type = 'purchase'
				AGGREGATE BY user_id
				SELECT { user_id, SUM(amount) as total }
			}
		"#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Flow(flow) => {
				assert_eq!(flow.flow.name.text(), "aggregated");
				// The AS clause should have multiple query nodes
				assert!(flow.as_clause.len() >= 4); // FROM, WHERE, AGGREGATE, SELECT
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_create_or_replace_flow_if_not_exists() {
		let tokens = tokenize("CREATE OR REPLACE FLOW IF NOT EXISTS test.my_flow AS FROM orders").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let create = result.first_unchecked().as_create();

		match create {
			AstCreate::Flow(flow) => {
				assert!(flow.or_replace);
				assert!(flow.if_not_exists);
				assert_eq!(flow.flow.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(flow.flow.name.text(), "my_flow");
			}
			_ => unreachable!(),
		}
	}
}
