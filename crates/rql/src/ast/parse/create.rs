// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use Keyword::{Create, Namespace};
use Operator::Colon;
use reifydb_core::interface::identifier::SourceKind;

use crate::ast::{
	AstColumnToCreate, AstCreate, AstCreateDeferredView,
	AstCreateNamespace, AstCreateSeries, AstCreateTable,
	AstCreateTransactionalView, AstDataType,
	identifier::{
		MaybeQualifiedNamespaceIdentifier,
		MaybeQualifiedSequenceIdentifier,
		MaybeQualifiedSourceIdentifier,
	},
	parse::Parser,
	tokenize::{
		Keyword,
		Keyword::{Deferred, Series, Table, Transactional, View},
		Operator,
		Separator::Comma,
		Token, TokenKind,
	},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_create(&mut self) -> crate::Result<AstCreate<'a>> {
		let token = self.consume_keyword(Create)?;

		if (self.consume_if(TokenKind::Keyword(Namespace))?).is_some() {
			return self.parse_namespace(token);
		}

		if (self.consume_if(TokenKind::Keyword(Deferred))?).is_some() {
			if (self.consume_if(TokenKind::Keyword(View))?)
				.is_some()
			{
				return self.parse_deferred_view(token);
			}
			unimplemented!()
		}

		if (self.consume_if(TokenKind::Keyword(Transactional))?)
			.is_some()
		{
			if (self.consume_if(TokenKind::Keyword(View))?)
				.is_some()
			{
				return self.parse_transactional_view(token);
			}
			unimplemented!()
		}

		if (self.consume_if(TokenKind::Keyword(Table))?).is_some() {
			return self.parse_table(token);
		}

		if (self.consume_if(TokenKind::Keyword(Series))?).is_some() {
			return self.parse_series(token);
		}

		if self.peek_is_index_creation()? {
			return self.parse_create_index(token);
		}

		unimplemented!();
	}

	fn parse_namespace(
		&mut self,
		token: Token<'a>,
	) -> crate::Result<AstCreate<'a>> {
		let name_token = self
			.consume(crate::ast::tokenize::TokenKind::Identifier)?;
		let namespace = MaybeQualifiedNamespaceIdentifier::new(
			name_token.fragment.clone(),
		);
		Ok(AstCreate::Namespace(AstCreateNamespace {
			token,
			namespace,
		}))
	}

	fn parse_series(
		&mut self,
		token: Token<'a>,
	) -> crate::Result<AstCreate<'a>> {
		let schema_token = self.consume(TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let name_token = self.consume(TokenKind::Identifier)?;
		let columns = self.parse_columns()?;

		let sequence = MaybeQualifiedSequenceIdentifier::new(
			name_token.fragment.clone(),
		)
		.with_namespace(schema_token.fragment.clone());

		Ok(AstCreate::Series(AstCreateSeries {
			token,
			sequence,
			columns,
		}))
	}

	fn parse_deferred_view(
		&mut self,
		token: Token<'a>,
	) -> crate::Result<AstCreate<'a>> {
		let schema_token = self.consume(TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let name_token = self.consume(TokenKind::Identifier)?;
		let columns = self.parse_columns()?;

		let view = MaybeQualifiedSourceIdentifier::new(
			name_token.fragment.clone(),
		)
		.with_namespace(schema_token.fragment.clone())
		.with_kind(SourceKind::DeferredView);

		// Parse optional AS clause
		let as_clause = if self
			.consume_if(TokenKind::Operator(Operator::As))?
			.is_some()
		{
			// Expect opening curly brace
			self.consume_operator(Operator::OpenCurly)?;

			// Parse the query nodes inside the AS clause
			let mut query_nodes = Vec::new();

			// Parse statements until we hit the closing brace
			loop {
				if self.is_eof()
					|| self.current()?.kind
						== TokenKind::Operator(
							Operator::CloseCurly,
						) {
					break;
				}

				let node = self.parse_node(
					crate::ast::parse::Precedence::None,
				)?;
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

	fn parse_transactional_view(
		&mut self,
		token: Token<'a>,
	) -> crate::Result<AstCreate<'a>> {
		let schema_token = self.consume(TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let name_token = self.consume(TokenKind::Identifier)?;
		let columns = self.parse_columns()?;

		// Create MaybeQualifiedSourceIdentifier for transactional view
		let view = MaybeQualifiedSourceIdentifier::new(
			name_token.fragment.clone(),
		)
		.with_namespace(schema_token.fragment.clone())
		.with_kind(SourceKind::TransactionalView);

		// Parse optional AS clause
		let as_clause = if self
			.consume_if(TokenKind::Operator(Operator::As))?
			.is_some()
		{
			// Expect opening curly brace
			self.consume_operator(Operator::OpenCurly)?;

			// Parse the query nodes inside the AS clause
			let mut query_nodes = Vec::new();

			// Parse statements until we hit the closing brace
			loop {
				if self.is_eof()
					|| self.current()?.kind
						== TokenKind::Operator(
							Operator::CloseCurly,
						) {
					break;
				}

				let node = self.parse_node(
					crate::ast::parse::Precedence::None,
				)?;
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

	fn parse_table(
		&mut self,
		token: Token<'a>,
	) -> crate::Result<AstCreate<'a>> {
		let schema_token = self.consume(TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let name_token = self.advance()?;
		let columns = self.parse_columns()?;

		let table = MaybeQualifiedSourceIdentifier::new(
			name_token.fragment.clone(),
		)
		.with_namespace(schema_token.fragment.clone())
		.with_kind(SourceKind::Table);

		Ok(AstCreate::Table(AstCreateTable {
			token,
			table,
			columns,
		}))
	}

	fn parse_columns(
		&mut self,
	) -> crate::Result<Vec<AstColumnToCreate<'a>>> {
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

			if self.consume_if(TokenKind::Separator(Comma))?
				.is_none()
			{
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
			while self
				.consume_if(TokenKind::Separator(Comma))?
				.is_some()
			{
				params.push(self.parse_literal_number()?);
			}

			self.consume_operator(Operator::CloseParen)?;

			AstDataType::WithConstraints {
				name: ty_token.fragment,
				params,
			}
		} else {
			// Simple type without parameters
			AstDataType::Simple(ty_token.fragment)
		};

		let auto_increment =
			if self.current()?.is_keyword(Keyword::Auto) {
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
}

#[cfg(test)]
mod tests {
	use crate::ast::{
		AstCreate, AstCreateDeferredView, AstCreateNamespace,
		AstCreateSeries, AstCreateTable, AstCreateTransactionalView,
		AstDataType, AstPolicyKind, parse::Parser, tokenize,
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
				assert_eq!(
					sequence.namespace
						.as_ref()
						.unwrap()
						.text(),
					"test"
				);
				assert_eq!(sequence.name.text(), "metrics");

				assert_eq!(columns.len(), 1);

				assert_eq!(columns[0].name.text(), "value");
				match &columns[0].ty {
					AstDataType::Simple(ident) => {
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
				assert_eq!(
					table.namespace
						.as_ref()
						.unwrap()
						.text(),
					"test"
				);
				assert_eq!(table.name.text(), "users");
				assert_eq!(columns.len(), 3);

				{
					let col = &columns[0];
					assert_eq!(col.name.text(), "id");
					match &col.ty {
						AstDataType::Simple(ident) => {
							assert_eq!(
								ident.text(),
								"int2"
							)
						}
						_ => panic!(
							"Expected simple type"
						),
					}
					assert_eq!(col.auto_increment, false);
					assert!(col.policies.is_none());
				}

				{
					let col = &columns[1];
					assert_eq!(col.name.text(), "name");
					match &col.ty {
						AstDataType::Simple(ident) => {
							assert_eq!(
								ident.text(),
								"text"
							)
						}
						_ => panic!(
							"Expected simple type"
						),
					}
					assert_eq!(col.auto_increment, false);
				}

				{
					let col = &columns[2];
					assert_eq!(
						col.name.text(),
						"is_premium"
					);
					match &col.ty {
						AstDataType::Simple(ident) => {
							assert_eq!(
								ident.text(),
								"bool"
							)
						}
						_ => panic!(
							"Expected simple type"
						),
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
				assert_eq!(
					table.namespace
						.as_ref()
						.unwrap()
						.text(),
					"test"
				);
				assert_eq!(table.name.text(), "items");

				assert_eq!(columns.len(), 1);

				let col = &columns[0];
				assert_eq!(col.name.text(), "field");
				match &col.ty {
					AstDataType::Simple(ident) => {
						assert_eq!(ident.text(), "int2")
					}
					_ => panic!("Expected simple type"),
				}
				assert_eq!(col.auto_increment, false);

				let policies = &col
					.policies
					.as_ref()
					.unwrap()
					.policies;
				assert_eq!(policies.len(), 1);
				let policy = &policies[0];
				assert!(matches!(
					policy.policy,
					AstPolicyKind::Saturation
				));
				assert_eq!(
					policy.value.as_identifier().text(),
					"error"
				);
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
				assert_eq!(
					table.namespace
						.as_ref()
						.unwrap()
						.text(),
					"test"
				);
				assert_eq!(table.name.text(), "users");
				assert_eq!(columns.len(), 2);

				{
					let col = &columns[0];
					assert_eq!(col.name.text(), "id");
					match &col.ty {
						AstDataType::Simple(ident) => {
							assert_eq!(
								ident.text(),
								"int4"
							)
						}
						_ => panic!(
							"Expected simple type"
						),
					}
					assert_eq!(col.auto_increment, true);
					assert!(col.policies.is_none());
				}

				{
					let col = &columns[1];
					assert_eq!(col.name.text(), "name");
					match &col.ty {
						AstDataType::Simple(ident) => {
							assert_eq!(
								ident.text(),
								"utf8"
							)
						}
						_ => panic!(
							"Expected simple type"
						),
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
				assert_eq!(
					view.namespace.as_ref().unwrap().text(),
					"test"
				);
				assert_eq!(view.name.text(), "views");

				assert_eq!(columns.len(), 1);

				let col = &columns[0];
				assert_eq!(col.name.text(), "field");
				match &col.ty {
					AstDataType::Simple(ident) => {
						assert_eq!(ident.text(), "int2")
					}
					_ => panic!("Expected simple type"),
				}
				assert_eq!(col.auto_increment, false);

				let policies = &col
					.policies
					.as_ref()
					.unwrap()
					.policies;
				assert_eq!(policies.len(), 1);
				let policy = &policies[0];
				assert!(matches!(
					policy.policy,
					AstPolicyKind::Saturation
				));
				assert_eq!(
					policy.value.as_identifier().text(),
					"error"
				);
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
			AstCreate::TransactionalView(
				AstCreateTransactionalView {
					view,
					columns,
					..
				},
			) => {
				assert_eq!(
					view.namespace.as_ref().unwrap().text(),
					"test"
				);
				assert_eq!(view.name.text(), "myview");

				assert_eq!(columns.len(), 2);

				{
					let col = &columns[0];
					assert_eq!(col.name.text(), "id");
					match &col.ty {
						AstDataType::Simple(ident) => {
							assert_eq!(
								ident.text(),
								"int4"
							)
						}
						_ => panic!(
							"Expected simple type"
						),
					}
					assert_eq!(col.auto_increment, false);
					assert!(col.policies.is_none());
				}

				{
					let col = &columns[1];
					assert_eq!(col.name.text(), "name");
					match &col.ty {
						AstDataType::Simple(ident) => {
							assert_eq!(
								ident.text(),
								"utf8"
							)
						}
						_ => panic!(
							"Expected simple type"
						),
					}
					assert_eq!(col.auto_increment, false);
					assert!(col.policies.is_none());
				}
			}
			_ => unreachable!(),
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
			AstCreate::TransactionalView(
				AstCreateTransactionalView {
					view,
					columns,
					as_clause,
					..
				},
			) => {
				assert_eq!(
					view.namespace.as_ref().unwrap().text(),
					"test"
				);
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
}
