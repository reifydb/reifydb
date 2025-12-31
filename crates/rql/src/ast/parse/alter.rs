// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::SortDirection;

use crate::ast::{
	AstAlter, AstAlterFlow, AstAlterFlowAction, AstAlterSequence, AstAlterTable, AstAlterTableOperation,
	AstAlterView, AstAlterViewOperation, AstIndexColumn, AstStatement,
	identifier::MaybeQualifiedFlowIdentifier,
	parse::Parser,
	tokenize::{Keyword, Operator, Separator, Token, TokenKind},
};

impl Parser {
	pub(crate) fn parse_alter(&mut self) -> crate::Result<AstAlter> {
		let token = self.consume_keyword(Keyword::Alter)?;

		if self.current()?.is_keyword(Keyword::Sequence) {
			self.consume_keyword(Keyword::Sequence)?;
			return self.parse_alter_sequence(token);
		}

		if self.current()?.is_keyword(Keyword::Table) {
			self.consume_keyword(Keyword::Table)?;
			return self.parse_alter_table(token);
		}

		if self.current()?.is_keyword(Keyword::View) {
			self.consume_keyword(Keyword::View)?;
			return self.parse_alter_view(token);
		}

		if self.current()?.is_keyword(Keyword::Flow) {
			self.consume_keyword(Keyword::Flow)?;
			return self.parse_alter_flow(token);
		}

		unimplemented!("Only ALTER SEQUENCE, ALTER TABLE, ALTER VIEW, and ALTER FLOW are supported");
	}

	fn parse_alter_sequence(&mut self, token: Token) -> crate::Result<AstAlter> {
		// Parse namespace.table.column or table.column
		let first_identifier_token = self.consume(crate::ast::tokenize::TokenKind::Identifier)?;

		if self.current()?.is_operator(Operator::Dot) {
			self.consume_operator(Operator::Dot)?;
			let second_identifier_token = self.consume(crate::ast::tokenize::TokenKind::Identifier)?;

			if self.current()?.is_operator(Operator::Dot) {
				self.consume_operator(Operator::Dot)?;
				let column_token = self.consume(crate::ast::tokenize::TokenKind::Identifier)?;

				// Expect SET VALUE <number>
				self.consume_keyword(Keyword::Set)?;
				self.consume_keyword(Keyword::Value)?;
				let value_token = self.consume(crate::ast::tokenize::TokenKind::Literal(
					crate::ast::tokenize::Literal::Number,
				))?;

				// Create MaybeQualifiedSequenceIdentifier with
				// namespace
				use crate::ast::identifier::MaybeQualifiedSequenceIdentifier;
				let sequence =
					MaybeQualifiedSequenceIdentifier::new(second_identifier_token.fragment.clone())
						.with_namespace(first_identifier_token.fragment.clone());

				let column = column_token.fragment;
				let value =
					crate::ast::AstLiteral::Number(crate::ast::ast::AstLiteralNumber(value_token));

				Ok(AstAlter::Sequence(AstAlterSequence {
					token,
					sequence,
					column,
					value,
				}))
			} else {
				// table.column
				self.consume_keyword(Keyword::Set)?;
				self.consume_keyword(Keyword::Value)?;
				let value_token = self.consume(crate::ast::tokenize::TokenKind::Literal(
					crate::ast::tokenize::Literal::Number,
				))?;

				// Create MaybeQualifiedSequenceIdentifier
				// without namespace
				use crate::ast::identifier::MaybeQualifiedSequenceIdentifier;
				let sequence =
					MaybeQualifiedSequenceIdentifier::new(first_identifier_token.fragment.clone());

				let column = second_identifier_token.fragment;
				let value =
					crate::ast::AstLiteral::Number(crate::ast::ast::AstLiteralNumber(value_token));

				Ok(AstAlter::Sequence(AstAlterSequence {
					token,
					sequence,
					column,
					value,
				}))
			}
		} else {
			unimplemented!("ALTER SEQUENCE requires table.column or namespace.table.column");
		}
	}

	fn parse_alter_table(&mut self, token: Token) -> crate::Result<AstAlter> {
		// Parse namespace.table
		let namespace_token = self.consume(crate::ast::tokenize::TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let table_token = self.consume(crate::ast::tokenize::TokenKind::Identifier)?;

		// Create MaybeQualifiedTableIdentifier
		use crate::ast::identifier::MaybeQualifiedTableIdentifier;
		let table = MaybeQualifiedTableIdentifier::new(table_token.fragment.clone())
			.with_namespace(namespace_token.fragment.clone());

		// Parse block of operations
		self.consume_operator(Operator::OpenCurly)?;

		let mut operations = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			// Parse operation
			if self.current()?.is_keyword(Keyword::Create) {
				self.consume_keyword(Keyword::Create)?;
				self.consume_keyword(Keyword::Primary)?;
				self.consume_keyword(Keyword::Key)?;

				// Check for optional name
				let name = if !self.current()?.is_operator(Operator::OpenCurly) {
					Some(self.parse_identifier()?.token.fragment)
				} else {
					None
				};

				// Parse columns
				let columns = self.parse_primary_key_columns()?;

				operations.push(AstAlterTableOperation::CreatePrimaryKey {
					name,
					columns,
				});
			} else if self.current()?.is_keyword(Keyword::Drop) {
				self.consume_keyword(Keyword::Drop)?;
				self.consume_keyword(Keyword::Primary)?;
				self.consume_keyword(Keyword::Key)?;

				operations.push(AstAlterTableOperation::DropPrimaryKey);
			} else {
				unimplemented!("Unsupported ALTER TABLE operation");
			}

			self.skip_new_line()?;

			// Check for comma separator for multiple operations
			if self.consume_if(TokenKind::Separator(Separator::Comma))?.is_some() {
				continue;
			}

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstAlter::Table(AstAlterTable {
			token,
			table,
			operations,
		}))
	}

	fn parse_alter_view(&mut self, token: Token) -> crate::Result<AstAlter> {
		// Parse namespace.view
		let namespace_token = self.consume(crate::ast::tokenize::TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let view_token = self.consume(crate::ast::tokenize::TokenKind::Identifier)?;

		// Create MaybeQualifiedViewIdentifier for view
		use crate::ast::identifier::MaybeQualifiedViewIdentifier;
		let view = MaybeQualifiedViewIdentifier::new(view_token.fragment.clone())
			.with_namespace(namespace_token.fragment.clone());

		// Parse block of operations
		self.consume_operator(Operator::OpenCurly)?;

		let mut operations = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			// Parse operation
			if self.current()?.is_keyword(Keyword::Create) {
				self.consume_keyword(Keyword::Create)?;
				self.consume_keyword(Keyword::Primary)?;
				self.consume_keyword(Keyword::Key)?;

				// Check for optional name
				let name = if !self.current()?.is_operator(Operator::OpenCurly) {
					Some(self.parse_identifier()?.token.fragment)
				} else {
					None
				};

				// Parse columns
				let columns = self.parse_primary_key_columns()?;

				operations.push(AstAlterViewOperation::CreatePrimaryKey {
					name,
					columns,
				});
			} else if self.current()?.is_keyword(Keyword::Drop) {
				self.consume_keyword(Keyword::Drop)?;
				self.consume_keyword(Keyword::Primary)?;
				self.consume_keyword(Keyword::Key)?;

				operations.push(AstAlterViewOperation::DropPrimaryKey);
			} else {
				unimplemented!("Unsupported ALTER VIEW operation");
			}

			self.skip_new_line()?;

			// Check for comma separator for multiple operations
			if self.consume_if(TokenKind::Separator(Separator::Comma))?.is_some() {
				continue;
			}

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstAlter::View(AstAlterView {
			token,
			view,
			operations,
		}))
	}

	fn parse_primary_key_columns(&mut self) -> crate::Result<Vec<AstIndexColumn>> {
		let mut columns = Vec::new();

		self.consume_operator(Operator::OpenCurly)?;

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let column = self.parse_column_identifier()?;

			// Check for optional sort direction
			let sort_direction = if self.current()?.is_operator(Operator::Colon) {
				self.consume_operator(Operator::Colon)?;

				if self.current()?.is_keyword(Keyword::Asc) {
					self.consume_keyword(Keyword::Asc)?;
					SortDirection::Asc
				} else if self.current()?.is_keyword(Keyword::Desc) {
					self.consume_keyword(Keyword::Desc)?;
					SortDirection::Desc
				} else {
					SortDirection::Asc
				}
			} else {
				SortDirection::Asc
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
			unimplemented!("Primary key must have at least one column");
		}

		Ok(columns)
	}

	fn parse_alter_flow(&mut self, token: Token) -> crate::Result<AstAlter> {
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

		// Parse the action
		let action = if self.current()?.is_keyword(Keyword::Rename) {
			self.consume_keyword(Keyword::Rename)?;
			self.consume_keyword(Keyword::To)?;
			let new_name = self.consume(TokenKind::Identifier)?;
			AstAlterFlowAction::Rename {
				new_name: new_name.fragment,
			}
		} else if self.current()?.is_keyword(Keyword::Set) {
			self.consume_keyword(Keyword::Set)?;
			self.consume_keyword(Keyword::Query)?;
			self.consume_operator(Operator::As)?;

			// Parse the new query
			let query = if self.current()?.kind == TokenKind::Operator(Operator::OpenCurly) {
				// Curly brace syntax
				self.consume_operator(Operator::OpenCurly)?;

				let mut query_nodes = Vec::new();

				// Parse statements until we hit the closing brace
				loop {
					if self.is_eof()
						|| self.current()?.kind == TokenKind::Operator(Operator::CloseCurly)
					{
						break;
					}

					let node = self.parse_node(crate::ast::parse::Precedence::None)?;
					query_nodes.push(node);
				}

				self.consume_operator(Operator::CloseCurly)?;

				AstStatement {
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
					if self.is_eof()
						|| self.current()?.kind == TokenKind::Separator(Separator::Semicolon)
					{
						break;
					}
				}

				AstStatement {
					nodes: query_nodes,
					has_pipes: false,
				}
			};

			AstAlterFlowAction::SetQuery {
				query,
			}
		} else if self.current()?.is_keyword(Keyword::Pause) {
			self.consume_keyword(Keyword::Pause)?;
			AstAlterFlowAction::Pause
		} else if self.current()?.is_keyword(Keyword::Resume) {
			self.consume_keyword(Keyword::Resume)?;
			AstAlterFlowAction::Resume
		} else {
			return Err(reifydb_type::Error(reifydb_type::diagnostic::ast::unexpected_token_error(
				"RENAME, SET, PAUSE, or RESUME",
				self.current()?.fragment.clone(),
			)));
		};

		Ok(AstAlter::Flow(AstAlterFlow {
			token,
			flow,
			action,
		}))
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{
		AstAlter, AstAlterFlowAction, AstAlterSequence, AstAlterTable, AstAlterTableOperation, AstAlterView,
		AstAlterViewOperation, parse::Parser, tokenize::tokenize,
	};

	#[test]
	fn test_alter_sequence_with_schema() {
		let tokens = tokenize("ALTER SEQUENCE test.users.id SET VALUE 1000").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Sequence(AstAlterSequence {
				sequence,
				column,
				value,
				..
			}) => {
				assert!(sequence.namespace.is_some());
				assert_eq!(sequence.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(sequence.name.text(), "users");
				assert_eq!(column.text(), "id");
				match value {
					crate::ast::AstLiteral::Number(num) => {
						assert_eq!(num.value(), "1000")
					}
					_ => panic!("Expected number literal"),
				}
			}
			_ => panic!("Expected AstAlter::Sequence"),
		}
	}

	#[test]
	fn test_alter_sequence_without_schema() {
		let tokens = tokenize("ALTER SEQUENCE users.id SET VALUE 500").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Sequence(AstAlterSequence {
				sequence,
				column,
				value,
				..
			}) => {
				assert!(sequence.namespace.is_none());
				assert_eq!(sequence.name.text(), "users");
				assert_eq!(column.text(), "id");
				match value {
					crate::ast::AstLiteral::Number(num) => {
						assert_eq!(num.value(), "500")
					}
					_ => panic!("Expected number literal"),
				}
			}
			_ => panic!("Expected AstAlter::Sequence"),
		}
	}

	#[test]
	fn test_alter_table_create_primary_key() {
		let tokens = tokenize("ALTER TABLE test.users { create primary key pk_users {id} }").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Table(AstAlterTable {
				table,
				operations,
				..
			}) => {
				assert!(table.namespace.is_some());
				assert_eq!(table.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(table.name.text(), "users");
				assert_eq!(operations.len(), 1);

				match &operations[0] {
					AstAlterTableOperation::CreatePrimaryKey {
						name,
						columns,
					} => {
						assert!(name.is_some());
						assert_eq!(name.as_ref().unwrap().text(), "pk_users");
						assert_eq!(columns.len(), 1);
						assert_eq!(columns[0].column.name.text(), "id");
					}
					_ => panic!("Expected CreatePrimaryKey operation"),
				}
			}
			_ => panic!("Expected AstAlter::Table"),
		}
	}

	#[test]
	fn test_alter_table_create_primary_key_no_name() {
		let tokens = tokenize("ALTER TABLE test.users { create primary key {id, email} }").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Table(AstAlterTable {
				table,
				operations,
				..
			}) => {
				assert!(table.namespace.is_some());
				assert_eq!(table.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(table.name.text(), "users");
				assert_eq!(operations.len(), 1);

				match &operations[0] {
					AstAlterTableOperation::CreatePrimaryKey {
						name,
						columns,
					} => {
						assert!(name.is_none());
						assert_eq!(columns.len(), 2);
						assert_eq!(columns[0].column.name.text(), "id");
						assert_eq!(columns[1].column.name.text(), "email");
					}
					_ => panic!("Expected CreatePrimaryKey operation"),
				}
			}
			_ => panic!("Expected AstAlter::Table"),
		}
	}

	#[test]
	fn test_alter_view_create_primary_key() {
		let tokens = tokenize("ALTER VIEW test.user_view { create primary key pk_view {user_id} }").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::View(AstAlterView {
				view,
				operations,
				..
			}) => {
				assert!(view.namespace.is_some());
				assert_eq!(view.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(view.name.text(), "user_view");
				assert_eq!(operations.len(), 1);

				match &operations[0] {
					AstAlterViewOperation::CreatePrimaryKey {
						name,
						columns,
					} => {
						assert!(name.is_some());
						assert_eq!(name.as_ref().unwrap().text(), "pk_view");
						assert_eq!(columns.len(), 1);
						assert_eq!(columns[0].column.name.text(), "user_id");
					}
					_ => panic!("Expected CreatePrimaryKey operation"),
				}
			}
			_ => panic!("Expected AstAlter::View"),
		}
	}

	#[test]
	fn test_alter_view_create_primary_key_no_name() {
		let tokens =
			tokenize("ALTER VIEW test.user_view { create primary key {user_id, created_at} }").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::View(AstAlterView {
				view,
				operations,
				..
			}) => {
				assert!(view.namespace.is_some());
				assert_eq!(view.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(view.name.text(), "user_view");
				assert_eq!(operations.len(), 1);

				match &operations[0] {
					AstAlterViewOperation::CreatePrimaryKey {
						name,
						columns,
					} => {
						assert!(name.is_none());
						assert_eq!(columns.len(), 2);
						assert_eq!(columns[0].column.name.text(), "user_id");
						assert_eq!(columns[1].column.name.text(), "created_at");
					}
					_ => panic!("Expected CreatePrimaryKey operation"),
				}
			}
			_ => panic!("Expected AstAlter::View"),
		}
	}

	#[test]
	fn test_alter_flow_rename() {
		let tokens = tokenize("ALTER FLOW old_flow RENAME TO new_flow").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Flow(flow) => {
				assert_eq!(flow.flow.name.text(), "old_flow");
				assert!(flow.flow.namespace.is_none());

				match &flow.action {
					AstAlterFlowAction::Rename {
						new_name,
					} => {
						assert_eq!(new_name.text(), "new_flow");
					}
					_ => panic!("Expected Rename action"),
				}
			}
			_ => panic!("Expected AstAlter::Flow"),
		}
	}

	#[test]
	fn test_alter_flow_rename_qualified() {
		let tokens = tokenize("ALTER FLOW test.old_flow RENAME TO new_flow").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Flow(flow) => {
				assert_eq!(flow.flow.namespace.as_ref().unwrap().text(), "test");
				assert_eq!(flow.flow.name.text(), "old_flow");

				match &flow.action {
					AstAlterFlowAction::Rename {
						new_name,
					} => {
						assert_eq!(new_name.text(), "new_flow");
					}
					_ => panic!("Expected Rename action"),
				}
			}
			_ => panic!("Expected AstAlter::Flow"),
		}
	}

	#[test]
	fn test_alter_flow_set_query() {
		let tokens = tokenize("ALTER FLOW my_flow SET QUERY AS FROM new_source FILTER active = true").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Flow(flow) => {
				assert_eq!(flow.flow.name.text(), "my_flow");

				match &flow.action {
					AstAlterFlowAction::SetQuery {
						query,
					} => {
						assert!(query.len() > 0);
						// Should have FROM and WHERE nodes
					}
					_ => panic!("Expected SetQuery action"),
				}
			}
			_ => panic!("Expected AstAlter::Flow"),
		}
	}

	#[test]
	fn test_alter_flow_set_query_with_braces() {
		let tokens = tokenize(
			r#"
			ALTER FLOW my_flow SET QUERY AS {
				FROM new_source
				FILTER active = true
				AGGREGATE {total: count(*) } BY category
			}
		"#,
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Flow(flow) => {
				assert_eq!(flow.flow.name.text(), "my_flow");

				match &flow.action {
					AstAlterFlowAction::SetQuery {
						query,
					} => {
						assert!(query.len() >= 3); // FROM, FILTER, AGGREGATE
					}
					_ => panic!("Expected SetQuery action"),
				}
			}
			_ => panic!("Expected AstAlter::Flow"),
		}
	}

	#[test]
	fn test_alter_flow_pause() {
		let tokens = tokenize("ALTER FLOW my_flow PAUSE").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Flow(flow) => {
				assert_eq!(flow.flow.name.text(), "my_flow");

				match &flow.action {
					AstAlterFlowAction::Pause => {
						// Pause action has no additional data
					}
					_ => panic!("Expected Pause action"),
				}
			}
			_ => panic!("Expected AstAlter::Flow"),
		}
	}

	#[test]
	fn test_alter_flow_resume() {
		let tokens = tokenize("ALTER FLOW analytics.my_flow RESUME").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Flow(flow) => {
				assert_eq!(flow.flow.namespace.as_ref().unwrap().text(), "analytics");
				assert_eq!(flow.flow.name.text(), "my_flow");

				match &flow.action {
					AstAlterFlowAction::Resume => {
						// Resume action has no additional data
					}
					_ => panic!("Expected Resume action"),
				}
			}
			_ => panic!("Expected AstAlter::Flow"),
		}
	}
}
