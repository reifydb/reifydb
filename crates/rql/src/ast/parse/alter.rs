// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::{AstErrorKind, Error, TypeError};

use crate::{
	Result,
	ast::{
		ast::{
			AstAlter, AstAlterFlow, AstAlterFlowAction, AstAlterSequence, AstAlterTable,
			AstAlterTableAction, AstLiteral, AstLiteralNumber, AstPolicyTargetType, AstStatement,
		},
		identifier::{
			MaybeQualifiedFlowIdentifier, MaybeQualifiedSequenceIdentifier, MaybeQualifiedTableIdentifier,
		},
		parse::{Parser, Precedence},
	},
	token::{
		keyword::Keyword,
		operator::Operator,
		separator::Separator,
		token::{Literal, Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_alter(&mut self) -> Result<AstAlter<'bump>> {
		let token = self.consume_keyword(Keyword::Alter)?;

		if self.current()?.is_keyword(Keyword::Sequence) {
			self.consume_keyword(Keyword::Sequence)?;
			return self.parse_alter_sequence(token);
		}

		if self.current()?.is_keyword(Keyword::Flow) {
			self.consume_keyword(Keyword::Flow)?;
			if (self.consume_if(TokenKind::Keyword(Keyword::Policy))?).is_some() {
				return self.parse_alter_policy(token, AstPolicyTargetType::Flow);
			}
			return self.parse_alter_flow(token);
		}

		if self.current()?.is_keyword(Keyword::Table) {
			self.consume_keyword(Keyword::Table)?;
			if self.current()?.is_keyword(Keyword::Policy) {
				self.consume_keyword(Keyword::Policy)?;
				return self.parse_alter_policy(token, AstPolicyTargetType::Table);
			}
			return self.parse_alter_table(token);
		}

		if self.current()?.is_keyword(Keyword::View) {
			self.consume_keyword(Keyword::View)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::View);
		}

		if self.current()?.is_keyword(Keyword::Ringbuffer) {
			self.consume_keyword(Keyword::Ringbuffer)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::RingBuffer);
		}

		if self.current()?.is_keyword(Keyword::Namespace) {
			self.consume_keyword(Keyword::Namespace)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Namespace);
		}

		if self.current()?.is_keyword(Keyword::Procedure) {
			self.consume_keyword(Keyword::Procedure)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Procedure);
		}

		if self.current()?.is_keyword(Keyword::Function) {
			self.consume_keyword(Keyword::Function)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Function);
		}

		if self.current()?.is_keyword(Keyword::Session) {
			self.consume_keyword(Keyword::Session)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Session);
		}

		if self.current()?.is_keyword(Keyword::Series) {
			self.consume_keyword(Keyword::Series)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Series);
		}

		if self.current()?.is_keyword(Keyword::Dictionary) {
			self.consume_keyword(Keyword::Dictionary)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Dictionary);
		}

		if self.current()?.is_keyword(Keyword::Subscription) {
			self.consume_keyword(Keyword::Subscription)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Subscription);
		}

		if self.current()?.is_keyword(Keyword::Feature) {
			self.consume_keyword(Keyword::Feature)?;
			self.consume_keyword(Keyword::Policy)?;
			return self.parse_alter_policy(token, AstPolicyTargetType::Feature);
		}

		unimplemented!("Only ALTER SEQUENCE, ALTER FLOW, ALTER TABLE, and ALTER <TYPE> POLICY are supported");
	}

	fn parse_alter_sequence(&mut self, token: Token<'bump>) -> Result<AstAlter<'bump>> {
		// Parse [namespace...].table.column (at least 2 segments required)
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		if segments.len() < 2 {
			unimplemented!("ALTER SEQUENCE requires table.column or namespace.table.column");
		}

		let column_token = segments.pop().unwrap();
		let table_token = segments.pop().unwrap();

		let sequence = if segments.is_empty() {
			MaybeQualifiedSequenceIdentifier::new(table_token.into_fragment())
		} else {
			let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
			MaybeQualifiedSequenceIdentifier::new(table_token.into_fragment()).with_namespace(namespace)
		};

		self.consume_keyword(Keyword::Set)?;
		self.consume_keyword(Keyword::Value)?;
		let value_token = self.consume(TokenKind::Literal(Literal::Number))?;

		let column = column_token.into_fragment();
		let value = AstLiteral::Number(AstLiteralNumber(value_token));

		Ok(AstAlter::Sequence(AstAlterSequence {
			token,
			sequence,
			column,
			value,
		}))
	}

	fn parse_alter_table(&mut self, token: Token<'bump>) -> Result<AstAlter<'bump>> {
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let table = MaybeQualifiedTableIdentifier::new(name).with_namespace(namespace);

		let action = if self.current()?.is_keyword(Keyword::Add) {
			self.consume_keyword(Keyword::Add)?;
			self.consume_keyword(Keyword::Column)?;
			let column = self.parse_column()?;
			AstAlterTableAction::AddColumn {
				column,
			}
		} else if self.current()?.is_keyword(Keyword::Drop) {
			self.consume_keyword(Keyword::Drop)?;
			self.consume_keyword(Keyword::Column)?;
			let col_name = self.consume(TokenKind::Identifier)?;
			AstAlterTableAction::DropColumn {
				column: col_name.fragment,
			}
		} else if self.current()?.is_keyword(Keyword::Rename) {
			self.consume_keyword(Keyword::Rename)?;
			self.consume_keyword(Keyword::Column)?;
			let old_name = self.consume(TokenKind::Identifier)?;
			self.consume_keyword(Keyword::To)?;
			let new_name = self.consume(TokenKind::Identifier)?;
			AstAlterTableAction::RenameColumn {
				old_name: old_name.fragment,
				new_name: new_name.fragment,
			}
		} else {
			let fragment = self.current()?.fragment.to_owned();
			return Err(Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "ADD, DROP, or RENAME".to_string(),
				},
				message: format!(
					"Unexpected token: expected {}, got {}",
					"ADD COLUMN, DROP COLUMN, or RENAME COLUMN",
					fragment.text()
				),
				fragment,
			}));
		};

		Ok(AstAlter::Table(AstAlterTable {
			token,
			table,
			action,
		}))
	}

	fn parse_alter_flow(&mut self, token: Token<'bump>) -> Result<AstAlter<'bump>> {
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let flow = if namespace.is_empty() {
			MaybeQualifiedFlowIdentifier::new(name)
		} else {
			MaybeQualifiedFlowIdentifier::new(name).with_namespace(namespace)
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

					let node = self.parse_node(Precedence::None)?;
					query_nodes.push(node);
				}

				self.consume_operator(Operator::CloseCurly)?;

				AstStatement {
					nodes: query_nodes,
					has_pipes: false,
					is_output: false,
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

					let node = self.parse_node(Precedence::None)?;
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
					is_output: false,
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
			let fragment = self.current()?.fragment.to_owned();
			return Err(Error::from(TypeError::Ast {
				kind: AstErrorKind::UnexpectedToken {
					expected: "RENAME, SET, PAUSE, or RESUME".to_string(),
				},
				message: format!(
					"Unexpected token: expected {}, got {}",
					"RENAME, SET, PAUSE, or RESUME",
					fragment.text()
				),
				fragment,
			}));
		};

		Ok(AstAlter::Flow(AstAlterFlow {
			token,
			flow,
			action,
		}))
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{AstAlter, AstAlterFlowAction, AstAlterSequence, AstAlterTableAction, AstLiteral},
			parse::Parser,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_alter_sequence_with_schema() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "ALTER SEQUENCE test::users::id SET VALUE 1000").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
				assert!(!sequence.namespace.is_empty());
				assert_eq!(sequence.namespace[0].text(), "test");
				assert_eq!(sequence.name.text(), "users");
				assert_eq!(column.text(), "id");
				match value {
					AstLiteral::Number(num) => {
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "ALTER SEQUENCE users::id SET VALUE 500").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
				assert!(sequence.namespace.is_empty());
				assert_eq!(sequence.name.text(), "users");
				assert_eq!(column.text(), "id");
				match value {
					AstLiteral::Number(num) => {
						assert_eq!(num.value(), "500")
					}
					_ => panic!("Expected number literal"),
				}
			}
			_ => panic!("Expected AstAlter::Sequence"),
		}
	}

	#[test]
	fn test_alter_flow_rename() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "ALTER FLOW old_flow RENAME TO new_flow").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Flow(flow) => {
				assert_eq!(flow.flow.name.text(), "old_flow");
				assert!(flow.flow.namespace.is_empty());

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
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "ALTER FLOW test::old_flow RENAME TO new_flow").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Flow(flow) => {
				assert_eq!(flow.flow.namespace[0].text(), "test");
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "ALTER FLOW my_flow SET QUERY AS FROM new_source FILTER {active = true}")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
					}
					_ => panic!("Expected SetQuery action"),
				}
			}
			_ => panic!("Expected AstAlter::Flow"),
		}
	}

	#[test]
	fn test_alter_flow_set_query_with_braces() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"
			ALTER FLOW my_flow SET QUERY AS {
				FROM new_source
				FILTER {active = true}
				AGGREGATE {total: count(*) } BY {category}
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
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Flow(flow) => {
				assert_eq!(flow.flow.name.text(), "my_flow");

				match &flow.action {
					AstAlterFlowAction::SetQuery {
						query,
					} => {
						assert!(query.len() >= 3);
					}
					_ => panic!("Expected SetQuery action"),
				}
			}
			_ => panic!("Expected AstAlter::Flow"),
		}
	}

	#[test]
	fn test_alter_flow_pause() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "ALTER FLOW my_flow PAUSE").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "ALTER FLOW analytics::my_flow RESUME").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Flow(flow) => {
				assert_eq!(flow.flow.namespace[0].text(), "analytics");
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

	#[test]
	fn test_alter_table_add_column() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "ALTER TABLE app::users ADD COLUMN email: Utf8").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Table(table) => {
				assert_eq!(table.table.namespace[0].text(), "app");
				assert_eq!(table.table.name.text(), "users");

				match &table.action {
					AstAlterTableAction::AddColumn {
						column,
					} => {
						assert_eq!(column.name.text(), "email");
					}
					_ => panic!("Expected AddColumn action"),
				}
			}
			_ => panic!("Expected AstAlter::Table"),
		}
	}

	#[test]
	fn test_alter_table_drop_column() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "ALTER TABLE app::users DROP COLUMN email").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Table(table) => {
				assert_eq!(table.table.name.text(), "users");

				match &table.action {
					AstAlterTableAction::DropColumn {
						column,
					} => {
						assert_eq!(column.text(), "email");
					}
					_ => panic!("Expected DropColumn action"),
				}
			}
			_ => panic!("Expected AstAlter::Table"),
		}
	}

	#[test]
	fn test_alter_table_rename_column() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "ALTER TABLE users RENAME COLUMN name TO full_name")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let alter = result.first_unchecked().as_alter();

		match alter {
			AstAlter::Table(table) => {
				assert_eq!(table.table.name.text(), "users");

				match &table.action {
					AstAlterTableAction::RenameColumn {
						old_name,
						new_name,
					} => {
						assert_eq!(old_name.text(), "name");
						assert_eq!(new_name.text(), "full_name");
					}
					_ => panic!("Expected RenameColumn action"),
				}
			}
			_ => panic!("Expected AstAlter::Table"),
		}
	}
}
