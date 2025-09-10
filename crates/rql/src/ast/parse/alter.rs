// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::SortDirection;

use crate::ast::{
	AstAlter, AstAlterSequence, AstAlterTable, AstAlterTableOperation,
	AstAlterView, AstAlterViewOperation, AstIndexColumn,
	parse::Parser,
	tokenize::{Keyword, Operator, Separator, Token, TokenKind},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_alter(&mut self) -> crate::Result<AstAlter<'a>> {
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

		unimplemented!(
			"Only ALTER SEQUENCE, ALTER TABLE, and ALTER VIEW are supported"
		);
	}

	fn parse_alter_sequence(
		&mut self,
		token: Token<'a>,
	) -> crate::Result<AstAlter<'a>> {
		// Parse schema.table.column or table.column
		let first_identifier_token = self
			.consume(crate::ast::tokenize::TokenKind::Identifier)?;

		if self.current()?.is_operator(Operator::Dot) {
			self.consume_operator(Operator::Dot)?;
			let second_identifier_token = self.consume(
				crate::ast::tokenize::TokenKind::Identifier,
			)?;

			if self.current()?.is_operator(Operator::Dot) {
				self.consume_operator(Operator::Dot)?;
				let column_token = self.consume(crate::ast::tokenize::TokenKind::Identifier)?;

				// Expect SET VALUE <number>
				self.consume_keyword(Keyword::Set)?;
				self.consume_keyword(Keyword::Value)?;
				let value_token = self.consume(crate::ast::tokenize::TokenKind::Literal(crate::ast::tokenize::Literal::Number))?;

				// Create MaybeQualifiedSequenceIdentifier with
				// schema
				use crate::ast::identifier::MaybeQualifiedSequenceIdentifier;
				let sequence =
					MaybeQualifiedSequenceIdentifier::new(
						second_identifier_token
							.fragment
							.clone(),
					)
					.with_schema(
						first_identifier_token
							.fragment
							.clone(),
					);

				let column = crate::ast::ast::AstIdentifier(
					column_token,
				);
				let value = crate::ast::AstLiteral::Number(
					crate::ast::ast::AstLiteralNumber(
						value_token,
					),
				);

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
				let value_token = self.consume(crate::ast::tokenize::TokenKind::Literal(crate::ast::tokenize::Literal::Number))?;

				// Create MaybeQualifiedSequenceIdentifier
				// without schema
				use crate::ast::identifier::MaybeQualifiedSequenceIdentifier;
				let sequence =
					MaybeQualifiedSequenceIdentifier::new(
						first_identifier_token
							.fragment
							.clone(),
					);

				let column = crate::ast::ast::AstIdentifier(
					second_identifier_token,
				);
				let value = crate::ast::AstLiteral::Number(
					crate::ast::ast::AstLiteralNumber(
						value_token,
					),
				);

				Ok(AstAlter::Sequence(AstAlterSequence {
					token,
					sequence,
					column,
					value,
				}))
			}
		} else {
			unimplemented!(
				"ALTER SEQUENCE requires table.column or schema.table.column"
			);
		}
	}

	fn parse_alter_table(
		&mut self,
		token: Token<'a>,
	) -> crate::Result<AstAlter<'a>> {
		// Parse schema.table
		let schema_token = self
			.consume(crate::ast::tokenize::TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let table_token = self
			.consume(crate::ast::tokenize::TokenKind::Identifier)?;

		// Create MaybeQualifiedSourceIdentifier
		use reifydb_core::interface::identifier::SourceKind;

		use crate::ast::identifier::MaybeQualifiedSourceIdentifier;
		let table = MaybeQualifiedSourceIdentifier::new(
			table_token.fragment.clone(),
		)
		.with_schema(schema_token.fragment.clone())
		.with_kind(SourceKind::Table);

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
				let name = if !self
					.current()?
					.is_operator(Operator::OpenCurly)
				{
					Some(self.parse_identifier()?)
				} else {
					None
				};

				// Parse columns
				let columns =
					self.parse_primary_key_columns()?;

				operations.push(AstAlterTableOperation::CreatePrimaryKey {
                    name,
                    columns,
                });
			} else if self.current()?.is_keyword(Keyword::Drop) {
				self.consume_keyword(Keyword::Drop)?;
				self.consume_keyword(Keyword::Primary)?;
				self.consume_keyword(Keyword::Key)?;

				operations.push(
					AstAlterTableOperation::DropPrimaryKey,
				);
			} else {
				unimplemented!(
					"Unsupported ALTER TABLE operation"
				);
			}

			self.skip_new_line()?;

			// Check for comma separator for multiple operations
			if self.consume_if(TokenKind::Separator(
				Separator::Comma,
			))?
			.is_some()
			{
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

	fn parse_alter_view(
		&mut self,
		token: Token<'a>,
	) -> crate::Result<AstAlter<'a>> {
		// Parse schema.view
		let schema_token = self
			.consume(crate::ast::tokenize::TokenKind::Identifier)?;
		self.consume_operator(Operator::Dot)?;
		let view_token = self
			.consume(crate::ast::tokenize::TokenKind::Identifier)?;

		// Create MaybeQualifiedSourceIdentifier for view
		use reifydb_core::interface::identifier::SourceKind;

		use crate::ast::identifier::MaybeQualifiedSourceIdentifier;
		let view = MaybeQualifiedSourceIdentifier::new(
			view_token.fragment.clone(),
		)
		.with_schema(schema_token.fragment.clone())
		.with_kind(SourceKind::View);

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
				let name = if !self
					.current()?
					.is_operator(Operator::OpenCurly)
				{
					Some(self.parse_identifier()?)
				} else {
					None
				};

				// Parse columns
				let columns =
					self.parse_primary_key_columns()?;

				operations.push(AstAlterViewOperation::CreatePrimaryKey {
                    name,
                    columns,
                });
			} else if self.current()?.is_keyword(Keyword::Drop) {
				self.consume_keyword(Keyword::Drop)?;
				self.consume_keyword(Keyword::Primary)?;
				self.consume_keyword(Keyword::Key)?;

				operations.push(
					AstAlterViewOperation::DropPrimaryKey,
				);
			} else {
				unimplemented!(
					"Unsupported ALTER VIEW operation"
				);
			}

			self.skip_new_line()?;

			// Check for comma separator for multiple operations
			if self.consume_if(TokenKind::Separator(
				Separator::Comma,
			))?
			.is_some()
			{
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

	fn parse_primary_key_columns(
		&mut self,
	) -> crate::Result<Vec<AstIndexColumn<'a>>> {
		let mut columns = Vec::new();

		self.consume_operator(Operator::OpenCurly)?;

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let column = self.parse_identifier()?;

			// Check for optional sort direction
			let sort_direction = if self
				.current()?
				.is_operator(Operator::Colon)
			{
				self.consume_operator(Operator::Colon)?;

				if self.current()?.is_keyword(Keyword::Asc) {
					self.consume_keyword(Keyword::Asc)?;
					SortDirection::Asc
				} else if self
					.current()?
					.is_keyword(Keyword::Desc)
				{
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

			if self.consume_if(TokenKind::Separator(
				Separator::Comma,
			))?
			.is_some()
			{
				continue;
			}

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		if columns.is_empty() {
			unimplemented!(
				"Primary key must have at least one column"
			);
		}

		Ok(columns)
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{
		AstAlter, AstAlterSequence, AstAlterTable,
		AstAlterTableOperation, AstAlterView, AstAlterViewOperation,
		parse::Parser, tokenize::tokenize,
	};

	#[test]
	fn test_alter_sequence_with_schema() {
		let tokens =
			tokenize("ALTER SEQUENCE test.users.id SET VALUE 1000")
				.unwrap();
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
				assert!(sequence.schema.is_some());
				assert_eq!(
					sequence.schema
						.as_ref()
						.unwrap()
						.text(),
					"test"
				);
				assert_eq!(sequence.name.text(), "users");
				assert_eq!(column.value(), "id");
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
		let tokens = tokenize("ALTER SEQUENCE users.id SET VALUE 500")
			.unwrap();
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
				assert!(sequence.schema.is_none());
				assert_eq!(sequence.name.text(), "users");
				assert_eq!(column.value(), "id");
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
		let tokens = tokenize(
			"ALTER TABLE test.users { create primary key pk_users {id} }",
		)
		.unwrap();
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
				assert!(table.schema.is_some());
				assert_eq!(
					table.schema.as_ref().unwrap().text(),
					"test"
				);
				assert_eq!(table.name.text(), "users");
				assert_eq!(operations.len(), 1);

				match &operations[0] {
					AstAlterTableOperation::CreatePrimaryKey { name, columns } => {
						assert!(name.is_some());
						assert_eq!(name.as_ref().unwrap().value(), "pk_users");
						assert_eq!(columns.len(), 1);
						assert_eq!(columns[0].column.value(), "id");
					}
					_ => panic!("Expected CreatePrimaryKey operation"),
				}
			}
			_ => panic!("Expected AstAlter::Table"),
		}
	}

	#[test]
	fn test_alter_table_create_primary_key_no_name() {
		let tokens = tokenize(
			"ALTER TABLE test.users { create primary key {id, email} }",
		)
		.unwrap();
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
				assert!(table.schema.is_some());
				assert_eq!(
					table.schema.as_ref().unwrap().text(),
					"test"
				);
				assert_eq!(table.name.text(), "users");
				assert_eq!(operations.len(), 1);

				match &operations[0] {
					AstAlterTableOperation::CreatePrimaryKey { name, columns } => {
						assert!(name.is_none());
						assert_eq!(columns.len(), 2);
						assert_eq!(columns[0].column.value(), "id");
						assert_eq!(columns[1].column.value(), "email");
					}
					_ => panic!("Expected CreatePrimaryKey operation"),
				}
			}
			_ => panic!("Expected AstAlter::Table"),
		}
	}

	#[test]
	fn test_alter_view_create_primary_key() {
		let tokens = tokenize(
			"ALTER VIEW test.user_view { create primary key pk_view {user_id} }",
		)
		.unwrap();
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
				assert!(view.schema.is_some());
				assert_eq!(
					view.schema.as_ref().unwrap().text(),
					"test"
				);
				assert_eq!(view.name.text(), "user_view");
				assert_eq!(operations.len(), 1);

				match &operations[0] {
					AstAlterViewOperation::CreatePrimaryKey { name, columns } => {
						assert!(name.is_some());
						assert_eq!(name.as_ref().unwrap().value(), "pk_view");
						assert_eq!(columns.len(), 1);
						assert_eq!(columns[0].column.value(), "user_id");
					}
					_ => panic!("Expected CreatePrimaryKey operation"),
				}
			}
			_ => panic!("Expected AstAlter::View"),
		}
	}

	#[test]
	fn test_alter_view_create_primary_key_no_name() {
		let tokens = tokenize(
			"ALTER VIEW test.user_view { create primary key {user_id, created_at} }",
		)
		.unwrap();
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
				assert!(view.schema.is_some());
				assert_eq!(
					view.schema.as_ref().unwrap().text(),
					"test"
				);
				assert_eq!(view.name.text(), "user_view");
				assert_eq!(operations.len(), 1);

				match &operations[0] {
					AstAlterViewOperation::CreatePrimaryKey { name, columns } => {
						assert!(name.is_none());
						assert_eq!(columns.len(), 2);
						assert_eq!(columns[0].column.value(), "user_id");
						assert_eq!(columns[1].column.value(), "created_at");
					}
					_ => panic!("Expected CreatePrimaryKey operation"),
				}
			}
			_ => panic!("Expected AstAlter::View"),
		}
	}
}
