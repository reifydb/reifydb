// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

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

				// Create AST nodes from tokens
				let first_identifier =
					crate::ast::ast::AstIdentifier(
						first_identifier_token,
					);
				let second_identifier =
					crate::ast::ast::AstIdentifier(
						second_identifier_token,
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
					schema: Some(first_identifier),
					table: second_identifier,
					column,
					value,
				}))
			} else {
				// table.column
				self.consume_keyword(Keyword::Set)?;
				self.consume_keyword(Keyword::Value)?;
				let value_token = self.consume(crate::ast::tokenize::TokenKind::Literal(crate::ast::tokenize::Literal::Number))?;

				// Create AST nodes from tokens
				let first_identifier =
					crate::ast::ast::AstIdentifier(
						first_identifier_token,
					);
				let second_identifier =
					crate::ast::ast::AstIdentifier(
						second_identifier_token,
					);
				let value = crate::ast::AstLiteral::Number(
					crate::ast::ast::AstLiteralNumber(
						value_token,
					),
				);

				Ok(AstAlter::Sequence(AstAlterSequence {
					token,
					schema: None,
					table: first_identifier,
					column: second_identifier,
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
		let schema = self.parse_identifier()?;
		self.consume_operator(Operator::Dot)?;
		let table = self.parse_identifier()?;

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
			schema,
			table,
			operations,
		}))
	}

	fn parse_alter_view(
		&mut self,
		token: Token<'a>,
	) -> crate::Result<AstAlter<'a>> {
		// Parse schema.view
		let schema = self.parse_identifier()?;
		self.consume_operator(Operator::Dot)?;
		let view = self.parse_identifier()?;

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
			schema,
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
					reifydb_core::SortDirection::Asc
				} else if self
					.current()?
					.is_keyword(Keyword::Desc)
				{
					self.consume_keyword(Keyword::Desc)?;
					reifydb_core::SortDirection::Desc
				} else {
					reifydb_core::SortDirection::Asc
				}
			} else {
				reifydb_core::SortDirection::Asc
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
				schema,
				table,
				column,
				value,
				..
			}) => {
				assert!(schema.is_some());
				assert_eq!(
					schema.as_ref().unwrap().value(),
					"test"
				);
				assert_eq!(table.value(), "users");
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
				schema,
				table,
				column,
				value,
				..
			}) => {
				assert!(schema.is_none());
				assert_eq!(table.value(), "users");
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
}
