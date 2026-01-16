// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CREATE TABLE statement parsing.
//!
//! RQL syntax: `CREATE TABLE namespace.table { col: Type, ... } [WITH { options }]`
//!
//! Unlike SQL, RQL uses curly braces and colon notation for column definitions.

use crate::{
	ast::{
		Statement,
		parse::{ParseError, Parser},
		stmt::ddl::{CreateStmt, CreateTable},
	},
	token::span::Span,
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse CREATE TABLE statement.
	///
	/// Syntax: `CREATE TABLE namespace.table { col: Type, ... }`
	///
	/// # Examples
	///
	/// ```rql
	/// CREATE TABLE test.users { id: Int4, name: Text }
	/// CREATE TABLE test.items { id: Int4 AUTO INCREMENT, value: Float8 }
	/// ```
	pub(in crate::ast::parse) fn parse_create_table(
		&mut self,
		start: Span,
	) -> Result<Statement<'bump>, ParseError> {
		// Parse namespace.table (namespace is required for tables)
		let (namespace, table_name) = self.parse_required_qualified_identifier()?;

		// Parse column definitions
		let columns = self.parse_column_definitions()?;
		let end_span = self.current().span;

		// TODO: Parse optional WITH block for primary_key

		let span = start.merge(&end_span);

		Ok(Statement::Create(CreateStmt::Table(CreateTable::new(
			Some(namespace),
			table_name,
			columns,
			false, // if_not_exists - TODO: add support
			span,
		))))
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::{ast::Statement, token::tokenize};

	#[test]
	fn test_create_table_simple() {
		let bump = Bump::new();
		let source = "CREATE TABLE test.users { id: Int4 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Table(t)) => {
				assert_eq!(t.namespace, Some("test"));
				assert_eq!(t.name, "users");
				assert_eq!(t.columns.len(), 1);
				assert_eq!(t.columns[0].name, "id");
				assert_eq!(t.columns[0].data_type, "Int4");
			}
			_ => panic!("Expected CREATE TABLE statement"),
		}
	}

	#[test]
	fn test_create_table_multiple_columns() {
		let bump = Bump::new();
		let source = "CREATE TABLE test.users { id: Int4, name: Text, active: Bool }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Table(t)) => {
				assert_eq!(t.namespace, Some("test"));
				assert_eq!(t.name, "users");
				assert_eq!(t.columns.len(), 3);
				assert_eq!(t.columns[0].name, "id");
				assert_eq!(t.columns[0].data_type, "Int4");
				assert_eq!(t.columns[1].name, "name");
				assert_eq!(t.columns[1].data_type, "Text");
				assert_eq!(t.columns[2].name, "active");
				assert_eq!(t.columns[2].data_type, "Bool");
			}
			_ => panic!("Expected CREATE TABLE statement"),
		}
	}

	#[test]
	fn test_create_table_lowercase() {
		let bump = Bump::new();
		let source = "create table myns.items { price: Float8 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Table(t)) => {
				assert_eq!(t.namespace, Some("myns"));
				assert_eq!(t.name, "items");
				assert_eq!(t.columns.len(), 1);
				assert_eq!(t.columns[0].name, "price");
				assert_eq!(t.columns[0].data_type, "Float8");
			}
			_ => panic!("Expected CREATE TABLE statement"),
		}
	}

	#[test]
	fn test_create_table_multiline() {
		let bump = Bump::new();
		let source = r#"CREATE TABLE test.users {
			id: Int4,
			name: Text,
			email: Utf8
		}"#;
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Table(t)) => {
				assert_eq!(t.name, "users");
				assert_eq!(t.columns.len(), 3);
			}
			_ => panic!("Expected CREATE TABLE statement"),
		}
	}
}
