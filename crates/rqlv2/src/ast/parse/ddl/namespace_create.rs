// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CREATE NAMESPACE statement parsing.
//!
//! RQL syntax: `CREATE NAMESPACE name [IF NOT EXISTS]`
//!             `CREATE NAMESPACE IF NOT EXISTS name`

use crate::{
	ast::{
		Statement,
		parse::{ParseError, Parser},
		stmt::ddl::{CreateNamespace, CreateStmt},
	},
	token::TokenKind,
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse CREATE NAMESPACE statement.
	///
	/// Syntax: `CREATE NAMESPACE [IF NOT EXISTS] name`
	///         `CREATE NAMESPACE name [IF NOT EXISTS]`
	///
	/// # Examples
	///
	/// ```rql
	/// CREATE NAMESPACE mydb
	/// CREATE NAMESPACE IF NOT EXISTS mydb
	/// CREATE NAMESPACE mydb IF NOT EXISTS
	/// ```
	pub(in crate::ast::parse) fn parse_create_namespace(
		&mut self,
		start: crate::token::Span,
	) -> Result<Statement<'bump>, ParseError> {
		// Check for IF NOT EXISTS before identifier
		let mut if_not_exists = self.try_parse_if_not_exists();

		// Parse namespace name
		let name_token = self.current();
		if !matches!(name_token.kind, TokenKind::Identifier) {
			return Err(self.error(crate::ast::parse::ParseErrorKind::ExpectedIdentifier));
		}
		let name = self.token_text(name_token);
		let name_span = self.advance().span;

		// Check for IF NOT EXISTS after identifier (alternate syntax)
		if !if_not_exists {
			if_not_exists = self.try_parse_if_not_exists();
		}

		let span = start.merge(&name_span);

		Ok(Statement::Create(CreateStmt::Namespace(CreateNamespace::new(
			name,
			if_not_exists,
			span,
		))))
	}
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use crate::{ast::Statement, token::tokenize};

	#[test]
	fn test_create_namespace_simple() {
		let bump = Bump::new();
		let result = tokenize("CREATE NAMESPACE mydb", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "CREATE NAMESPACE mydb").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Namespace(ns)) => {
				assert_eq!(ns.name, "mydb");
				assert!(!ns.if_not_exists);
			}
			_ => panic!("Expected CREATE NAMESPACE statement"),
		}
	}

	#[test]
	fn test_create_namespace_if_not_exists_before() {
		let bump = Bump::new();
		let result = tokenize("CREATE NAMESPACE IF NOT EXISTS mydb", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "CREATE NAMESPACE IF NOT EXISTS mydb").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Namespace(ns)) => {
				assert_eq!(ns.name, "mydb");
				assert!(ns.if_not_exists);
			}
			_ => panic!("Expected CREATE NAMESPACE statement"),
		}
	}

	#[test]
	fn test_create_namespace_if_not_exists_after() {
		let bump = Bump::new();
		let result = tokenize("CREATE NAMESPACE mydb IF NOT EXISTS", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "CREATE NAMESPACE mydb IF NOT EXISTS").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Namespace(ns)) => {
				assert_eq!(ns.name, "mydb");
				assert!(ns.if_not_exists);
			}
			_ => panic!("Expected CREATE NAMESPACE statement"),
		}
	}

	#[test]
	fn test_create_namespace_lowercase() {
		let bump = Bump::new();
		let result = tokenize("create namespace test", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "create namespace test").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Namespace(ns)) => {
				assert_eq!(ns.name, "test");
				assert!(!ns.if_not_exists);
			}
			_ => panic!("Expected CREATE NAMESPACE statement"),
		}
	}
}
