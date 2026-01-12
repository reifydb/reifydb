// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CREATE VIEW statement parsing.
//!
//! RQL syntax:
//! - `CREATE VIEW namespace.view AS { query }`
//! - `CREATE DEFERRED VIEW namespace.view { columns } [AS { query }]`
//! - `CREATE TRANSACTIONAL VIEW namespace.view { columns } [AS { query }]`

use bumpalo::collections::Vec as BumpVec;

use crate::{
	ast::{
		Expr, Statement,
		parse::{ParseError, Parser, Precedence},
		stmt::ddl::{CreateStmt, CreateView},
	},
	token::{Keyword, Operator, Punctuation},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse CREATE VIEW statement (simple view with AS query).
	///
	/// Syntax: `CREATE VIEW [IF NOT EXISTS] namespace.view AS { query }`
	///
	/// # Examples
	///
	/// ```rql
	/// CREATE VIEW test.active_users AS { FROM test.users | FILTER active = true }
	/// ```
	pub(in crate::ast::parse) fn parse_create_view(
		&mut self,
		start: crate::token::Span,
	) -> Result<Statement<'bump>, ParseError> {
		// Check for IF NOT EXISTS
		let if_not_exists = self.try_parse_if_not_exists();

		// Parse namespace.view
		let (namespace, view_name) = self.parse_required_qualified_identifier()?;

		// Parse AS clause (required for simple views)
		self.expect_operator(Operator::As)?;

		// Parse query in braces
		let query = self.parse_view_query()?;

		let end_span = self.current().span;
		let span = start.merge(&end_span);

		Ok(Statement::Create(CreateStmt::View(CreateView::new(
			Some(namespace),
			view_name,
			query,
			if_not_exists,
			span,
		))))
	}

	/// Parse CREATE DEFERRED VIEW statement.
	///
	/// Syntax: `CREATE DEFERRED VIEW namespace.view { columns } [AS { query }]`
	pub(in crate::ast::parse) fn parse_create_deferred_view(
		&mut self,
		start: crate::token::Span,
	) -> Result<Statement<'bump>, ParseError> {
		// Consume VIEW keyword
		self.expect_keyword(Keyword::View)?;

		// Parse namespace.view
		let (namespace, view_name) = self.parse_required_qualified_identifier()?;

		// Parse column definitions (required for deferred views)
		let _columns = self.parse_column_definitions()?;

		// Parse optional AS clause
		let query = if self.check_operator(Operator::As) {
			self.advance();
			self.parse_view_query()?
		} else {
			&[]
		};

		let end_span = self.current().span;
		let span = start.merge(&end_span);

		Ok(Statement::Create(CreateStmt::View(CreateView::new(
			Some(namespace),
			view_name,
			query,
			false,
			span,
		))))
	}

	/// Parse CREATE TRANSACTIONAL VIEW statement.
	///
	/// Syntax: `CREATE TRANSACTIONAL VIEW namespace.view { columns } [AS { query }]`
	pub(in crate::ast::parse) fn parse_create_transactional_view(
		&mut self,
		start: crate::token::Span,
	) -> Result<Statement<'bump>, ParseError> {
		// Consume VIEW keyword
		self.expect_keyword(Keyword::View)?;

		// Parse namespace.view
		let (namespace, view_name) = self.parse_required_qualified_identifier()?;

		// Parse column definitions (required for transactional views)
		let _columns = self.parse_column_definitions()?;

		// Parse optional AS clause
		let query = if self.check_operator(Operator::As) {
			self.advance();
			self.parse_view_query()?
		} else {
			&[]
		};

		let end_span = self.current().span;
		let span = start.merge(&end_span);

		Ok(Statement::Create(CreateStmt::View(CreateView::new(
			Some(namespace),
			view_name,
			query,
			false,
			span,
		))))
	}

	/// Parse view query: `{ FROM ... | FILTER ... }`
	fn parse_view_query(&mut self) -> Result<&'bump [Expr<'bump>], ParseError> {
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut exprs = BumpVec::new_in(self.bump);

		loop {
			self.skip_newlines();

			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}

			let expr = self.parse_expr(Precedence::None)?;
			exprs.push(*expr);

			self.skip_newlines();

			// Check for pipe operator
			if self.try_consume_operator(Operator::Pipe) {
				continue;
			}

			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}
		}

		self.expect_punct(Punctuation::CloseCurly)?;

		Ok(exprs.into_bump_slice())
	}
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use crate::{ast::Statement, token::tokenize};

	#[test]
	fn test_create_view_simple() {
		let bump = Bump::new();
		let source = "CREATE VIEW test.active_users AS { FROM test.users }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::View(v)) => {
				assert_eq!(v.namespace, Some("test"));
				assert_eq!(v.name, "active_users");
				assert!(!v.query.is_empty());
			}
			_ => panic!("Expected CREATE VIEW statement"),
		}
	}

	#[test]
	fn test_create_view_if_not_exists() {
		let bump = Bump::new();
		let source = "CREATE VIEW IF NOT EXISTS test.myview AS { FROM test.data }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::View(v)) => {
				assert_eq!(v.namespace, Some("test"));
				assert_eq!(v.name, "myview");
				assert!(v.if_not_exists);
			}
			_ => panic!("Expected CREATE VIEW statement"),
		}
	}

	#[test]
	fn test_create_deferred_view() {
		let bump = Bump::new();
		let source = "CREATE DEFERRED VIEW test.myview { id: Int4 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::View(v)) => {
				assert_eq!(v.namespace, Some("test"));
				assert_eq!(v.name, "myview");
			}
			_ => panic!("Expected CREATE VIEW statement"),
		}
	}

	#[test]
	fn test_create_transactional_view() {
		let bump = Bump::new();
		let source = "CREATE TRANSACTIONAL VIEW test.myview { id: Int4, name: Text }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::View(v)) => {
				assert_eq!(v.namespace, Some("test"));
				assert_eq!(v.name, "myview");
			}
			_ => panic!("Expected CREATE VIEW statement"),
		}
	}

	#[test]
	fn test_create_view_lowercase() {
		let bump = Bump::new();
		let source = "create view test.users_view as { from test.users }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::View(v)) => {
				assert_eq!(v.namespace, Some("test"));
				assert_eq!(v.name, "users_view");
			}
			_ => panic!("Expected CREATE VIEW statement"),
		}
	}
}
