// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CREATE SUBSCRIPTION statement parsing.
//!
//! RQL syntax:
//! - `CREATE SUBSCRIPTION { columns } [AS { query }]`
//!
//! Note: Subscriptions don't have names - they're identified by UUID v7.

use bumpalo::collections::Vec as BumpVec;

use crate::{
	ast::{
		Expr, Statement,
		parse::{ParseError, Parser, Precedence},
		stmt::ddl::{CreateSubscription, CreateStmt},
	},
	token::{Operator, Punctuation, Span},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse CREATE SUBSCRIPTION statement.
	///
	/// Syntax: `CREATE SUBSCRIPTION { columns } [AS { query }]`
	///
	/// Subscriptions don't have names - they're identified by UUID v7 at runtime.
	///
	/// # Examples
	///
	/// ```rql
	/// CREATE SUBSCRIPTION { id: Int4, name: Utf8 }
	/// CREATE SUBSCRIPTION { id: Int4, price: Float8 } AS { FROM test.products | FILTER price > 50 }
	/// ```
	pub(in crate::ast::parse) fn parse_create_subscription(
		&mut self,
		start: Span,
	) -> Result<Statement<'bump>, ParseError> {
		// Parse column definitions
		let columns = self.parse_column_definitions()?;

		// Parse optional AS clause
		let query = if self.check_operator(Operator::As) {
			self.advance();
			Some(self.parse_subscription_query()?)
		} else {
			None
		};

		let end_span = self.current().span;
		let span = start.merge(&end_span);

		Ok(Statement::Create(CreateStmt::Subscription(
			CreateSubscription::new(columns, query, span),
		)))
	}

	/// Parse subscription query: `{ FROM ... | FILTER ... }`
	fn parse_subscription_query(&mut self) -> Result<&'bump [Expr<'bump>], ParseError> {
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
	fn test_create_subscription_basic() {
		let bump = Bump::new();
		let source = "CREATE SUBSCRIPTION { id: Int4, name: Utf8 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Subscription(s)) => {
				assert_eq!(s.columns.len(), 2);
				assert_eq!(s.columns[0].name, "id");
				assert_eq!(s.columns[0].data_type, "Int4");
				assert_eq!(s.columns[1].name, "name");
				assert_eq!(s.columns[1].data_type, "Utf8");
				assert!(s.query.is_none());
			}
			_ => panic!("Expected CREATE SUBSCRIPTION statement"),
		}
	}

	#[test]
	fn test_create_subscription_single_column() {
		let bump = Bump::new();
		let source = "CREATE SUBSCRIPTION { value: Float8 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Subscription(s)) => {
				assert_eq!(s.columns.len(), 1);
				assert_eq!(s.columns[0].name, "value");
				assert_eq!(s.columns[0].data_type, "Float8");
			}
			_ => panic!("Expected CREATE SUBSCRIPTION statement"),
		}
	}

	#[test]
	fn test_create_subscription_with_query() {
		let bump = Bump::new();
		let source = "CREATE SUBSCRIPTION { id: Int4, name: Utf8 } AS { FROM test.products }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Subscription(s)) => {
				assert_eq!(s.columns.len(), 2);
				assert!(s.query.is_some());
				assert!(!s.query.unwrap().is_empty());
			}
			_ => panic!("Expected CREATE SUBSCRIPTION statement"),
		}
	}

	#[test]
	fn test_create_subscription_with_pipeline() {
		let bump = Bump::new();
		let source =
			"CREATE SUBSCRIPTION { id: Int4, price: Float8 } AS { FROM test.products | FILTER price > 50 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Subscription(s)) => {
				assert_eq!(s.columns.len(), 2);
				assert!(s.query.is_some());
				// Should have FROM and FILTER
				assert!(s.query.unwrap().len() >= 1);
			}
			_ => panic!("Expected CREATE SUBSCRIPTION statement"),
		}
	}

	#[test]
	fn test_create_subscription_lowercase() {
		let bump = Bump::new();
		let source = "create subscription { data: blob }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Subscription(s)) => {
				assert_eq!(s.columns.len(), 1);
				assert_eq!(s.columns[0].name, "data");
			}
			_ => panic!("Expected CREATE SUBSCRIPTION statement"),
		}
	}
}
