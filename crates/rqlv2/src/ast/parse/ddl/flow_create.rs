// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CREATE FLOW statement parsing.
//!
//! RQL syntax:
//! - `CREATE FLOW [IF NOT EXISTS] [namespace.]name AS { query }`
//! - `CREATE OR REPLACE FLOW [IF NOT EXISTS] [namespace.]name AS { query }`

use bumpalo::collections::Vec as BumpVec;

use crate::{
	ast::{
		Expr, Statement,
		parse::{ParseError, Parser, Precedence},
		stmt::ddl::{CreateFlow, CreateStmt},
	},
	token::{operator::Operator, punctuation::Punctuation},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse CREATE FLOW statement.
	///
	/// Syntax: `CREATE [OR REPLACE] FLOW [IF NOT EXISTS] [namespace.]name AS { query }`
	///
	/// # Examples
	///
	/// ```rql
	/// CREATE FLOW my_flow AS { FROM orders | FILTER status = 'pending' }
	/// CREATE OR REPLACE FLOW analytics.sales_flow AS { FROM sales.orders }
	/// CREATE FLOW IF NOT EXISTS my_flow AS { FROM events }
	/// ```
	pub(in crate::ast::parse) fn parse_create_flow(
		&mut self,
		start: crate::token::span::Span,
		or_replace: bool,
	) -> Result<Statement<'bump>, ParseError> {
		// Check for IF NOT EXISTS
		let if_not_exists = self.try_parse_if_not_exists();

		// Parse flow name: [namespace.]name
		let (namespace, name) = self.parse_qualified_identifier()?;

		// Parse required AS clause
		self.expect_operator(Operator::As)?;

		// Parse query in braces
		let query = self.parse_flow_query()?;

		let end_span = self.current().span;
		let span = start.merge(&end_span);

		Ok(Statement::Create(CreateStmt::Flow(CreateFlow::new(
			namespace,
			name,
			query,
			or_replace,
			if_not_exists,
			span,
		))))
	}

	/// Parse flow query: `{ FROM ... | FILTER ... }`
	fn parse_flow_query(&mut self) -> Result<&'bump [Expr<'bump>], ParseError> {
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
pub mod tests {
	use bumpalo::Bump;

	use crate::{ast::Statement, token::tokenize};

	#[test]
	fn test_create_flow_basic() {
		let bump = Bump::new();
		let source = "CREATE FLOW my_flow AS { FROM orders }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Flow(f)) => {
				assert!(f.namespace.is_none());
				assert_eq!(f.name, "my_flow");
				assert!(!f.or_replace);
				assert!(!f.if_not_exists);
				assert!(!f.query.is_empty());
			}
			_ => panic!("Expected CREATE FLOW statement"),
		}
	}

	#[test]
	fn test_create_flow_qualified() {
		let bump = Bump::new();
		let source = "CREATE FLOW analytics.sales_flow AS { FROM sales.orders }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Flow(f)) => {
				assert_eq!(f.namespace, Some("analytics"));
				assert_eq!(f.name, "sales_flow");
				assert!(!f.or_replace);
			}
			_ => panic!("Expected CREATE FLOW statement"),
		}
	}

	#[test]
	fn test_create_flow_or_replace() {
		let bump = Bump::new();
		let source = "CREATE OR REPLACE FLOW my_flow AS { FROM orders }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Flow(f)) => {
				assert_eq!(f.name, "my_flow");
				assert!(f.or_replace);
				assert!(!f.if_not_exists);
			}
			_ => panic!("Expected CREATE FLOW statement"),
		}
	}

	#[test]
	fn test_create_flow_if_not_exists() {
		let bump = Bump::new();
		let source = "CREATE FLOW IF NOT EXISTS my_flow AS { FROM orders }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Flow(f)) => {
				assert_eq!(f.name, "my_flow");
				assert!(!f.or_replace);
				assert!(f.if_not_exists);
			}
			_ => panic!("Expected CREATE FLOW statement"),
		}
	}

	#[test]
	fn test_create_flow_or_replace_if_not_exists() {
		let bump = Bump::new();
		let source = "CREATE OR REPLACE FLOW IF NOT EXISTS test.my_flow AS { FROM orders }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Flow(f)) => {
				assert_eq!(f.namespace, Some("test"));
				assert_eq!(f.name, "my_flow");
				assert!(f.or_replace);
				assert!(f.if_not_exists);
			}
			_ => panic!("Expected CREATE FLOW statement"),
		}
	}

	#[test]
	fn test_create_flow_with_pipeline() {
		let bump = Bump::new();
		let source = "CREATE FLOW filtered AS { FROM events | FILTER type = 'purchase' }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Flow(f)) => {
				assert_eq!(f.name, "filtered");
				// Query should have multiple expressions (FROM and FILTER)
				assert!(f.query.len() >= 1);
			}
			_ => panic!("Expected CREATE FLOW statement"),
		}
	}

	#[test]
	fn test_create_flow_lowercase() {
		let bump = Bump::new();
		let source = "create flow my_flow as { from orders }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Flow(f)) => {
				assert_eq!(f.name, "my_flow");
			}
			_ => panic!("Expected CREATE FLOW statement"),
		}
	}

	#[test]
	fn test_create_flow_complex_query() {
		let bump = Bump::new();
		let source = r#"CREATE FLOW aggregated AS {
			FROM raw_events
			FILTER type = 'purchase'
			AGGREGATE { total: count(*) } BY { user_id }
			MAP { user_id, total }
		}"#;
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Flow(f)) => {
				assert_eq!(f.name, "aggregated");
				// Query should have multiple stages: FROM, FILTER, AGGREGATE, MAP
				assert!(f.query.len() >= 4);
			}
			_ => panic!("Expected CREATE FLOW statement"),
		}
	}

	#[test]
	fn test_create_flow_multiline_stages() {
		let bump = Bump::new();
		let source = r#"CREATE FLOW processed AS {
			FROM events
			FILTER active = true
			SORT { timestamp: desc }
			TAKE 100
		}"#;
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Flow(f)) => {
				assert_eq!(f.name, "processed");
				// Should have FROM, FILTER, SORT, TAKE stages
				assert!(f.query.len() >= 4);
			}
			_ => panic!("Expected CREATE FLOW statement"),
		}
	}
}
