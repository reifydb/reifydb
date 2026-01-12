// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! INSERT statement parsing.
//!
//! RQL syntax: `INSERT [namespace.]table`
//!
//! Unlike SQL, RQL INSERT only specifies the target table.
//! The data to insert comes from the preceding pipeline operations.

use crate::{
	ast::{
		Statement,
		parse::{ParseError, ParseErrorKind, Parser},
		stmt::dml::{InsertSource, InsertStmt},
	},
	token::{Keyword, Operator, TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse INSERT statement.
	///
	/// Syntax: `INSERT [namespace.]table`
	///
	/// # Examples
	///
	/// ```rql
	/// INSERT users
	/// INSERT test.users
	/// FROM source | INSERT target
	/// ```
	pub(in crate::ast::parse) fn parse_insert(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start = self.expect_keyword(Keyword::Insert)?;

		// Parse target: [namespace.]table
		// The target is required for INSERT (unlike UPDATE/DELETE)
		let first_token = self.current();
		if !matches!(first_token.kind, TokenKind::Identifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}
		let first = self.token_text(first_token);
		let first_span = self.advance().span;

		let (namespace, table, end_span) = if self.check_operator(Operator::Dot) {
			self.advance(); // consume dot
			let second_token = self.current();
			if !matches!(second_token.kind, TokenKind::Identifier) {
				return Err(self.error(ParseErrorKind::ExpectedIdentifier));
			}
			let second = self.token_text(second_token);
			let second_span = self.advance().span;
			(Some(first), second, second_span)
		} else {
			(None, first, first_span)
		};

		let span = start.merge(&end_span);

		// In RQL, INSERT just specifies target - data comes from pipeline
		Ok(Statement::Insert(InsertStmt::new(
			namespace,
			table,
			None,                     // columns - inferred from pipeline
			InsertSource::Query(&[]), // source comes from pipeline
			span,
		)))
	}
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use crate::{ast::Statement, token::tokenize};

	#[test]
	fn test_insert_table_only() {
		let bump = Bump::new();
		let result = tokenize("INSERT users", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "INSERT users").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Insert(insert) => {
				assert!(insert.namespace.is_none());
				assert_eq!(insert.table, "users");
			}
			_ => panic!("Expected INSERT statement"),
		}
	}

	#[test]
	fn test_insert_qualified() {
		let bump = Bump::new();
		let result = tokenize("INSERT test.users", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "INSERT test.users").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Insert(insert) => {
				assert_eq!(insert.namespace, Some("test"));
				assert_eq!(insert.table, "users");
			}
			_ => panic!("Expected INSERT statement"),
		}
	}

	#[test]
	fn test_insert_lowercase() {
		let bump = Bump::new();
		let result = tokenize("insert users", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "insert users").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Insert(insert) => {
				assert!(insert.namespace.is_none());
				assert_eq!(insert.table, "users");
			}
			_ => panic!("Expected INSERT statement"),
		}
	}
}
