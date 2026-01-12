// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DELETE statement parsing.
//!
//! RQL syntax: `DELETE [namespace.]table` or `DELETE` (target inferred from pipeline)
//!
//! Unlike SQL, RQL DELETE only specifies the target table.
//! The filter comes from preceding pipeline operations (FILTER).

use crate::{
	ast::{
		Statement,
		parse::{ParseError, Parser},
		stmt::dml::DeleteStmt,
	},
	token::{Keyword, Operator, TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse DELETE statement.
	///
	/// Syntax: `DELETE [namespace.]table` or `DELETE`
	///
	/// # Examples
	///
	/// ```rql
	/// DELETE users
	/// DELETE test.users
	/// DELETE
	/// FROM source | FILTER condition | DELETE target
	/// ```
	pub(in crate::ast::parse) fn parse_delete(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start = self.expect_keyword(Keyword::Delete)?;

		// Check if there's a target specified (optional for DELETE)
		if !matches!(self.current().kind, TokenKind::Identifier) {
			// No target specified - will be inferred from pipeline
			return Ok(Statement::Delete(DeleteStmt::new(
				None,
				"",   // empty table means infer from pipeline
				None, // filter comes from pipeline FILTER
				start,
			)));
		}

		let first_token = self.current();
		let first = self.token_text(first_token);
		let first_span = self.advance().span;

		let (namespace, table, end_span) = if self.check_operator(Operator::Dot) {
			self.advance(); // consume dot
			if matches!(self.current().kind, TokenKind::Identifier) {
				let second_token = self.current();
				let second = self.token_text(second_token);
				let second_span = self.advance().span;
				(Some(first), second, second_span)
			} else {
				// Dot but no identifier after - just use what we have
				(None, first, first_span)
			}
		} else {
			(None, first, first_span)
		};

		let span = start.merge(&end_span);

		Ok(Statement::Delete(DeleteStmt::new(
			namespace,
			table,
			None, // filter comes from pipeline FILTER
			span,
		)))
	}
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use crate::{ast::Statement, token::tokenize};

	#[test]
	fn test_delete_table_only() {
		let bump = Bump::new();
		let result = tokenize("DELETE users", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "DELETE users").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Delete(delete) => {
				assert!(delete.namespace.is_none());
				assert_eq!(delete.table, "users");
			}
			_ => panic!("Expected DELETE statement"),
		}
	}

	#[test]
	fn test_delete_qualified() {
		let bump = Bump::new();
		let result = tokenize("DELETE test.users", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "DELETE test.users").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Delete(delete) => {
				assert_eq!(delete.namespace, Some("test"));
				assert_eq!(delete.table, "users");
			}
			_ => panic!("Expected DELETE statement"),
		}
	}

	#[test]
	fn test_delete_no_target() {
		let bump = Bump::new();
		let result = tokenize("DELETE", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "DELETE").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Delete(delete) => {
				assert!(delete.namespace.is_none());
				assert_eq!(delete.table, "");
			}
			_ => panic!("Expected DELETE statement"),
		}
	}

	#[test]
	fn test_delete_lowercase() {
		let bump = Bump::new();
		let result = tokenize("delete users", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "delete users").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Delete(delete) => {
				assert!(delete.namespace.is_none());
				assert_eq!(delete.table, "users");
			}
			_ => panic!("Expected DELETE statement"),
		}
	}
}
