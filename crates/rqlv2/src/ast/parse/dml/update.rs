// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! UPDATE statement parsing.
//!
//! RQL syntax: `UPDATE [namespace.]table` or `UPDATE` (target inferred from pipeline)
//!
//! Unlike SQL, RQL UPDATE only specifies the target table.
//! The modifications come from preceding pipeline operations (MAP).

use crate::{
	ast::{
		Statement,
		parse::{ParseError, Parser},
		stmt::dml::UpdateStmt,
	},
	token::{keyword::Keyword, operator::Operator, token::TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse UPDATE statement.
	///
	/// Syntax: `UPDATE [namespace.]table` or `UPDATE`
	///
	/// # Examples
	///
	/// ```rql
	/// UPDATE users
	/// UPDATE test.users
	/// UPDATE
	/// FROM source | FILTER condition | MAP { col: new_value } | UPDATE target
	/// ```
	pub(in crate::ast::parse) fn parse_update(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start = self.expect_keyword(Keyword::Update)?;

		// Check if there's a target specified (optional for UPDATE)
		if !matches!(self.current().kind, TokenKind::Identifier) {
			// No target specified - will be inferred from pipeline
			return Ok(Statement::Update(UpdateStmt::new(
				None,
				"",   // empty table means infer from pipeline
				&[],  // assignments come from pipeline MAP
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

		Ok(Statement::Update(UpdateStmt::new(
			namespace,
			table,
			&[],  // assignments come from pipeline MAP
			None, // filter comes from pipeline FILTER
			span,
		)))
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::{ast::Statement, token::tokenize};

	#[test]
	fn test_update_table_only() {
		let bump = Bump::new();
		let result = tokenize("UPDATE users", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "UPDATE users").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Update(update) => {
				assert!(update.namespace.is_none());
				assert_eq!(update.table, "users");
			}
			_ => panic!("Expected UPDATE statement"),
		}
	}

	#[test]
	fn test_update_qualified() {
		let bump = Bump::new();
		let result = tokenize("UPDATE test.users", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "UPDATE test.users").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Update(update) => {
				assert_eq!(update.namespace, Some("test"));
				assert_eq!(update.table, "users");
			}
			_ => panic!("Expected UPDATE statement"),
		}
	}

	#[test]
	fn test_update_no_target() {
		let bump = Bump::new();
		let result = tokenize("UPDATE", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "UPDATE").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Update(update) => {
				assert!(update.namespace.is_none());
				assert_eq!(update.table, "");
			}
			_ => panic!("Expected UPDATE statement"),
		}
	}

	#[test]
	fn test_update_lowercase() {
		let bump = Bump::new();
		let result = tokenize("update users", &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, "update users").unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Update(update) => {
				assert!(update.namespace.is_none());
				assert_eq!(update.table, "users");
			}
			_ => panic!("Expected UPDATE statement"),
		}
	}
}
