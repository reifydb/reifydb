// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CREATE INDEX statement parsing.
//!
//! RQL syntax:
//! - `CREATE INDEX idx_name ON namespace.table { col1, col2 DESC }`
//! - `CREATE UNIQUE INDEX idx_name ON namespace.table { col1 }`

use bumpalo::collections::Vec as BumpVec;

use crate::{
	ast::{
		Statement,
		parse::{ParseError, ParseErrorKind, Parser},
		stmt::ddl::{CreateIndex, CreateStmt, IndexColumn},
	},
	token::{Keyword, Punctuation, TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse CREATE INDEX statement.
	///
	/// Syntax: `CREATE [UNIQUE] INDEX idx_name ON namespace.table { columns }`
	///
	/// # Examples
	///
	/// ```rql
	/// CREATE INDEX idx_email ON test.users { email }
	/// CREATE UNIQUE INDEX idx_id ON test.users { id }
	/// CREATE INDEX idx_multi ON test.orders { status, created_at DESC }
	/// ```
	pub(in crate::ast::parse) fn parse_create_index(
		&mut self,
		start: crate::token::Span,
		unique: bool,
	) -> Result<Statement<'bump>, ParseError> {
		// Consume INDEX keyword
		self.expect_keyword(Keyword::Index)?;

		// Parse index name
		let name_token = self.current();
		if !matches!(name_token.kind, TokenKind::Identifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}
		let index_name = self.token_text(name_token);
		self.advance();

		// Expect ON keyword
		self.expect_keyword(Keyword::On)?;

		// Parse namespace.table
		let (namespace, table_name) = self.parse_required_qualified_identifier()?;

		// Parse columns in braces
		let columns = self.parse_index_columns()?;

		let end_span = self.current().span;
		let span = start.merge(&end_span);

		Ok(Statement::Create(CreateStmt::Index(CreateIndex::new(
			index_name,
			Some(namespace),
			table_name,
			columns,
			unique,
			span,
		))))
	}

	/// Parse index columns: `{ col1, col2 DESC, col3 ASC }`
	fn parse_index_columns(&mut self) -> Result<&'bump [IndexColumn<'bump>], ParseError> {
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut columns = BumpVec::new_in(self.bump);

		loop {
			self.skip_newlines();

			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}

			// Parse column name
			let col_token = self.current();
			if !matches!(col_token.kind, TokenKind::Identifier) {
				break;
			}
			let col_name = self.token_text(col_token);
			let col_span = col_token.span;
			self.advance();

			// Parse optional sort direction (ASC/DESC)
			let descending = if self.try_consume_keyword(Keyword::Desc) {
				true
			} else if self.try_consume_keyword(Keyword::Asc) {
				false
			} else {
				false // Default to ASC
			};

			columns.push(IndexColumn::new(col_name, descending, col_span));

			self.skip_newlines();

			// Check for comma or closing brace
			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		self.expect_punct(Punctuation::CloseCurly)?;

		Ok(columns.into_bump_slice())
	}
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use crate::{ast::Statement, token::tokenize};

	#[test]
	fn test_create_index_simple() {
		let bump = Bump::new();
		let source = "CREATE INDEX idx_email ON test.users { email }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Index(idx)) => {
				assert_eq!(idx.name, "idx_email");
				assert_eq!(idx.namespace, Some("test"));
				assert_eq!(idx.table, "users");
				assert!(!idx.unique);
				assert_eq!(idx.columns.len(), 1);
				assert_eq!(idx.columns[0].name, "email");
				assert!(!idx.columns[0].descending);
			}
			_ => panic!("Expected CREATE INDEX statement"),
		}
	}

	#[test]
	fn test_create_unique_index() {
		let bump = Bump::new();
		let source = "CREATE UNIQUE INDEX idx_id ON test.users { id }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Index(idx)) => {
				assert_eq!(idx.name, "idx_id");
				assert!(idx.unique);
			}
			_ => panic!("Expected CREATE INDEX statement"),
		}
	}

	#[test]
	fn test_create_index_multi_column() {
		let bump = Bump::new();
		let source = "CREATE INDEX idx_multi ON test.orders { status, created_at }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Index(idx)) => {
				assert_eq!(idx.columns.len(), 2);
				assert_eq!(idx.columns[0].name, "status");
				assert_eq!(idx.columns[1].name, "created_at");
			}
			_ => panic!("Expected CREATE INDEX statement"),
		}
	}

	#[test]
	fn test_create_index_with_sort_order() {
		let bump = Bump::new();
		let source = "CREATE INDEX idx_sorted ON test.events { created_at DESC, id ASC }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Index(idx)) => {
				assert_eq!(idx.columns.len(), 2);
				assert_eq!(idx.columns[0].name, "created_at");
				assert!(idx.columns[0].descending);
				assert_eq!(idx.columns[1].name, "id");
				assert!(!idx.columns[1].descending);
			}
			_ => panic!("Expected CREATE INDEX statement"),
		}
	}

	#[test]
	fn test_create_index_lowercase() {
		let bump = Bump::new();
		let source = "create index idx_name on myns.items { name }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Index(idx)) => {
				assert_eq!(idx.name, "idx_name");
				assert_eq!(idx.namespace, Some("myns"));
				assert_eq!(idx.table, "items");
			}
			_ => panic!("Expected CREATE INDEX statement"),
		}
	}
}
