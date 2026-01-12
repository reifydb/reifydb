// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! ALTER statement parsing.
//!
//! RQL syntax:
//! - `ALTER TABLE namespace.table { CREATE PRIMARY KEY { cols } }`
//! - `ALTER TABLE namespace.table { DROP PRIMARY KEY }`
//! - `ALTER SEQUENCE [namespace.]table.column SET VALUE N`

use bumpalo::collections::Vec as BumpVec;

use crate::{
	ast::{
		Expr, Statement,
		parse::{ParseError, ParseErrorKind, Parser, Precedence},
		stmt::ddl::{
			AlterFlow, AlterFlowAction, AlterSequence, AlterStmt, AlterTable, AlterTableAction,
			AlterView, AlterViewAction, ColumnDef,
		},
	},
	token::{Keyword, LiteralKind, Operator, Punctuation, TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse ALTER statement.
	///
	/// # Examples
	///
	/// ```rql
	/// ALTER TABLE test.users { CREATE PRIMARY KEY { id } }
	/// ALTER SEQUENCE test.users.id SET VALUE 1000
	/// ```
	pub(in crate::ast::parse) fn parse_alter(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start = self.expect_keyword(Keyword::Alter)?;

		if self.check_keyword(Keyword::Table) {
			self.advance();
			return self.parse_alter_table(start);
		}

		if self.check_keyword(Keyword::Sequence) {
			self.advance();
			return self.parse_alter_sequence(start);
		}

		if self.check_keyword(Keyword::View) {
			self.advance();
			return self.parse_alter_view(start);
		}

		if self.check_keyword(Keyword::Flow) {
			self.advance();
			return self.parse_alter_flow(start);
		}

		Err(self.error(ParseErrorKind::UnexpectedToken))
	}

	/// Parse ALTER TABLE statement.
	fn parse_alter_table(
		&mut self,
		start: crate::token::Span,
	) -> Result<Statement<'bump>, ParseError> {
		// Parse namespace.table
		let (namespace, table_name) = self.parse_required_qualified_identifier()?;

		// Parse operations block
		self.expect_punct(Punctuation::OpenCurly)?;
		self.skip_newlines();

		// Parse operation
		let action = if self.try_consume_keyword(Keyword::Create) {
			// CREATE PRIMARY KEY
			self.expect_keyword(Keyword::Primary)?;
			self.expect_keyword(Keyword::Key)?;

			// Parse column list
			self.expect_punct(Punctuation::OpenCurly)?;
			let mut cols = bumpalo::collections::Vec::new_in(self.bump);

			loop {
				self.skip_newlines();
				if self.check_punct(Punctuation::CloseCurly) {
					break;
				}

				let col_token = self.current();
				if !matches!(col_token.kind, TokenKind::Identifier) {
					break;
				}
				let col_span = col_token.span;
				let col_name = self.token_text(col_token);
				self.advance();

				// Create a minimal ColumnDef for the primary key column
				cols.push(ColumnDef::new(col_name, "", true, None, col_span));

				self.skip_newlines();
				if !self.try_consume_punct(Punctuation::Comma) {
					break;
				}
			}

			self.expect_punct(Punctuation::CloseCurly)?;

			// For now, just use the first column name
			if let Some(col) = cols.first() {
				AlterTableAction::AddColumn(*col)
			} else {
				return Err(self.error(ParseErrorKind::ExpectedIdentifier));
			}
		} else if self.try_consume_keyword(Keyword::Drop) {
			// DROP PRIMARY KEY or DROP COLUMN
			if self.try_consume_keyword(Keyword::Primary) {
				self.expect_keyword(Keyword::Key)?;
				// Use empty string for DROP PRIMARY KEY
				AlterTableAction::DropColumn(self.alloc_str(""))
			} else {
				// DROP COLUMN col_name
				let col_token = self.current();
				if !matches!(col_token.kind, TokenKind::Identifier) {
					return Err(self.error(ParseErrorKind::ExpectedIdentifier));
				}
				let col_name = self.token_text(col_token);
				self.advance();
				AlterTableAction::DropColumn(col_name)
			}
		} else {
			return Err(self.error(ParseErrorKind::UnexpectedToken));
		};

		self.skip_newlines();
		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		let span = start.merge(&end_span);

		Ok(Statement::Alter(AlterStmt::Table(AlterTable::new(
			Some(namespace),
			table_name,
			action,
			span,
		))))
	}

	/// Parse ALTER SEQUENCE statement.
	fn parse_alter_sequence(
		&mut self,
		start: crate::token::Span,
	) -> Result<Statement<'bump>, ParseError> {
		// Parse [namespace.]table.column
		let first_token = self.current();
		if !matches!(first_token.kind, TokenKind::Identifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}
		let first = self.token_text(first_token);
		self.advance();

		if !self.check_operator(Operator::Dot) {
			return Err(self.error(ParseErrorKind::ExpectedOperator(Operator::Dot)));
		}
		self.advance();

		let second_token = self.current();
		if !matches!(second_token.kind, TokenKind::Identifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}
		let second = self.token_text(second_token);
		self.advance();

		// Check for third part (namespace.table.column vs table.column)
		let (namespace, name) = if self.check_operator(Operator::Dot) {
			self.advance();
			let third_token = self.current();
			if !matches!(third_token.kind, TokenKind::Identifier) {
				return Err(self.error(ParseErrorKind::ExpectedIdentifier));
			}
			let _column = self.token_text(third_token);
			self.advance();
			(Some(first), second)
		} else {
			(None, first)
		};

		// Expect SET VALUE
		self.expect_keyword(Keyword::Set)?;
		self.expect_keyword(Keyword::Value)?;

		// Parse the value
		let value_token = self.current();
		let value_span = value_token.span;
		let restart = match &value_token.kind {
			TokenKind::Literal(LiteralKind::Integer) => {
				let text = self.token_text(value_token);
				let val: i64 = text.parse().unwrap_or(0);
				self.advance();
				Some(val)
			}
			_ => None,
		};

		let end_span = value_span;
		let span = start.merge(&end_span);

		Ok(Statement::Alter(AlterStmt::Sequence(AlterSequence::new(
			namespace, name, restart, span,
		))))
	}

	/// Parse ALTER VIEW statement.
	///
	/// # Examples
	///
	/// ```rql
	/// ALTER VIEW test.my_view { CREATE PRIMARY KEY { id } }
	/// ALTER VIEW test.my_view { DROP PRIMARY KEY }
	/// ```
	fn parse_alter_view(
		&mut self,
		start: crate::token::Span,
	) -> Result<Statement<'bump>, ParseError> {
		// Parse namespace.view
		let (namespace, view_name) = self.parse_required_qualified_identifier()?;

		// Parse operations block
		self.expect_punct(Punctuation::OpenCurly)?;
		self.skip_newlines();

		// Parse action
		let action = if self.try_consume_keyword(Keyword::Create) {
			// CREATE PRIMARY KEY { columns }
			self.expect_keyword(Keyword::Primary)?;
			self.expect_keyword(Keyword::Key)?;

			let columns = self.parse_primary_key_columns()?;
			AlterViewAction::CreatePrimaryKey(columns)
		} else if self.try_consume_keyword(Keyword::Drop) {
			// DROP PRIMARY KEY
			self.expect_keyword(Keyword::Primary)?;
			self.expect_keyword(Keyword::Key)?;
			AlterViewAction::DropPrimaryKey
		} else {
			return Err(self.error(ParseErrorKind::UnexpectedToken));
		};

		self.skip_newlines();
		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		let span = start.merge(&end_span);

		Ok(Statement::Alter(AlterStmt::View(AlterView::new(
			Some(namespace),
			view_name,
			action,
			span,
		))))
	}

	/// Parse primary key columns list: `{ col1, col2, ... }`
	fn parse_primary_key_columns(&mut self) -> Result<&'bump [&'bump str], ParseError> {
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut cols = BumpVec::new_in(self.bump);

		loop {
			self.skip_newlines();
			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}

			let col_token = self.current();
			let col_name = match &col_token.kind {
				TokenKind::Identifier => self.token_text(col_token),
				TokenKind::Keyword(_) => self.token_text(col_token),
				_ => return Err(self.error(ParseErrorKind::ExpectedIdentifier)),
			};
			self.advance();
			cols.push(col_name);

			self.skip_newlines();
			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		self.expect_punct(Punctuation::CloseCurly)?;

		Ok(cols.into_bump_slice())
	}

	/// Parse ALTER FLOW statement.
	///
	/// # Examples
	///
	/// ```rql
	/// ALTER FLOW test.my_flow RENAME TO new_name
	/// ALTER FLOW test.my_flow SET QUERY AS { FROM source | FILTER x > 0 }
	/// ALTER FLOW my_flow PAUSE
	/// ALTER FLOW my_flow RESUME
	/// ```
	fn parse_alter_flow(
		&mut self,
		start: crate::token::Span,
	) -> Result<Statement<'bump>, ParseError> {
		// Parse [namespace.]flow_name
		let (namespace, flow_name) = self.parse_qualified_identifier()?;

		// Parse action and capture end span
		let (action, end_span) = if self.check_keyword(Keyword::Rename) {
			self.advance();
			self.expect_keyword(Keyword::To)?;

			let new_name_token = self.current();
			if !matches!(new_name_token.kind, TokenKind::Identifier) {
				return Err(self.error(ParseErrorKind::ExpectedIdentifier));
			}
			let new_name = self.token_text(new_name_token);
			let span = self.advance().span;

			(AlterFlowAction::RenameTo(new_name), span)
		} else if self.check_keyword(Keyword::Set) {
			self.advance();
			self.expect_keyword(Keyword::Query)?;
			self.expect_operator(Operator::As)?;

			// Parse query block
			let query = self.parse_alter_flow_query()?;
			let span = self.current().span;
			(AlterFlowAction::SetQuery(query), span)
		} else if self.check_keyword(Keyword::Pause) {
			let span = self.advance().span;
			(AlterFlowAction::Pause, span)
		} else if self.check_keyword(Keyword::Resume) {
			let span = self.advance().span;
			(AlterFlowAction::Resume, span)
		} else {
			return Err(self.error(ParseErrorKind::UnexpectedToken));
		};

		let span = start.merge(&end_span);

		Ok(Statement::Alter(AlterStmt::Flow(AlterFlow::new(
			namespace,
			flow_name,
			action,
			span,
		))))
	}

	/// Parse ALTER FLOW query body: `{ FROM ... | FILTER ... }`
	fn parse_alter_flow_query(&mut self) -> Result<&'bump [Expr<'bump>], ParseError> {
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
	fn test_alter_table_drop_primary_key() {
		let bump = Bump::new();
		let source = "ALTER TABLE test.users { DROP PRIMARY KEY }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::Table(t)) => {
				assert_eq!(t.namespace, Some("test"));
				assert_eq!(t.name, "users");
			}
			_ => panic!("Expected ALTER TABLE statement"),
		}
	}

	#[test]
	fn test_alter_sequence() {
		let bump = Bump::new();
		let source = "ALTER SEQUENCE users.id SET VALUE 1000";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::Sequence(s)) => {
				assert_eq!(s.name, "users");
				assert_eq!(s.restart, Some(1000));
			}
			_ => panic!("Expected ALTER SEQUENCE statement"),
		}
	}

	#[test]
	fn test_alter_sequence_with_namespace() {
		let bump = Bump::new();
		let source = "ALTER SEQUENCE test.users.id SET VALUE 500";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::Sequence(s)) => {
				assert_eq!(s.namespace, Some("test"));
				assert_eq!(s.name, "users");
				assert_eq!(s.restart, Some(500));
			}
			_ => panic!("Expected ALTER SEQUENCE statement"),
		}
	}

	#[test]
	fn test_alter_lowercase() {
		let bump = Bump::new();
		let source = "alter table myns.items { drop primary key }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::Table(t)) => {
				assert_eq!(t.namespace, Some("myns"));
				assert_eq!(t.name, "items");
			}
			_ => panic!("Expected ALTER TABLE statement"),
		}
	}

	// ALTER VIEW tests

	#[test]
	fn test_alter_view_create_primary_key() {
		let bump = Bump::new();
		let source = "ALTER VIEW test.my_view { CREATE PRIMARY KEY { id } }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::View(v)) => {
				assert_eq!(v.namespace, Some("test"));
				assert_eq!(v.name, "my_view");
				match v.action {
					crate::ast::stmt::ddl::AlterViewAction::CreatePrimaryKey(cols) => {
						assert_eq!(cols.len(), 1);
						assert_eq!(cols[0], "id");
					}
					_ => panic!("Expected CREATE PRIMARY KEY action"),
				}
			}
			_ => panic!("Expected ALTER VIEW statement"),
		}
	}

	#[test]
	fn test_alter_view_create_composite_primary_key() {
		let bump = Bump::new();
		let source = "ALTER VIEW test.my_view { CREATE PRIMARY KEY { id, name } }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::View(v)) => {
				match v.action {
					crate::ast::stmt::ddl::AlterViewAction::CreatePrimaryKey(cols) => {
						assert_eq!(cols.len(), 2);
						assert_eq!(cols[0], "id");
						assert_eq!(cols[1], "name");
					}
					_ => panic!("Expected CREATE PRIMARY KEY action"),
				}
			}
			_ => panic!("Expected ALTER VIEW statement"),
		}
	}

	#[test]
	fn test_alter_view_drop_primary_key() {
		let bump = Bump::new();
		let source = "ALTER VIEW test.my_view { DROP PRIMARY KEY }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::View(v)) => {
				assert_eq!(v.namespace, Some("test"));
				assert_eq!(v.name, "my_view");
				assert!(matches!(
					v.action,
					crate::ast::stmt::ddl::AlterViewAction::DropPrimaryKey
				));
			}
			_ => panic!("Expected ALTER VIEW statement"),
		}
	}

	// ALTER FLOW tests

	#[test]
	fn test_alter_flow_rename() {
		let bump = Bump::new();
		let source = "ALTER FLOW test.my_flow RENAME TO new_flow_name";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::Flow(f)) => {
				assert_eq!(f.namespace, Some("test"));
				assert_eq!(f.name, "my_flow");
				match f.action {
					crate::ast::stmt::ddl::AlterFlowAction::RenameTo(new_name) => {
						assert_eq!(new_name, "new_flow_name");
					}
					_ => panic!("Expected RENAME TO action"),
				}
			}
			_ => panic!("Expected ALTER FLOW statement"),
		}
	}

	#[test]
	fn test_alter_flow_pause() {
		let bump = Bump::new();
		let source = "ALTER FLOW my_flow PAUSE";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::Flow(f)) => {
				assert_eq!(f.namespace, None);
				assert_eq!(f.name, "my_flow");
				assert!(matches!(
					f.action,
					crate::ast::stmt::ddl::AlterFlowAction::Pause
				));
			}
			_ => panic!("Expected ALTER FLOW statement"),
		}
	}

	#[test]
	fn test_alter_flow_resume() {
		let bump = Bump::new();
		let source = "ALTER FLOW test.my_flow RESUME";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::Flow(f)) => {
				assert_eq!(f.namespace, Some("test"));
				assert_eq!(f.name, "my_flow");
				assert!(matches!(
					f.action,
					crate::ast::stmt::ddl::AlterFlowAction::Resume
				));
			}
			_ => panic!("Expected ALTER FLOW statement"),
		}
	}

	#[test]
	fn test_alter_flow_set_query() {
		let bump = Bump::new();
		let source = "ALTER FLOW test.my_flow SET QUERY AS { FROM test.source }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::Flow(f)) => {
				assert_eq!(f.namespace, Some("test"));
				assert_eq!(f.name, "my_flow");
				match f.action {
					crate::ast::stmt::ddl::AlterFlowAction::SetQuery(query) => {
						assert!(!query.is_empty());
					}
					_ => panic!("Expected SET QUERY action"),
				}
			}
			_ => panic!("Expected ALTER FLOW statement"),
		}
	}

	#[test]
	fn test_alter_flow_lowercase() {
		let bump = Bump::new();
		let source = "alter flow my_flow pause";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::Flow(f)) => {
				assert_eq!(f.name, "my_flow");
				assert!(matches!(
					f.action,
					crate::ast::stmt::ddl::AlterFlowAction::Pause
				));
			}
			_ => panic!("Expected ALTER FLOW statement"),
		}
	}

	#[test]
	fn test_alter_flow_set_query_complex() {
		let bump = Bump::new();
		let source = r#"ALTER FLOW my_flow SET QUERY AS {
			FROM new_source
			FILTER active = true
			AGGREGATE { total: count(*) } BY { category }
		}"#;
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::Flow(f)) => {
				assert_eq!(f.name, "my_flow");
				match f.action {
					crate::ast::stmt::ddl::AlterFlowAction::SetQuery(query) => {
						// Should have FROM, FILTER, AGGREGATE
						assert!(query.len() >= 3);
					}
					_ => panic!("Expected SET QUERY action"),
				}
			}
			_ => panic!("Expected ALTER FLOW statement"),
		}
	}

	#[test]
	fn test_alter_flow_rename_unqualified() {
		let bump = Bump::new();
		let source = "ALTER FLOW old_name RENAME TO new_name";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::Flow(f)) => {
				assert!(f.namespace.is_none());
				assert_eq!(f.name, "old_name");
				match f.action {
					crate::ast::stmt::ddl::AlterFlowAction::RenameTo(new_name) => {
						assert_eq!(new_name, "new_name");
					}
					_ => panic!("Expected RENAME TO action"),
				}
			}
			_ => panic!("Expected ALTER FLOW statement"),
		}
	}

	#[test]
	fn test_alter_table_create_primary_key() {
		let bump = Bump::new();
		let source = "ALTER TABLE test.users { CREATE PRIMARY KEY { id } }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::Table(t)) => {
				assert_eq!(t.namespace, Some("test"));
				assert_eq!(t.name, "users");
			}
			_ => panic!("Expected ALTER TABLE statement"),
		}
	}

	#[test]
	fn test_alter_view_lowercase() {
		let bump = Bump::new();
		let source = "alter view myns.myview { drop primary key }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Alter(crate::ast::stmt::ddl::AlterStmt::View(v)) => {
				assert_eq!(v.namespace, Some("myns"));
				assert_eq!(v.name, "myview");
				assert!(matches!(
					v.action,
					crate::ast::stmt::ddl::AlterViewAction::DropPrimaryKey
				));
			}
			_ => panic!("Expected ALTER VIEW statement"),
		}
	}
}
