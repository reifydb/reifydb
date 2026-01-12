// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Common DDL parsing helpers.
//!
//! Shared utilities for DDL statement parsing:
//! - Qualified identifier parsing (`namespace.name` vs `[namespace.]name`)
//! - IF EXISTS / IF NOT EXISTS clause parsing
//! - Column definition parsing

use bumpalo::collections::Vec as BumpVec;

use crate::{
	ast::{
		parse::{ParseError, ParseErrorKind, Parser, Precedence},
		stmt::ddl::{ColumnDef, Policy, PolicyBlock, PolicyKind},
	},
	token::{Keyword, Operator, Punctuation, TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse a qualified identifier with optional namespace: `[namespace.]name`
	///
	/// Returns `(Option<namespace>, name)`.
	///
	/// # Examples
	///
	/// - `users` -> `(None, "users")`
	/// - `test.users` -> `(Some("test"), "users")`
	pub(in crate::ast::parse) fn parse_qualified_identifier(
		&mut self,
	) -> Result<(Option<&'bump str>, &'bump str), ParseError> {
		let first_token = self.current();
		if !matches!(first_token.kind, TokenKind::Identifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}
		let first = self.token_text(first_token);
		self.advance();

		if self.check_operator(Operator::Dot) {
			self.advance();
			let second_token = self.current();
			if !matches!(second_token.kind, TokenKind::Identifier) {
				return Err(self.error(ParseErrorKind::ExpectedIdentifier));
			}
			let second = self.token_text(second_token);
			self.advance();
			Ok((Some(first), second))
		} else {
			Ok((None, first))
		}
	}

	/// Parse a qualified identifier with required namespace: `namespace.name`
	///
	/// Returns `(namespace, name)`. Errors if no dot is present.
	///
	/// # Examples
	///
	/// - `test.users` -> `("test", "users")`
	/// - `users` -> Error (namespace required)
	pub(in crate::ast::parse) fn parse_required_qualified_identifier(
		&mut self,
	) -> Result<(&'bump str, &'bump str), ParseError> {
		let namespace_token = self.current();
		if !matches!(namespace_token.kind, TokenKind::Identifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}
		let namespace = self.token_text(namespace_token);
		self.advance();

		if !self.check_operator(Operator::Dot) {
			return Err(self.error(ParseErrorKind::ExpectedOperator(Operator::Dot)));
		}
		self.advance();

		let name_token = self.current();
		if !matches!(name_token.kind, TokenKind::Identifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}
		let name = self.token_text(name_token);
		self.advance();

		Ok((namespace, name))
	}

	/// Try to parse IF NOT EXISTS clause.
	///
	/// Returns `true` if the clause was present and consumed.
	pub(in crate::ast::parse) fn try_parse_if_not_exists(&mut self) -> bool {
		if self.check_keyword(Keyword::If) {
			// Check for NOT EXISTS
			if matches!(self.peek().kind, TokenKind::Operator(Operator::Not)) {
				self.advance(); // consume IF
				self.advance(); // consume NOT
				if self.check_keyword(Keyword::Exists) {
					self.advance(); // consume EXISTS
					return true;
				}
			}
		}
		false
	}

	/// Try to parse IF EXISTS clause.
	///
	/// Returns `true` if the clause was present and consumed.
	pub(in crate::ast::parse) fn try_parse_if_exists(&mut self) -> bool {
		if self.check_keyword(Keyword::If) {
			// Check for EXISTS
			if matches!(self.peek().kind, TokenKind::Keyword(Keyword::Exists)) {
				self.advance(); // consume IF
				self.advance(); // consume EXISTS
				return true;
			}
		}
		false
	}

	/// Parse column definitions: `{ col: Type, col2: Type2, ... }`
	///
	/// Expects to be called when the parser is positioned before the opening `{`.
	pub(in crate::ast::parse) fn parse_column_definitions(
		&mut self,
	) -> Result<&'bump [ColumnDef<'bump>], ParseError> {
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut columns = BumpVec::new_in(self.bump);

		loop {
			self.skip_newlines();

			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}

			let col = self.parse_column_definition()?;
			columns.push(col);

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

	/// Parse a single column definition: `name: Type [AUTO INCREMENT] [POLICY { ... }]`
	fn parse_column_definition(&mut self) -> Result<ColumnDef<'bump>, ParseError> {
		let start_span = self.current().span;

		// Parse column name (can be identifier or keyword since keywords can be column names)
		let name_token = self.current();
		let name = match &name_token.kind {
			TokenKind::Identifier => self.token_text(name_token),
			TokenKind::Keyword(_) => self.token_text(name_token),
			_ => return Err(self.error(ParseErrorKind::ExpectedIdentifier)),
		};
		self.advance();

		// Expect colon
		if !self.check_operator(Operator::Colon) {
			return Err(self.error(ParseErrorKind::ExpectedOperator(Operator::Colon)));
		}
		self.advance();

		// Parse type (can also be keyword since type names like Int4 might be keywords)
		let type_token = self.current();
		let data_type = match &type_token.kind {
			TokenKind::Identifier => self.token_text(type_token),
			TokenKind::Keyword(_) => self.token_text(type_token),
			_ => return Err(self.error(ParseErrorKind::ExpectedIdentifier)),
		};
		let mut end_span = self.advance().span;

		// TODO: Parse type parameters like UTF8(50) or DECIMAL(10,2)

		// Parse optional AUTO INCREMENT
		let auto_increment = if self.check_keyword(Keyword::Auto) {
			self.advance();
			end_span = self.expect_keyword(Keyword::Increment)?;
			true
		} else {
			false
		};

		// Parse optional POLICY block
		let policies = if self.check_keyword(Keyword::Policy) {
			let policy_block = self.parse_policy_block()?;
			end_span = policy_block.span;
			Some(policy_block)
		} else {
			None
		};

		// TODO: Parse DICTIONARY clause

		let span = start_span.merge(&end_span);

		Ok(ColumnDef::with_policies(
			name,
			data_type,
			true, // nullable - default
			None, // default value
			policies,
			auto_increment,
			span,
		))
	}

	/// Parse a POLICY block: `POLICY { saturation error, default 0, not undefined }`
	fn parse_policy_block(&mut self) -> Result<PolicyBlock<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::Policy)?;
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut policies = BumpVec::new_in(self.bump);

		loop {
			self.skip_newlines();

			if self.check_punct(Punctuation::CloseCurly) {
				break;
			}

			let policy = self.parse_single_policy()?;
			policies.push(policy);

			self.skip_newlines();

			// Check for comma
			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;
		let span = start_span.merge(&end_span);

		Ok(PolicyBlock::new(policies.into_bump_slice(), span))
	}

	/// Parse a single policy: `saturation error` or `default 0` or `not undefined`
	fn parse_single_policy(&mut self) -> Result<Policy<'bump>, ParseError> {
		let start_token = self.current();
		let start_span = start_token.span;

		// Parse policy kind - it's an identifier like "saturation", "default", or "not"
		let kind_text = match &start_token.kind {
			TokenKind::Identifier => self.token_text(start_token),
			TokenKind::Operator(Operator::Not) => {
				// Handle "not undefined" special case
				self.advance();
				let end_span = self.expect_keyword(Keyword::Undefined)?;
				// For "not undefined", the value is a placeholder undefined literal
				let undefined_expr = self.bump.alloc(crate::ast::Expr::Literal(
					crate::ast::expr::Literal::Undefined { span: end_span },
				));
				return Ok(Policy::new(PolicyKind::NotUndefined, undefined_expr, start_span.merge(&end_span)));
			}
			_ => return Err(self.error(ParseErrorKind::ExpectedIdentifier)),
		};
		self.advance();

		let kind = match kind_text.to_lowercase().as_str() {
			"saturation" => PolicyKind::Saturation,
			"default" => PolicyKind::Default,
			"not" => {
				// "not undefined"
				self.expect_keyword(Keyword::Undefined)?;
				PolicyKind::NotUndefined
			}
			_ => {
				return Err(self.error(ParseErrorKind::Custom(format!(
					"Invalid policy kind: {}. Expected 'saturation', 'default', or 'not'",
					kind_text
				))))
			}
		};

		// Parse the policy value (expression)
		let value = self.parse_expr(Precedence::None)?;
		let end_span = value.span();

		Ok(Policy::new(kind, value, start_span.merge(&end_span)))
	}
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use crate::{ast::Statement, token::tokenize};

	#[test]
	fn test_create_table_with_policy_saturation() {
		let bump = Bump::new();
		let source = "CREATE TABLE test.items { field: Int2 POLICY { saturation error } }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Table(t)) => {
				assert_eq!(t.columns.len(), 1);
				assert_eq!(t.columns[0].name, "field");
				assert_eq!(t.columns[0].data_type, "Int2");
				let policies = t.columns[0].policies.as_ref().expect("Expected policies");
				assert_eq!(policies.policies.len(), 1);
				assert_eq!(
					policies.policies[0].kind,
					crate::ast::stmt::ddl::PolicyKind::Saturation
				);
			}
			_ => panic!("Expected CREATE TABLE statement"),
		}
	}

	#[test]
	fn test_create_table_with_policy_default() {
		let bump = Bump::new();
		let source = "CREATE TABLE test.items { field: Int2 POLICY { default 0 } }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Table(t)) => {
				assert_eq!(t.columns.len(), 1);
				let policies = t.columns[0].policies.as_ref().expect("Expected policies");
				assert_eq!(policies.policies.len(), 1);
				assert_eq!(
					policies.policies[0].kind,
					crate::ast::stmt::ddl::PolicyKind::Default
				);
			}
			_ => panic!("Expected CREATE TABLE statement"),
		}
	}

	#[test]
	fn test_create_table_with_multiple_policies() {
		let bump = Bump::new();
		let source =
			"CREATE TABLE test.items { field: Int2 POLICY { saturation error, default 0 } }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Table(t)) => {
				assert_eq!(t.columns.len(), 1);
				let policies = t.columns[0].policies.as_ref().expect("Expected policies");
				assert_eq!(policies.policies.len(), 2);
				assert_eq!(
					policies.policies[0].kind,
					crate::ast::stmt::ddl::PolicyKind::Saturation
				);
				assert_eq!(
					policies.policies[1].kind,
					crate::ast::stmt::ddl::PolicyKind::Default
				);
			}
			_ => panic!("Expected CREATE TABLE statement"),
		}
	}

	#[test]
	fn test_create_table_with_auto_increment() {
		let bump = Bump::new();
		let source = "CREATE TABLE test.items { id: Int4 AUTO INCREMENT }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Table(t)) => {
				assert_eq!(t.columns.len(), 1);
				assert_eq!(t.columns[0].name, "id");
				assert!(t.columns[0].auto_increment);
			}
			_ => panic!("Expected CREATE TABLE statement"),
		}
	}

	#[test]
	fn test_create_table_with_auto_increment_and_policy() {
		let bump = Bump::new();
		let source = "CREATE TABLE test.items { id: Int4 AUTO INCREMENT POLICY { default 1 } }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Table(t)) => {
				assert_eq!(t.columns.len(), 1);
				assert!(t.columns[0].auto_increment);
				let policies = t.columns[0].policies.as_ref().expect("Expected policies");
				assert_eq!(policies.policies.len(), 1);
			}
			_ => panic!("Expected CREATE TABLE statement"),
		}
	}

	#[test]
	fn test_create_table_policy_lowercase() {
		let bump = Bump::new();
		let source = "create table test.items { field: int2 policy { saturation undefined } }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		match stmt {
			Statement::Create(crate::ast::stmt::ddl::CreateStmt::Table(t)) => {
				assert_eq!(t.columns.len(), 1);
				let policies = t.columns[0].policies.as_ref().expect("Expected policies");
				assert_eq!(policies.policies.len(), 1);
			}
			_ => panic!("Expected CREATE TABLE statement"),
		}
	}
}
