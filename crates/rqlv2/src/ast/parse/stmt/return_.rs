// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! RETURN statement parsing.
//!
//! Syntax:
//! - `return`
//! - `return expr`

use crate::{
	ast::{
		Statement,
		parse::{Parser, error::ParseError, pratt::Precedence},
		stmt::control::ReturnStmt,
	},
	token::keyword::Keyword,
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse return statement.
	pub(in crate::ast::parse) fn parse_return(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::Return)?;

		// Optional return value
		let value = if self.is_at_statement_end() {
			None
		} else {
			Some(self.parse_expr(Precedence::None)?)
		};

		let span = match &value {
			Some(v) => start_span.merge(&v.span()),
			None => start_span,
		};

		Ok(Statement::Return(ReturnStmt::new(value, span)))
	}
}
