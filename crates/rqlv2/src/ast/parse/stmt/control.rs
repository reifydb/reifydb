// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Control flow statement parsing (break, continue).
//!
//! Syntax:
//! - `break`
//! - `continue`

use super::super::{Parser, error::ParseError};
use crate::{ast::Statement, token::Keyword};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse break statement.
	pub(in crate::ast::parse) fn parse_break(&mut self) -> Result<Statement<'bump>, ParseError> {
		let span = self.expect_keyword(Keyword::Break)?;
		Ok(Statement::Break(crate::ast::stmt::BreakStmt::new(span)))
	}

	/// Parse continue statement.
	pub(in crate::ast::parse) fn parse_continue(&mut self) -> Result<Statement<'bump>, ParseError> {
		let span = self.expect_keyword(Keyword::Continue)?;
		Ok(Statement::Continue(crate::ast::stmt::ContinueStmt::new(span)))
	}
}
