// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Control flow statement parsing (break, continue).
//!
//! Syntax:
//! - `break`
//! - `continue`

use crate::{
	ast::{
		parse::{Parser, error::ParseError},
		stmt::{
			Statement,
			control::{BreakStmt, ContinueStmt},
		},
	},
	token::keyword::Keyword,
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse break statement.
	pub(in crate::ast::parse) fn parse_break(&mut self) -> Result<Statement<'bump>, ParseError> {
		let span = self.expect_keyword(Keyword::Break)?;
		Ok(Statement::Break(BreakStmt::new(span)))
	}

	/// Parse continue statement.
	pub(in crate::ast::parse) fn parse_continue(&mut self) -> Result<Statement<'bump>, ParseError> {
		let span = self.expect_keyword(Keyword::Continue)?;
		Ok(Statement::Continue(ContinueStmt::new(span)))
	}
}
