// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DML statement parsing (INSERT, UPDATE, DELETE).
//!
//! Currently stubs - to be implemented.

use super::super::{
	Parser,
	error::{ParseError, ParseErrorKind},
};
use crate::ast::Statement;

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse INSERT statement.
	pub(in crate::ast::parse) fn parse_insert(&mut self) -> Result<Statement<'bump>, ParseError> {
		// TODO: Implement INSERT parsing
		Err(self.error(ParseErrorKind::NotImplemented("INSERT")))
	}

	/// Parse UPDATE statement.
	pub(in crate::ast::parse) fn parse_update(&mut self) -> Result<Statement<'bump>, ParseError> {
		// TODO: Implement UPDATE parsing
		Err(self.error(ParseErrorKind::NotImplemented("UPDATE")))
	}

	/// Parse DELETE statement.
	pub(in crate::ast::parse) fn parse_delete(&mut self) -> Result<Statement<'bump>, ParseError> {
		// TODO: Implement DELETE parsing
		Err(self.error(ParseErrorKind::NotImplemented("DELETE")))
	}
}
