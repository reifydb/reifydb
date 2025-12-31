// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DDL statement parsing (CREATE, ALTER, DROP).
//!
//! Currently stubs - to be implemented.

use super::super::{
	Parser,
	error::{ParseError, ParseErrorKind},
};
use crate::ast::Statement;

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse CREATE statement.
	pub(in crate::ast::parse) fn parse_create(&mut self) -> Result<Statement<'bump>, ParseError> {
		// TODO: Implement CREATE parsing
		Err(self.error(ParseErrorKind::NotImplemented("CREATE")))
	}

	/// Parse ALTER statement.
	pub(in crate::ast::parse) fn parse_alter(&mut self) -> Result<Statement<'bump>, ParseError> {
		// TODO: Implement ALTER parsing
		Err(self.error(ParseErrorKind::NotImplemented("ALTER")))
	}

	/// Parse DROP statement.
	pub(in crate::ast::parse) fn parse_drop(&mut self) -> Result<Statement<'bump>, ParseError> {
		// TODO: Implement DROP parsing
		Err(self.error(ParseErrorKind::NotImplemented("DROP")))
	}
}
