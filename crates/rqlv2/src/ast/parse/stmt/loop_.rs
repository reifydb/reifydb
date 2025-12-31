// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! LOOP statement parsing.
//!
//! Syntax:
//! - `loop { body }`

use super::super::{Parser, error::ParseError};
use crate::{
	ast::Statement,
	token::{Keyword, Punctuation},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse loop statement.
	pub(in crate::ast::parse) fn parse_loop(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::Loop)?;

		self.expect_punct(Punctuation::OpenCurly)?;
		let body = self.parse_block()?;
		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(Statement::Loop(crate::ast::stmt::LoopStmt::new(body, start_span.merge(&end_span))))
	}
}
