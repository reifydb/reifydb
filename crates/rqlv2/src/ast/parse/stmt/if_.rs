// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! IF statement parsing.
//!
//! Syntax:
//! - `if condition { body }`
//! - `if condition { body } else { body }`
//! - `if condition { body } else if condition { body } else { body }`

use bumpalo::collections::Vec as BumpVec;

use super::super::{Parser, error::ParseError, pratt::Precedence};
use crate::{
	ast::{Statement, stmt::ElseIfBranch},
	token::{Keyword, Punctuation},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse if statement.
	pub(in crate::ast::parse) fn parse_if_stmt(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::If)?;

		let condition = self.parse_expr(Precedence::None)?;

		self.expect_punct(Punctuation::OpenCurly)?;
		let then_branch = self.parse_block()?;
		let mut end_span = self.expect_punct(Punctuation::CloseCurly)?;

		// Parse else-if branches
		let mut else_ifs = BumpVec::new_in(self.bump);
		while self.try_consume_keyword(Keyword::Else) {
			if self.try_consume_keyword(Keyword::If) {
				let cond = self.parse_expr(Precedence::None)?;
				self.expect_punct(Punctuation::OpenCurly)?;
				let body = self.parse_block()?;
				end_span = self.expect_punct(Punctuation::CloseCurly)?;

				else_ifs.push(ElseIfBranch::new(cond, body, cond.span().merge(&end_span)));
			} else {
				// else branch
				self.expect_punct(Punctuation::OpenCurly)?;
				let else_body = self.parse_block()?;
				end_span = self.expect_punct(Punctuation::CloseCurly)?;

				return Ok(Statement::If(crate::ast::stmt::IfStmt::new(
					condition,
					then_branch,
					else_ifs.into_bump_slice(),
					Some(else_body),
					start_span.merge(&end_span),
				)));
			}
		}

		Ok(Statement::If(crate::ast::stmt::IfStmt::new(
			condition,
			then_branch,
			else_ifs.into_bump_slice(),
			None,
			start_span.merge(&end_span),
		)))
	}
}
