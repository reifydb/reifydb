// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DISTINCT expression parsing.

use bumpalo::collections::Vec as BumpVec;

use super::{Parser, error::ParseError, pratt::Precedence};
use crate::{
	ast::{Expr, expr::*},
	token::Punctuation,
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse DISTINCT expression.
	pub(super) fn parse_distinct(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume DISTINCT

		// Require opening brace
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut columns = BumpVec::new_in(self.bump);

		// Parse columns (can be empty for DISTINCT {} meaning all columns)
		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let col = self.parse_expr(Precedence::Comparison)?;
			columns.push(*col);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Distinct(DistinctExpr::new(
			columns.into_bump_slice(),
			start_span.merge(&end_span),
		))))
	}
}
