// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! AGGREGATE expression parsing.

use bumpalo::collections::Vec as BumpVec;

use super::{Parser, error::ParseError, pratt::Precedence};
use crate::{
	ast::{Expr, expr::*},
	token::{Keyword, Punctuation},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse AGGREGATE expression: AGGREGATE { expr, ... } BY { col, ... }
	pub(super) fn parse_aggregate(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume AGGREGATE

		// Require opening brace for aggregations
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut aggregations = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let agg = self.parse_expr(Precedence::None)?;
			aggregations.push(*agg);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		self.expect_punct(Punctuation::CloseCurly)?;

		// Require BY keyword
		self.expect_keyword(Keyword::By)?;

		// Require opening brace for group-by columns
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut group_by = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let col = self.parse_expr(Precedence::Comparison)?;
			group_by.push(*col);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Aggregate(AggregateExpr::new(
			group_by.into_bump_slice(),
			aggregations.into_bump_slice(),
			start_span.merge(&end_span),
		))))
	}
}
