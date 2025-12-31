// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! SORT expression parsing.

use bumpalo::collections::Vec as BumpVec;

use super::{
	Parser,
	error::{ParseError, ParseErrorKind},
	pratt::Precedence,
};
use crate::{
	ast::{Expr, expr::*},
	token::{Keyword, Operator, Punctuation},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse SORT expression.
	/// Syntax: SORT { key: ASC, key2: DESC } or SORT { key } (defaults to ASC)
	pub(super) fn parse_sort(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume SORT

		// Require opening brace
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut columns = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let expr = self.parse_expr(Precedence::Comparison)?;

			// Check for colon followed by direction, or just use default
			let direction = if self.try_consume_operator(Operator::Colon) {
				if self.try_consume_keyword(Keyword::Asc) {
					Some(SortDirection::Asc)
				} else if self.try_consume_keyword(Keyword::Desc) {
					Some(SortDirection::Desc)
				} else {
					return Err(self.error(ParseErrorKind::ExpectedKeyword(Keyword::Asc)));
				}
			} else {
				None // Default direction
			};

			columns.push(SortColumn::new(expr, direction));

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Sort(SortExpr::new(columns.into_bump_slice(), start_span.merge(&end_span)))))
	}
}
