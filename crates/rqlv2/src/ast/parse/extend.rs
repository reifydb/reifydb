// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! EXTEND expression parsing.

use bumpalo::collections::Vec as BumpVec;

use super::{Parser, error::ParseError, pratt::Precedence};
use crate::{
	ast::{Expr, expr::*},
	token::Punctuation,
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse EXTEND expression.
	pub(super) fn parse_extend(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume EXTEND

		// Require opening brace
		self.expect_punct(Punctuation::OpenCurly)?;

		let mut extensions = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseCurly) && !self.is_eof() {
			let ext = self.parse_expr(Precedence::None)?;
			extensions.push(*ext);

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(self.alloc(Expr::Extend(ExtendExpr::new(extensions.into_bump_slice(), start_span.merge(&end_span)))))
	}
}
