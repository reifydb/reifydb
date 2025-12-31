// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! TAKE expression parsing.

use super::{Parser, error::ParseError, pratt::Precedence};
use crate::ast::{Expr, expr::*};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse TAKE expression.
	pub(super) fn parse_take(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume TAKE

		let count = self.parse_expr(Precedence::None)?;
		let span = start_span.merge(&count.span());

		Ok(self.alloc(Expr::Take(TakeExpr::new(count, span))))
	}
}
