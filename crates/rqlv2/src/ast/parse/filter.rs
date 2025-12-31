// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FILTER expression parsing.

use super::{Parser, error::ParseError, pratt::Precedence};
use crate::ast::{Expr, expr::*};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse FILTER expression.
	pub(super) fn parse_filter(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume FILTER

		let predicate = self.parse_expr(Precedence::None)?;
		let span = start_span.merge(&predicate.span());

		Ok(self.alloc(Expr::Filter(FilterExpr::new(predicate, span))))
	}
}
