// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FROM expression parsing.

use super::{Parser, error::ParseError};
use crate::{
	ast::{Expr, expr::*},
	token::{Punctuation, TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse FROM expression.
	pub(super) fn parse_from(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let start_span = self.advance().span; // consume FROM

		// Check for special cases
		if matches!(self.current().kind, TokenKind::Variable) {
			let var = self.parse_variable()?;
			if let Expr::Variable(v) = var {
				return Ok(self.alloc(Expr::From(FromExpr::Variable(FromVariable {
					variable: *v,
					span: start_span.merge(&v.span),
				}))));
			}
			if let Expr::Environment(e) = var {
				return Ok(self.alloc(Expr::From(FromExpr::Environment(FromEnvironment {
					span: e.span,
				}))));
			}
		}

		// Check for inline data: [ ... ]
		if self.check_punct(Punctuation::OpenBracket) {
			let list = self.parse_list()?;
			if let Expr::List(l) = list {
				return Ok(self.alloc(Expr::From(FromExpr::Inline(FromInline {
					rows: l.elements,
					span: start_span.merge(&l.span),
				}))));
			}
		}

		// Parse qualified name: namespace.table or ns1::ns2.table
		let qualified = self.parse_qualified_name()?;

		Ok(self.alloc(Expr::From(FromExpr::Source(
			SourceRef::new(qualified.name, start_span.merge(&qualified.span))
				.with_namespace(qualified.namespace),
		))))
	}
}
