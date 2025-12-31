// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FROM expression parsing.

use super::{
	Parser,
	error::{ParseError, ParseErrorKind},
};
use crate::{
	ast::{Expr, expr::*},
	token::{Operator, Punctuation, TokenKind},
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

		// Regular table reference (identifiers or keywords used as table names)
		if !matches!(
			self.current().kind,
			TokenKind::Identifier | TokenKind::QuotedIdentifier | TokenKind::Keyword(_)
		) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}

		let name_token = self.advance();
		let name = self.token_text(&name_token);
		let mut end_span = name_token.span;

		// Check for namespace qualification
		if self.check_operator(Operator::Dot) {
			self.advance();
			if !matches!(self.current().kind, TokenKind::Identifier | TokenKind::Keyword(_)) {
				return Err(self.error(ParseErrorKind::ExpectedIdentifier));
			}
			let table_token = self.advance();
			let table_name = self.token_text(&table_token);
			end_span = table_token.span;

			return Ok(self.alloc(Expr::From(FromExpr::Source(
				SourceRef::new(table_name, start_span.merge(&end_span)).with_namespace(name),
			))));
		}

		Ok(self.alloc(Expr::From(FromExpr::Source(SourceRef::new(name, start_span.merge(&end_span))))))
	}
}
