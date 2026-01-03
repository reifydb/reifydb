// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FOR statement parsing.
//!
//! Syntax:
//! - `for $var in iterable { body }`
//! - `for $var in pipeline { body }`

use bumpalo::collections::Vec as BumpVec;

use super::super::{
	Parser,
	error::{ParseError, ParseErrorKind},
	pratt::Precedence,
};
use crate::{
	ast::{Statement, stmt::ForIterable},
	token::{Keyword, Operator, Punctuation, TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse for statement.
	pub(in crate::ast::parse) fn parse_for(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::For)?;

		// Expect variable
		if !matches!(self.current().kind, TokenKind::Variable) {
			return Err(self.error(ParseErrorKind::ExpectedVariable));
		}

		let var_token = self.advance();
		let name = self.token_text(&var_token);
		let name = if name.starts_with('$') {
			&name[1..]
		} else {
			name
		};
		let name = self.alloc_str(name);

		self.expect_keyword(Keyword::In)?;

		// Parse iterable (expression or pipeline)
		let iterable = self.parse_for_iterable()?;

		self.expect_punct(Punctuation::OpenCurly)?;
		let body = self.parse_block()?;
		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(Statement::For(crate::ast::stmt::ForStmt::new(name, iterable, body, start_span.merge(&end_span))))
	}

	/// Parse the iterable part of a for statement.
	fn parse_for_iterable(&mut self) -> Result<ForIterable<'bump>, ParseError> {
		// Parse first expression
		let first = self.parse_expr(Precedence::None)?;

		// Check for pipe to make it a pipeline
		if self.try_consume_operator(Operator::Pipe) {
			let mut stages = BumpVec::new_in(self.bump);
			stages.push(*first);

			loop {
				let stage = self.parse_expr(Precedence::None)?;
				stages.push(*stage);

				if !self.try_consume_operator(Operator::Pipe) {
					break;
				}
			}

			Ok(ForIterable::Pipeline(stages.into_bump_slice()))
		} else {
			Ok(ForIterable::Expr(first))
		}
	}
}
