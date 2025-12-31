// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! LET statement parsing.
//!
//! Syntax:
//! - `let $name = expr`
//! - `let $name = pipeline`

use bumpalo::collections::Vec as BumpVec;

use super::super::{
	Parser,
	error::{ParseError, ParseErrorKind},
	pratt::Precedence,
};
use crate::{
	ast::{Statement, stmt::LetValue},
	token::{Keyword, Operator, TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse let statement.
	pub(in crate::ast::parse) fn parse_let(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::Let)?;

		// Expect variable
		if !matches!(self.current().kind, TokenKind::Variable) {
			return Err(self.error(ParseErrorKind::ExpectedVariable));
		}

		let var_token = self.advance();
		let name = self.token_text(&var_token);
		// Strip the $ prefix
		let name = if name.starts_with('$') {
			&name[1..]
		} else {
			name
		};
		let name = self.alloc_str(name);

		// Expect =
		self.expect_operator(Operator::Equal)?;

		// Parse value (expression or pipeline)
		let value = self.parse_let_value()?;

		let span = start_span.merge(&value.span());

		Ok(Statement::Let(crate::ast::stmt::LetStmt::new(name, value, span)))
	}

	/// Parse the value part of a let statement.
	fn parse_let_value(&mut self) -> Result<LetValue<'bump>, ParseError> {
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

			Ok(LetValue::Pipeline(stages.into_bump_slice()))
		} else {
			Ok(LetValue::Expr(first))
		}
	}
}
