// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Function definition (DEF/FN) parsing.
//!
//! Syntax:
//! - `fn name() { body }`
//! - `fn name($param) { body }`
//! - `fn name($param: Type) { body }`

use bumpalo::collections::Vec as BumpVec;

use crate::{
	ast::{
		Statement,
		parse::{
			Parser,
			error::{ParseError, ParseErrorKind},
		},
		stmt::binding::{DefStmt, Parameter},
	},
	token::{keyword::Keyword, operator::Operator, punctuation::Punctuation, token::TokenKind},
};

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse fn statement.
	pub(in crate::ast::parse) fn parse_def(&mut self) -> Result<Statement<'bump>, ParseError> {
		let start_span = self.expect_keyword(Keyword::Fn)?;

		// Expect function name
		if !matches!(self.current().kind, TokenKind::Identifier) {
			return Err(self.error(ParseErrorKind::ExpectedIdentifier));
		}

		let name_token = self.advance();
		let name = self.token_text(&name_token);

		// Parse parameters
		self.expect_punct(Punctuation::OpenParen)?;
		let params = self.parse_parameters()?;
		self.expect_punct(Punctuation::CloseParen)?;

		// Parse body
		self.expect_punct(Punctuation::OpenCurly)?;
		let body = self.parse_block()?;
		let end_span = self.expect_punct(Punctuation::CloseCurly)?;

		Ok(Statement::Def(DefStmt::new(name, params, body, start_span.merge(&end_span))))
	}

	/// Parse function parameters.
	pub(in crate::ast::parse) fn parse_parameters(&mut self) -> Result<&'bump [Parameter<'bump>], ParseError> {
		let mut params = BumpVec::new_in(self.bump);

		while !self.check_punct(Punctuation::CloseParen) {
			if !matches!(self.current().kind, TokenKind::Variable) {
				return Err(self.error(ParseErrorKind::ExpectedVariable));
			}

			let name_token = self.advance();
			let name = self.token_text(&name_token);
			let span = name_token.span;

			// Optional type annotation
			let param_type = if self.try_consume_operator(Operator::Colon) {
				if !matches!(self.current().kind, TokenKind::Identifier) {
					return Err(self.error(ParseErrorKind::ExpectedIdentifier));
				}
				let type_token = self.advance();
				Some(self.token_text(&type_token))
			} else {
				None
			};

			params.push(Parameter::new(name, param_type, span));

			if !self.try_consume_punct(Punctuation::Comma) {
				break;
			}
		}

		Ok(params.into_bump_slice())
	}
}
