// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::error::{AstErrorKind, Error, TypeError};

use super::{Parser, Precedence};
use crate::{
	Result,
	ast::ast::{AstMatch, AstMatchArm},
	bump::BumpBox,
	token::{keyword::Keyword, operator::Operator, separator::Separator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_match(&mut self) -> Result<AstMatch<'bump>> {
		let token = self.consume_keyword(Keyword::Match)?;

		let subject = if !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly) {
			None
		} else {
			Some(BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump()))
		};

		self.consume_operator(Operator::OpenCurly)?;

		let mut arms = Vec::new();
		loop {
			self.skip_new_line()?;

			if self.is_eof() {
				let fragment = token.fragment.to_owned();
				return Err(Error::from(TypeError::Ast {
					kind: AstErrorKind::UnexpectedToken {
						expected: "expected '}' to close MATCH".to_string(),
					},
					message: format!(
						"Unexpected token: expected {}, got {}",
						"expected '}' to close MATCH",
						fragment.text()
					),
					fragment,
				}));
			}

			if self.current()?.is_operator(Operator::CloseCurly) {
				self.advance()?;
				break;
			}

			arms.push(self.parse_match_arm(subject.is_some())?);

			if !self.is_eof() {
				self.consume_if(TokenKind::Separator(Separator::Comma))?;
			}
		}

		Ok(AstMatch {
			token,
			subject,
			arms,
		})
	}

	fn parse_match_arm(&mut self, has_subject: bool) -> Result<AstMatchArm<'bump>> {
		self.skip_new_line()?;

		if !self.is_eof() && self.current()?.is_keyword(Keyword::Else) {
			self.advance()?;
			self.consume_operator(Operator::Arrow)?;
			let result = self.parse_let_value()?;
			return Ok(AstMatchArm::Else {
				result,
			});
		}

		let expr = self.parse_node(Precedence::None)?;

		let guard = if !self.is_eof() && self.current()?.is_keyword(Keyword::If) {
			self.advance()?;
			Some(BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump()))
		} else {
			None
		};

		self.consume_operator(Operator::Arrow)?;

		let result = self.parse_let_value()?;

		if has_subject {
			Ok(AstMatchArm::Value {
				pattern: BumpBox::new_in(expr, self.bump()),
				guard,
				result,
			})
		} else {
			Ok(AstMatchArm::Condition {
				condition: BumpBox::new_in(expr, self.bump()),
				guard,
				result,
			})
		}
	}
}
