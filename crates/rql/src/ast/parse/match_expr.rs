// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::diagnostic::ast::unexpected_token_error;

use super::Parser;
use crate::{
	ast::ast::{AstMatch, AstMatchArm, AstMatchArmDestructure},
	bump::BumpBox,
	token::{keyword::Keyword, operator::Operator, separator::Separator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	/// Parse a MATCH expression:
	///   MATCH [subject] { arm, arm, ... }
	pub(crate) fn parse_match(&mut self) -> crate::Result<AstMatch<'bump>> {
		let token = self.consume_keyword(Keyword::Match)?;

		// Determine if this is a searched MATCH (no subject) or value MATCH
		// If the next token is `{`, it's searched
		let subject = if !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly) {
			None
		} else {
			Some(BumpBox::new_in(self.parse_node(super::Precedence::None)?, self.bump()))
		};

		// Consume opening brace
		self.consume_operator(Operator::OpenCurly)?;

		// Parse arms
		let mut arms = Vec::new();
		loop {
			self.skip_new_line()?;

			if self.is_eof() {
				return Err(reifydb_type::error::Error(unexpected_token_error(
					"expected '}' to close MATCH",
					token.fragment.to_owned(),
				)));
			}

			// Check for closing brace
			if self.current()?.is_operator(Operator::CloseCurly) {
				self.advance()?;
				break;
			}

			arms.push(self.parse_match_arm(subject.is_some())?);

			// Optional comma between arms
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

	/// Parse a single match arm.
	/// `has_subject` indicates whether the MATCH has a subject expression.
	fn parse_match_arm(&mut self, has_subject: bool) -> crate::Result<AstMatchArm<'bump>> {
		self.skip_new_line()?;

		// ELSE arm
		if !self.is_eof() && self.current()?.is_keyword(Keyword::Else) {
			self.advance()?; // consume ELSE
			self.consume_operator(Operator::Arrow)?;
			let result = BumpBox::new_in(self.parse_node(super::Precedence::None)?, self.bump());
			return Ok(AstMatchArm::Else {
				result,
			});
		}

		// IS variant arm
		if !self.is_eof() && self.current()?.is_keyword(Keyword::Is) {
			return self.parse_match_is_variant_arm();
		}

		// Value or Condition arm
		let expr = self.parse_node(super::Precedence::None)?;

		// Optional IF guard
		let guard = if !self.is_eof() && self.current()?.is_keyword(Keyword::If) {
			self.advance()?; // consume IF
			Some(BumpBox::new_in(self.parse_node(super::Precedence::None)?, self.bump()))
		} else {
			None
		};

		// Consume =>
		self.consume_operator(Operator::Arrow)?;

		// Parse result expression
		let result = BumpBox::new_in(self.parse_node(super::Precedence::None)?, self.bump());

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

	/// Parse an IS variant match arm:
	///   IS [ns.]Type::Variant [{ field1, field2, ... }] [IF guard] => result
	fn parse_match_is_variant_arm(&mut self) -> crate::Result<AstMatchArm<'bump>> {
		self.advance()?; // consume IS

		// Parse [namespace.]SumType::Variant
		let first = self.consume(TokenKind::Identifier)?;

		let (namespace, sumtype_name) = if !self.is_eof() && self.current()?.is_operator(Operator::Dot) {
			self.consume_operator(Operator::Dot)?;
			let sumtype_token = self.consume(TokenKind::Identifier)?;
			(Some(first.fragment), sumtype_token.fragment)
		} else {
			(None, first.fragment)
		};

		self.consume_operator(Operator::DoubleColon)?;
		let variant_token = self.consume(TokenKind::Identifier)?;
		let variant_name = variant_token.fragment;

		// Optional destructuring: { field1, field2, ... }
		let destructure = if !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly) {
			self.advance()?; // consume {
			let mut fields = Vec::new();
			loop {
				self.skip_new_line()?;
				if self.is_eof() || self.current()?.is_operator(Operator::CloseCurly) {
					break;
				}
				let field_token = self.consume(TokenKind::Identifier)?;
				fields.push(field_token.fragment);
				// Optional comma
				if !self.is_eof() && self.current()?.is_separator(Separator::Comma) {
					self.advance()?;
				}
			}
			self.consume_operator(Operator::CloseCurly)?;
			Some(AstMatchArmDestructure {
				fields,
			})
		} else {
			None
		};

		// Optional IF guard
		let guard = if !self.is_eof() && self.current()?.is_keyword(Keyword::If) {
			self.advance()?; // consume IF
			Some(BumpBox::new_in(self.parse_node(super::Precedence::None)?, self.bump()))
		} else {
			None
		};

		// Consume =>
		self.consume_operator(Operator::Arrow)?;

		// Parse result expression
		let result = BumpBox::new_in(self.parse_node(super::Precedence::None)?, self.bump());

		Ok(AstMatchArm::IsVariant {
			namespace,
			sumtype_name,
			variant_name,
			destructure,
			guard,
			result,
		})
	}
}
