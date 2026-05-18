// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::{AstErrorKind, Error, TypeError};

use super::{Parser, Precedence};
use crate::{
	Result,
	ast::ast::{AstMatch, AstMatchArm, AstMatchArmDestructure},
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
			let result = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());
			return Ok(AstMatchArm::Else {
				result,
			});
		}

		if !self.is_eof() && self.current()?.is_keyword(Keyword::Is) {
			return self.parse_match_is_variant_arm();
		}

		if has_subject
			&& !self.is_eof() && self.current()?.is_identifier()
			&& self.position + 1 < self.tokens.len()
		{
			let next = self.tokens[self.position + 1];
			if next.is_operator(Operator::Arrow)
				|| next.is_operator(Operator::OpenCurly)
				|| next.is_keyword(Keyword::If)
			{
				return self.parse_match_variant_arm();
			}
		}

		let expr = self.parse_node(Precedence::None)?;

		let guard = if !self.is_eof() && self.current()?.is_keyword(Keyword::If) {
			self.advance()?;
			Some(BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump()))
		} else {
			None
		};

		self.consume_operator(Operator::Arrow)?;

		let result = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());

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

	fn parse_match_is_variant_arm(&mut self) -> Result<AstMatchArm<'bump>> {
		self.advance()?;

		let first = self.consume(TokenKind::Identifier)?;

		let (namespace, sumtype_name) = if !self.is_eof() && self.current()?.is_operator(Operator::DoubleColon)
		{
			self.consume_operator(Operator::DoubleColon)?;
			let sumtype_token = self.consume(TokenKind::Identifier)?;
			(Some(first.fragment), sumtype_token.fragment)
		} else {
			(None, first.fragment)
		};

		self.consume_operator(Operator::DoubleColon)?;
		let variant_token = self.consume(TokenKind::Identifier)?;
		let variant_name = variant_token.fragment;

		let destructure = if !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly) {
			self.advance()?;
			let mut fields = Vec::new();
			loop {
				self.skip_new_line()?;
				if self.is_eof() || self.current()?.is_operator(Operator::CloseCurly) {
					break;
				}
				let field_token = self.consume(TokenKind::Identifier)?;
				fields.push(field_token.fragment);

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

		let guard = if !self.is_eof() && self.current()?.is_keyword(Keyword::If) {
			self.advance()?;
			Some(BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump()))
		} else {
			None
		};

		self.consume_operator(Operator::Arrow)?;

		let result = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());

		Ok(AstMatchArm::IsVariant {
			namespace,
			sumtype_name,
			variant_name,
			destructure,
			guard,
			result,
		})
	}

	fn parse_match_variant_arm(&mut self) -> Result<AstMatchArm<'bump>> {
		let variant_token = self.consume(TokenKind::Identifier)?;
		let variant_name = variant_token.fragment;

		let destructure = if !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly) {
			self.advance()?;
			let mut fields = Vec::new();
			loop {
				self.skip_new_line()?;
				if self.is_eof() || self.current()?.is_operator(Operator::CloseCurly) {
					break;
				}
				let field_token = self.consume(TokenKind::Identifier)?;
				fields.push(field_token.fragment);

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

		let guard = if !self.is_eof() && self.current()?.is_keyword(Keyword::If) {
			self.advance()?;
			Some(BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump()))
		} else {
			None
		};

		self.consume_operator(Operator::Arrow)?;

		let result = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());

		Ok(AstMatchArm::Variant {
			variant_name,
			destructure,
			guard,
			result,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{ast::AstMatchArm, parse::parse},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_simple_variant_arm() {
		let bump = Bump::new();
		let source = "MATCH x { Active => 1, ELSE => 0 }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let m = result[0].first_unchecked().as_match();
		assert!(m.subject.is_some());
		assert_eq!(m.arms.len(), 2);
		assert!(matches!(&m.arms[0], AstMatchArm::Variant { variant_name, destructure, guard, .. }
			if variant_name.text() == "Active" && destructure.is_none() && guard.is_none()));
		assert!(matches!(&m.arms[1], AstMatchArm::Else { .. }));
	}

	#[test]
	fn test_variant_arm_with_destructure() {
		let bump = Bump::new();
		let source = "MATCH x { Circle { radius } => radius, ELSE => 0 }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let m = result[0].first_unchecked().as_match();
		assert_eq!(m.arms.len(), 2);
		match &m.arms[0] {
			AstMatchArm::Variant {
				variant_name,
				destructure,
				guard,
				..
			} => {
				assert_eq!(variant_name.text(), "Circle");
				assert!(guard.is_none());
				let destr = destructure.as_ref().unwrap();
				assert_eq!(destr.fields.len(), 1);
				assert_eq!(destr.fields[0].text(), "radius");
			}
			_ => panic!("expected Variant arm"),
		}
	}

	#[test]
	fn test_variant_arm_with_guard() {
		let bump = Bump::new();
		let source = "MATCH x { Active IF y > 0 => 1, ELSE => 0 }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let m = result[0].first_unchecked().as_match();
		assert_eq!(m.arms.len(), 2);
		assert!(matches!(&m.arms[0], AstMatchArm::Variant { variant_name, guard, .. }
			if variant_name.text() == "Active" && guard.is_some()));
	}

	#[test]
	fn test_mixed_value_and_variant_arms() {
		let bump = Bump::new();
		let source = "MATCH x { 1 => 'one', Active => 'active', ELSE => 'other' }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let m = result[0].first_unchecked().as_match();
		assert_eq!(m.arms.len(), 3);
		assert!(matches!(&m.arms[0], AstMatchArm::Value { .. }));
		assert!(
			matches!(&m.arms[1], AstMatchArm::Variant { variant_name, .. } if variant_name.text() == "Active")
		);
		assert!(matches!(&m.arms[2], AstMatchArm::Else { .. }));
	}

	#[test]
	fn test_variant_arm_multi_field_destructure() {
		let bump = Bump::new();
		let source = "MATCH shape { Rectangle { width, height } => width * height, ELSE => 0 }";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let m = result[0].first_unchecked().as_match();
		assert_eq!(m.arms.len(), 2);
		match &m.arms[0] {
			AstMatchArm::Variant {
				variant_name,
				destructure,
				..
			} => {
				assert_eq!(variant_name.text(), "Rectangle");
				let destr = destructure.as_ref().unwrap();
				assert_eq!(destr.fields.len(), 2);
				assert_eq!(destr.fields[0].text(), "width");
				assert_eq!(destr.fields[1].text(), "height");
			}
			_ => panic!("expected Variant arm"),
		}
	}
}
