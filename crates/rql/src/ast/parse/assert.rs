// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::AstAssert,
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{
		keyword::Keyword,
		operator::Operator,
		token::{Literal, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_assert(&mut self) -> Result<AstAssert<'bump>> {
		let token = self.consume_keyword(Keyword::Assert)?;
		self.consume_operator(Operator::OpenCurly)?;
		let node = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());
		self.consume_operator(Operator::CloseCurly)?;

		// Optionally consume a trailing string literal message
		let message = if !self.is_eof() && self.current()?.kind == TokenKind::Literal(Literal::Text) {
			Some(self.advance()?)
		} else {
			None
		};

		Ok(AstAssert {
			token,
			node,
			message,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{ast::Ast, parse::parse},
		bump::Bump,
		token::{keyword::Keyword, token::TokenKind, tokenize},
	};

	#[test]
	fn test_assert_simple() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "ASSERT { 1 + 1 == 2 }").unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let assert_node = result[0].first_unchecked().as_assert();
		assert_eq!(assert_node.token.kind, TokenKind::Keyword(Keyword::Assert));
		assert!(assert_node.message.is_none());
	}

	#[test]
	fn test_assert_with_message() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"ASSERT { x > 0 } "must be positive""#).unwrap().into_iter().collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let assert_node = result[0].first_unchecked().as_assert();
		assert!(assert_node.message.is_some());
	}

	#[test]
	fn test_assert_in_pipeline() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "FROM users | ASSERT { count(*) > 0 } | MAP { name }")
			.unwrap()
			.into_iter()
			.collect();
		let result = parse(&bump, "", tokens).unwrap();
		assert_eq!(result.len(), 1);

		let statement = &result[0];
		assert_eq!(statement.len(), 3);
		assert!(matches!(statement[0], Ast::From(_)));
		assert!(matches!(statement[1], Ast::Assert(_)));
		assert!(matches!(statement[2], Ast::Map(_)));
	}
}
