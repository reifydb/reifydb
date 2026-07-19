// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::{
		ast::AstCast,
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{
		keyword::Keyword::Cast,
		operator::Operator::{As, CloseParen, OpenParen},
		separator::Separator::Comma,
		token::TokenKind,
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_cast(&mut self) -> Result<AstCast<'bump>> {
		let token = self.consume_keyword(Cast)?;
		let open = self.consume_operator(OpenParen)?;

		let expression = BumpBox::new_in(self.parse_node(Precedence::Assignment)?, self.bump());
		if self.current()?.is_operator(As) {
			self.consume_operator(As)?;
		} else {
			self.consume(TokenKind::Separator(Comma))?;
		}
		let to = self.parse_type()?;
		self.consume_operator(CloseParen)?;
		Ok(AstCast {
			token,
			open,
			expression,
			to,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{AstCast, AstType},
			parse::parse,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_cast() {
		let bump = Bump::new();
		let source = "cast(9924, int8)";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let AstCast {
			expression,
			to,
			..
		} = result[0].first_unchecked().as_cast();
		assert_eq!(expression.as_literal_number().value(), "9924");
		assert!(matches!(to, AstType::Unconstrained(name) if name.text() == "int8"));
	}

	#[test]
	fn test_cast_constrained() {
		let bump = Bump::new();
		let source = "cast('abcdef', utf8(3))";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let result = parse(&bump, source, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let AstCast {
			to,
			..
		} = result[0].first_unchecked().as_cast();
		match to {
			AstType::Constrained {
				name,
				params,
			} => {
				assert_eq!(name.text(), "utf8");
				assert_eq!(params.len(), 1);
			}
			other => panic!("expected a constrained type, got {other:?}"),
		}
	}
}
