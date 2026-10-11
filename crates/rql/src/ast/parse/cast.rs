// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::{
		ast::AstCast,
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	diagnostic::AstError,
	token::{
		keyword::Keyword::Cast,
		operator::Operator::{CloseParen, OpenParen},
		separator::Separator::Comma,
		token::TokenKind,
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_cast(&mut self) -> Result<AstCast<'bump>> {
		let token = self.consume_keyword(Cast)?;
		let paren = self.consume_operator(OpenParen)?;
		self.skip_new_line()?;
		let expression = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());
		if self.consume_if(TokenKind::Separator(Comma))?.is_none() {
			return Err(AstError::UnexpectedToken {
				expected: "cast(value, type)".to_string(),
				fragment: self.current()?.fragment.to_owned(),
			}
			.into());
		}
		self.skip_new_line()?;
		let ty = self.parse_type()?;
		self.skip_new_line()?;
		self.consume_operator(CloseParen)?;
		Ok(AstCast {
			token,
			paren,
			expression,
			ty,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::{
		ast::{
			ast::{AstCast, AstType},
			parse::parse,
		},
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
			ty,
			..
		} = result[0].first_unchecked().as_cast();
		assert_eq!(expression.as_literal_number().value(), "9924");
		assert!(matches!(ty, AstType::Unconstrained(name) if name.text() == "int8"));
	}
}
