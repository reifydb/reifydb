// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{diagnostic::operation, return_error};

use crate::ast::{
	Ast, AstLiteral, AstTake,
	parse::{Parser, Precedence},
	tokenize::Keyword,
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_take(&mut self) -> crate::Result<AstTake<'a>> {
		let token = self.consume_keyword(Keyword::Take)?;
		let take = self.parse_node(Precedence::None)?;
		match take {
			Ast::Literal(literal) => match literal {
				AstLiteral::Number(number) => {
					let take_value: i64 =
						number.value().parse().unwrap();
					if take_value < 0 {
						return_error!(operation::take_negative_value(number.0.fragment));
					}
					Ok(AstTake {
						token,
						take: take_value as usize,
					})
				}
				_ => unimplemented!(),
			},
			_ => unimplemented!(),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ast::tokenize::tokenize;

	#[test]
	fn test_take_number() {
		let tokens = tokenize("TAKE 10").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let take = result.first_unchecked().as_take();
		assert_eq!(take.take, 10);
	}

	#[test]
	fn test_take_zero() {
		let tokens = tokenize("TAKE 0").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let take = result.first_unchecked().as_take();
		assert_eq!(take.take, 0);
	}

	#[test]
	fn test_take_negative() {
		let tokens = tokenize("TAKE -1").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();

		let error = result.unwrap_err();
		assert_eq!(error.code.as_str(), "TAKE_001");
	}
}
