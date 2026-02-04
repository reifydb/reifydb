// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::operation;
use reifydb_type::return_error;

use crate::ast::{
	ast::{Ast, AstLiteral, AstTake},
	parse::{Parser, Precedence},
	tokenize::{keyword::Keyword, operator::Operator},
};

impl Parser {
	pub(crate) fn parse_take(&mut self) -> crate::Result<AstTake> {
		let token = self.consume_keyword(Keyword::Take)?;

		// Check if braces are used (optional)
		let has_braces = !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly);

		if has_braces {
			self.advance()?; // consume opening brace
		}

		let take = self.parse_node(Precedence::None)?;

		if has_braces {
			self.consume_operator(Operator::CloseCurly)?;
		}

		match take {
			Ast::Literal(literal) => match literal {
				AstLiteral::Number(number) => {
					let take_value: i64 = number.value().parse().unwrap();
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
pub mod tests {
	use super::*;
	use crate::ast::tokenize::tokenize;

	#[test]
	fn test_take_with_braces() {
		let tokens = tokenize("TAKE {10}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let take = result.first_unchecked().as_take();
		assert_eq!(take.take, 10);
	}

	#[test]
	fn test_take_without_braces() {
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
