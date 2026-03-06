// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{Ast, AstLiteral, AstTake, AstTakeValue},
		parse::{Parser, Precedence},
	},
	error::RqlError,
	token::{keyword::Keyword, operator::Operator},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_take(&mut self) -> Result<AstTake<'bump>> {
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
						return Err(RqlError::TakeNegativeValue {
							fragment: number.0.fragment.to_owned(),
						}
						.into());
					}
					Ok(AstTake {
						token,
						take: AstTakeValue::Literal(take_value as usize),
					})
				}
				_ => unimplemented!(),
			},
			Ast::Variable(var) => Ok(AstTake {
				token,
				take: AstTakeValue::Variable(var.token),
			}),
			_ => unimplemented!(),
		}
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{bump::Bump, token::tokenize};

	#[test]
	fn test_take_with_braces() {
		let bump = Bump::new();
		let source = "TAKE {10}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let take = result.first_unchecked().as_take();
		assert_eq!(take.take, AstTakeValue::Literal(10));
	}

	#[test]
	fn test_take_without_braces() {
		let bump = Bump::new();
		let source = "TAKE 10";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let take = result.first_unchecked().as_take();
		assert_eq!(take.take, AstTakeValue::Literal(10));
	}

	#[test]
	fn test_take_zero() {
		let bump = Bump::new();
		let source = "TAKE 0";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let take = result.first_unchecked().as_take();
		assert_eq!(take.take, AstTakeValue::Literal(0));
	}

	#[test]
	fn test_take_negative() {
		let bump = Bump::new();
		let source = "TAKE -1";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse();

		let error = result.unwrap_err();
		assert_eq!(error.code.as_str(), "TAKE_001");
	}
}
