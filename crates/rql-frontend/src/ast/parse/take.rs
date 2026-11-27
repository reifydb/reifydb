// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::error::ValueError;

use crate::{
	ast::{
		ast::{Ast, AstLiteral, AstTake, AstTakeValue},
		parse::{Parser, Precedence},
	},
	diagnostic::AstError,
	token::{keyword::Keyword, operator::Operator},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_take(&mut self) -> Result<AstTake<'bump>, ValueError> {
		let token = self.consume_keyword(Keyword::Take)?;

		let has_braces = !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly);

		if has_braces {
			self.advance()?;
		}

		let take = self.parse_node(Precedence::None)?;

		if has_braces {
			self.consume_operator(Operator::CloseCurly)?;
		}

		match take {
			Ast::Literal(AstLiteral::Number(number)) => {
				let Ok(take_value) = number.value().parse::<i64>() else {
					return Err(AstError::TakeInvalidValue {
						fragment: number.0.fragment.to_owned(),
					}
					.into());
				};
				if take_value < 0 {
					return Err(AstError::TakeNegativeValue {
						fragment: number.0.fragment.to_owned(),
					}
					.into());
				}
				Ok(AstTake {
					token,
					take: AstTakeValue::Literal(take_value as usize),
				})
			}
			Ast::Variable(var) => Ok(AstTake {
				token,
				take: AstTakeValue::Variable(var.token),
			}),
			other => Err(AstError::TakeInvalidValue {
				fragment: other.token().fragment.to_owned(),
			}
			.into()),
		}
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use super::*;
	use crate::token::tokenize;

	#[test]
	fn test_take_with_braces() {
		let bump = Bump::new();
		let source = "TAKE {10}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let Some(Ast::Take(take)) = result.nodes.first() else {
			panic!("not take")
		};
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
		let Some(Ast::Take(take)) = result.nodes.first() else {
			panic!("not take")
		};
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
		let Some(Ast::Take(take)) = result.nodes.first() else {
			panic!("not take")
		};
		assert_eq!(take.take, AstTakeValue::Literal(0));
	}

	#[test]
	fn test_take_negative() {
		assert_eq!(parse_error("TAKE -1").code, "TAKE_001");
	}

	fn parse_error(source: &str) -> ValueError {
		let bump = Bump::new();
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		parser.parse().unwrap_err()
	}

	#[test]
	fn test_take_without_an_integer_operand() {
		for source in ["TAKE 1.5", "TAKE 9223372036854775808", "TAKE true", "TAKE {1 + 1}"] {
			let error = parse_error(source);
			assert_eq!(error.code, "TAKE_002", "{source}");
			assert_eq!(error.message, "TAKE operator requires an integer value", "{source}");
		}
	}
}
