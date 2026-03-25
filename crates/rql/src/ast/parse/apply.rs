// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::AstApply,
		parse::{Parser, Precedence},
	},
	error::{OperationKind, RqlError},
	token::{keyword::Keyword, operator::Operator, separator::Separator},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_apply(&mut self) -> Result<AstApply<'bump>> {
		let start = self.current()?.fragment.offset();
		let token = self.consume_keyword(Keyword::Apply)?;

		let operator = self.parse_as_identifier()?;

		if self.is_eof() || !self.current()?.is_operator(Operator::OpenCurly) {
			return Err(RqlError::OperatorMissingBraces {
				kind: OperationKind::Apply,
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		self.advance()?;

		let mut expressions = Vec::new();

		if !self.current()?.is_operator(Operator::CloseCurly) {
			loop {
				expressions.push(self.parse_node(Precedence::None)?);

				if self.current()?.is_separator(Separator::Comma) {
					self.advance()?;
					if self.current()?.is_operator(Operator::CloseCurly) {
						break;
					}
				} else {
					break;
				}
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstApply {
			token,
			operator,
			expressions,
			rql: self.source_since(start),
		})
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{bump::Bump, token::tokenize};

	#[test]
	fn test_apply_counter_no_args() {
		let bump = Bump::new();
		let source = "APPLY counter {}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let apply = result.first_unchecked().as_apply();
		assert_eq!(apply.operator.token.value(), "counter");
		assert_eq!(apply.expressions.len(), 0);
	}

	#[test]
	fn test_apply_with_single_expression() {
		let bump = Bump::new();
		let source = "APPLY running_sum {value}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let apply = result.first_unchecked().as_apply();
		assert_eq!(apply.operator.text(), "running_sum");
		assert_eq!(apply.expressions.len(), 1);
		assert_eq!(apply.expressions[0].as_identifier().text(), "value");
	}

	#[test]
	fn test_apply_with_block() {
		let bump = Bump::new();
		let source = "APPLY counter {row_number: row_number, id: id}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let apply = result.first_unchecked().as_apply();
		assert_eq!(apply.operator.token.value(), "counter");
		assert_eq!(apply.expressions.len(), 2);
	}

	#[test]
	fn test_apply_without_braces_fails() {
		let bump = Bump::new();
		let source = "APPLY some_op value";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "APPLY_002");
	}
}
