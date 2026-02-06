// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::operation::apply_missing_braces;
use reifydb_type::return_error;

use crate::{
	ast::{
		ast::AstApply,
		parse::{Parser, Precedence},
	},
	token::{keyword::Keyword, operator::Operator, separator::Separator},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_apply(&mut self) -> crate::Result<AstApply<'bump>> {
		let token = self.consume_keyword(Keyword::Apply)?;

		let operator = self.parse_identifier()?;

		if self.is_eof() || !self.current()?.is_operator(Operator::OpenCurly) {
			return_error!(apply_missing_braces(token.fragment.to_owned()));
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
		let tokens = tokenize(&bump, "APPLY counter {}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let tokens = tokenize(&bump, "APPLY running_sum {value}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let tokens = tokenize(&bump, "APPLY counter {row_number: row_number, id: id}")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let tokens = tokenize(&bump, "APPLY some_op value").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "APPLY_002");
	}
}
