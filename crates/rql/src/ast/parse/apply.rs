// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::return_error;
use reifydb_type::diagnostic::operation::apply_multiple_arguments_without_braces;

use crate::ast::{
	AstApply,
	parse::{Parser, Precedence},
	tokenize::{Keyword, Operator, Separator},
};

impl Parser {
	pub(crate) fn parse_apply(&mut self) -> crate::Result<AstApply> {
		let token = self.consume_keyword(Keyword::Apply)?;

		// Parse the operator name (identifier)
		let operator = self.parse_identifier()?;

		// Check if we have arguments
		let expressions = if self.current()?.is_operator(Operator::OpenCurly) {
			// We have a block - could be empty {} or
			// contain expressions
			self.advance()?; // consume '{'

			let mut exprs = Vec::new();

			// Check if it's empty braces
			if !self.current()?.is_operator(Operator::CloseCurly) {
				// Parse expressions until we hit the
				// closing brace
				loop {
					exprs.push(self.parse_node(Precedence::None)?);

					if self.current()?.is_separator(Separator::Comma) {
						self.advance()?; // consume comma
						// Check for trailing
						// comma
						if self.current()?.is_operator(Operator::CloseCurly) {
							break;
						}
					} else {
						break;
					}
				}
			}

			self.consume_operator(Operator::CloseCurly)?; // consume '}'
			exprs
		} else if !self.is_eof()
			&& !self.current()?.is_separator(Separator::NewLine)
			&& !self.current()?.is_keyword(Keyword::Map)
			&& !self.current()?.is_keyword(Keyword::Filter)
			&& !self.current()?.is_keyword(Keyword::From)
		{
			// Try to parse a single expression
			let first_expr = self.parse_node(Precedence::None)?;

			// Check if there's a comma following (which
			// would indicate multiple arguments)
			if !self.is_eof() && self.current()?.is_separator(Separator::Comma) {
				// Multiple arguments without braces -
				// this is an error
				return_error!(apply_multiple_arguments_without_braces(token.fragment));
			}

			vec![first_expr]
		} else {
			// No arguments
			Vec::new()
		};

		Ok(AstApply {
			token,
			operator,
			expressions,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ast::tokenize::tokenize;

	#[test]
	fn test_apply_counter_no_args() {
		let tokens = tokenize("APPLY counter {}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let apply = result.first_unchecked().as_apply();
		assert_eq!(apply.operator.token.value(), "counter");
		assert_eq!(apply.expressions.len(), 0);
	}

	#[test]
	fn test_apply_with_single_expression() {
		let tokens = tokenize("APPLY running_sum value").unwrap();
		let mut parser = Parser::new(tokens);
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
		let tokens = tokenize("APPLY counter {row_number: row_number, id: id}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let apply = result.first_unchecked().as_apply();
		assert_eq!(apply.operator.token.value(), "counter");
		assert_eq!(apply.expressions.len(), 2);
	}

	#[test]
	fn test_apply_multiple_without_braces_fails() {
		let tokens = tokenize("APPLY some_op value1, value2").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "APPLY_001");
	}
}
