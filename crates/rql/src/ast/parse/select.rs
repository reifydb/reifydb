// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::return_error;
use reifydb_type::diagnostic::operation::select_multiple_expressions_without_braces;

use crate::ast::{AstMap, parse::Parser, tokenize::Keyword};

impl Parser {
	/// Parse SELECT statement - this is an alias for MAP that delegates to
	/// the same logic
	pub(crate) fn parse_select(&mut self) -> crate::Result<AstMap> {
		let token = self.consume_keyword(Keyword::Select)?;

		let (nodes, has_braces) = self.parse_expressions(true)?;

		if nodes.len() > 1 && !has_braces {
			return_error!(select_multiple_expressions_without_braces(token.fragment));
		}

		Ok(AstMap {
			token,
			nodes,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ast::{Ast, AstInfix, InfixOperator, tokenize::tokenize};

	#[test]
	fn test_select_constant_number() {
		let tokens = tokenize("SELECT 1").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);

		let number = map.nodes[0].as_literal_number();
		assert_eq!(number.value(), "1");
	}

	#[test]
	fn test_select_multiple_expressions() {
		let tokens = tokenize("SELECT {1 + 2, 4 * 3}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 2);

		let first = map.nodes[0].as_infix();
		assert_eq!(first.left.as_literal_number().value(), "1");
		assert!(matches!(first.operator, InfixOperator::Add(_)));
		assert_eq!(first.right.as_literal_number().value(), "2");

		let second = map.nodes[1].as_infix();
		assert_eq!(second.left.as_literal_number().value(), "4");
		assert!(matches!(second.operator, InfixOperator::Multiply(_)));
		assert_eq!(second.right.as_literal_number().value(), "3");
	}

	#[test]
	fn test_select_star() {
		let tokens = tokenize("SELECT *").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Wildcard(_)));
	}

	#[test]
	fn test_select_columns() {
		let tokens = tokenize("SELECT {name, age}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 2);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].as_identifier().text(), "name");

		assert!(matches!(map.nodes[1], Ast::Identifier(_)));
		assert_eq!(map.nodes[1].as_identifier().text(), "age");
	}

	#[test]
	fn test_select_with_as() {
		let tokens = tokenize("SELECT 1 as a").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);

		let AstInfix {
			left,
			operator,
			right,
			..
		} = map.nodes[0].as_infix();
		let left = left.as_literal_number();
		assert_eq!(left.value(), "1");

		assert!(matches!(operator, InfixOperator::As(_)));

		let right = right.as_identifier();
		assert_eq!(right.text(), "a");
	}

	#[test]
	fn test_select_colon_syntax() {
		let tokens = tokenize("SELECT total: price * quantity").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);

		// Should be parsed as "price * quantity as total"
		let infix = map.nodes[0].as_infix();
		assert!(matches!(infix.operator, InfixOperator::As(_)));

		// Left side should be "price * quantity"
		let left_infix = infix.left.as_infix();
		assert!(matches!(left_infix.operator, InfixOperator::Multiply(_)));
		assert_eq!(left_infix.left.as_identifier().text(), "price");
		assert_eq!(left_infix.right.as_identifier().text(), "quantity");

		// Right side should be identifier "total"
		let right = infix.right.as_identifier();
		assert_eq!(right.text(), "total");
	}

	#[test]
	fn test_select_mixed_case() {
		// Test that SELECT is case-insensitive
		let tokens = tokenize("select name").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].as_identifier().text(), "name");
	}

	#[test]
	fn test_select_multiple_without_braces_fails() {
		let tokens = tokenize("SELECT 1, 2").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "SELECT_001");
	}
}
