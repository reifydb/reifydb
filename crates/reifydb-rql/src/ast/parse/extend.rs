// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	result::error::diagnostic::operation::extend_multiple_expressions_without_braces,
	return_error,
};

use crate::ast::{AstExtend, parse::Parser, tokenize::Keyword};

impl Parser {
	pub(crate) fn parse_extend(&mut self) -> crate::Result<AstExtend> {
		let token = self.consume_keyword(Keyword::Extend)?;

		let (nodes, has_braces) = self.parse_expressions(true)?;

		if nodes.len() > 1 && !has_braces {
			return_error!(
				extend_multiple_expressions_without_braces(
					token.fragment
				)
			);
		}

		Ok(AstExtend {
			token,
			nodes,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ast::{InfixOperator, tokenize::tokenize};

	#[test]
	fn test_extend_constant_number() {
		let tokens = tokenize("EXTEND 1").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let statement = result.pop().unwrap();
		let ast_node = statement.first_unchecked();

		// Debug print the actual AST node type
		let extend = ast_node.as_extend();
		assert_eq!(extend.nodes.len(), 1);

		let number = extend.nodes[0].as_literal_number();
		assert_eq!(number.value(), "1");
	}

	#[test]
	fn test_extend_colon_syntax() {
		let tokens =
			tokenize("EXTEND total: price * quantity").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let extend = result.first_unchecked().as_extend();
		assert_eq!(extend.nodes.len(), 1);

		// Should be parsed as "price * quantity as total"
		let infix = extend.nodes[0].as_infix();
		assert!(matches!(infix.operator, InfixOperator::As(_)));

		// Left side should be "price * quantity"
		let left_infix = infix.left.as_infix();
		assert!(matches!(
			left_infix.operator,
			InfixOperator::Multiply(_)
		));
		assert_eq!(left_infix.left.as_identifier().value(), "price");
		assert_eq!(
			left_infix.right.as_identifier().value(),
			"quantity"
		);

		// Right side should be identifier "total"
		let right = infix.right.as_identifier();
		assert_eq!(right.value(), "total");
	}

	#[test]
	fn test_extend_multiple_columns() {
		let tokens = tokenize(
			"EXTEND {total: price * quantity, tax: price * 0.1}",
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let extend = result.first_unchecked().as_extend();
		assert_eq!(extend.nodes.len(), 2);

		// First expression: "price * quantity as total"
		let first_infix = extend.nodes[0].as_infix();
		assert!(matches!(first_infix.operator, InfixOperator::As(_)));
		assert_eq!(first_infix.right.as_identifier().value(), "total");

		// Second expression: "price * 0.1 as tax"
		let second_infix = extend.nodes[1].as_infix();
		assert!(matches!(second_infix.operator, InfixOperator::As(_)));
		assert_eq!(second_infix.right.as_identifier().value(), "tax");
	}

	#[test]
	fn test_extend_without_braces_fails() {
		let tokens = tokenize(
			"EXTEND total: price * quantity, tax: price * 0.1",
		)
		.unwrap();
		let mut parser = Parser::new(tokens);

		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "EXTEND_001");
	}
}
