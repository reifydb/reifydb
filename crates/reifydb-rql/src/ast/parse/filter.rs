// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{
	Ast, AstFilter,
	parse::{Parser, Precedence},
	tokenize::{Keyword, Operator},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_filter(&mut self) -> crate::Result<AstFilter<'a>> {
		let token = self.consume_keyword(Keyword::Filter)?;

		// Check if we have an opening brace
		let has_braces = !self.is_eof()
			&& self.current()?.is_operator(Operator::OpenCurly);

		if has_braces {
			self.advance()?;
		}

		// Handle case where there's no expression (like "filter {}"
		// alone)
		let node = if self.is_eof()
			|| (has_braces
				&& self.current()?
					.is_operator(Operator::CloseCurly))
		{
			Ast::Nop
		} else {
			self.parse_node(Precedence::None)?
		};

		if has_braces {
			self.consume_operator(Operator::CloseCurly)?; // consume closing brace
		}

		Ok(AstFilter {
			token,
			node: Box::new(node),
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{
		Ast, InfixOperator, TokenKind,
		parse::Parser,
		tokenize::{Keyword, tokenize},
	};

	#[test]
	fn test_simple_comparison() {
		let tokens = tokenize("filter price > 100").unwrap();
		let mut parser = Parser::new(tokens);
		let filter = parser.parse_filter().unwrap();

		assert_eq!(
			filter.token.kind,
			TokenKind::Keyword(Keyword::Filter)
		);

		let node = filter.node.as_infix();
		assert_eq!(node.left.as_identifier().name(), "price");
		assert!(matches!(node.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(node.right.as_literal_number().value(), "100");
	}

	#[test]
	fn test_nested_expression() {
		let tokens = tokenize("filter (price + fee) > 100").unwrap();
		let mut parser = Parser::new(tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(node.right.as_literal_number().value(), "100");

		let nested = node.left.as_tuple().nodes[0].as_infix();
		assert_eq!(nested.left.as_identifier().name(), "price");
		assert!(matches!(nested.operator, InfixOperator::Add(_)));
		assert_eq!(nested.right.as_identifier().name(), "fee");
	}

	#[test]
	fn test_filter_missing_expression() {
		let tokens = tokenize("filter").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse_filter().unwrap();
		assert_eq!(*result.node, Ast::Nop);
	}

	#[test]
	fn test_keyword() {
		let tokens = tokenize("filter value > 100").unwrap();
		let mut parser = Parser::new(tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert_eq!(node.left.as_identifier().name(), "value");
		assert!(matches!(node.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(node.right.as_literal_number().value(), "100");
	}

	#[test]
	fn test_logical_and() {
		let tokens =
			tokenize("filter price > 100 and qty < 50").unwrap();
		let mut parser = Parser::new(tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::And(_)));

		let left = node.left.as_infix();
		assert_eq!(left.left.as_identifier().name(), "price");
		assert!(matches!(left.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(left.right.as_literal_number().value(), "100");

		let right = node.right.as_infix();
		assert_eq!(right.left.as_identifier().name(), "qty");
		assert!(matches!(right.operator, InfixOperator::LessThan(_)));
		assert_eq!(right.right.as_literal_number().value(), "50");
	}

	#[test]
	fn test_logical_or() {
		let tokens =
			tokenize("filter active == true or premium == true")
				.unwrap();
		let mut parser = Parser::new(tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::Or(_)));

		let left = node.left.as_infix();
		assert_eq!(left.left.as_identifier().name(), "active");
		assert!(matches!(left.operator, InfixOperator::Equal(_)));

		let right = node.right.as_infix();
		assert_eq!(right.left.as_identifier().name(), "premium");
		assert!(matches!(right.operator, InfixOperator::Equal(_)));
	}

	#[test]
	fn test_logical_xor() {
		let tokens =
			tokenize("filter active == true xor guest == true")
				.unwrap();
		let mut parser = Parser::new(tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::Xor(_)));

		let left = node.left.as_infix();
		assert_eq!(left.left.as_identifier().name(), "active");
		assert!(matches!(left.operator, InfixOperator::Equal(_)));

		let right = node.right.as_infix();
		assert_eq!(right.left.as_identifier().name(), "guest");
		assert!(matches!(right.operator, InfixOperator::Equal(_)));
	}

	#[test]
	fn test_comptokenize_logical_chain() {
		let tokens = tokenize(
			"filter active == true and price > 100 or premium == true",
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let filter = parser.parse_filter().unwrap();

		// Should parse as: (active == true and price > 100) or premium
		// == true Due to precedence, AND has higher precedence than
		// OR
		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::Or(_)));

		let left_and = node.left.as_infix();
		assert!(matches!(left_and.operator, InfixOperator::And(_)));

		let right_or = node.right.as_infix();
		assert_eq!(right_or.left.as_identifier().name(), "premium");
		assert!(matches!(right_or.operator, InfixOperator::Equal(_)));
	}

	#[test]
	fn test_filter_with_braces() {
		let tokens = tokenize("filter { price > 100 }").unwrap();
		let mut parser = Parser::new(tokens);
		let filter = parser.parse_filter().unwrap();

		assert_eq!(
			filter.token.kind,
			TokenKind::Keyword(Keyword::Filter)
		);

		let node = filter.node.as_infix();
		assert_eq!(node.left.as_identifier().name(), "price");
		assert!(matches!(node.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(node.right.as_literal_number().value(), "100");
	}

	#[test]
	fn test_filter_comptokenize_expression_with_braces() {
		let tokens = tokenize(
			"filter { (price + fee) > 100 and active == true }",
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::And(_)));

		// Left side: (price + fee) > 100
		let left = node.left.as_infix();
		assert!(matches!(left.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(left.right.as_literal_number().value(), "100");

		let nested = left.left.as_tuple().nodes[0].as_infix();
		assert_eq!(nested.left.as_identifier().name(), "price");
		assert!(matches!(nested.operator, InfixOperator::Add(_)));
		assert_eq!(nested.right.as_identifier().name(), "fee");

		// Right side: active == true
		let right = node.right.as_infix();
		assert_eq!(right.left.as_identifier().name(), "active");
		assert!(matches!(right.operator, InfixOperator::Equal(_)));
	}

	#[test]
	fn test_filter_without_braces_still_works() {
		let tokens = tokenize("filter price > 100").unwrap();
		let mut parser = Parser::new(tokens);
		let filter = parser.parse_filter().unwrap();

		assert_eq!(
			filter.token.kind,
			TokenKind::Keyword(Keyword::Filter)
		);

		let node = filter.node.as_infix();
		assert_eq!(node.left.as_identifier().name(), "price");
		assert!(matches!(node.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(node.right.as_literal_number().value(), "100");
	}

	#[test]
	fn test_filter_with_braces_logical_operators() {
		let tokens = tokenize(
			"filter { active == true or premium == true }",
		)
		.unwrap();
		let mut parser = Parser::new(tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::Or(_)));

		let left = node.left.as_infix();
		assert_eq!(left.left.as_identifier().name(), "active");
		assert!(matches!(left.operator, InfixOperator::Equal(_)));

		let right = node.right.as_infix();
		assert_eq!(right.left.as_identifier().name(), "premium");
		assert!(matches!(right.operator, InfixOperator::Equal(_)));
	}

	#[test]
	fn test_filter_empty_braces() {
		let tokens = tokenize("filter { }").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse_filter().unwrap();
		assert_eq!(*result.node, Ast::Nop);
	}
}
