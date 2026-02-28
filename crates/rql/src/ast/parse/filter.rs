// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{Ast, AstFilter},
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{keyword::Keyword, operator::Operator},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_filter(&mut self) -> Result<AstFilter<'bump>> {
		let token = self.consume_keyword(Keyword::Filter)?;

		// Check if braces are used (optional)
		let has_braces = !self.is_eof() && self.current()?.is_operator(Operator::OpenCurly);

		if has_braces {
			self.advance()?; // consume opening brace
		}

		let node = if has_braces && self.current()?.is_operator(Operator::CloseCurly) {
			// Empty braces: filter {}
			Ast::Nop
		} else {
			self.parse_node(Precedence::None)?
		};

		if has_braces {
			self.consume_operator(Operator::CloseCurly)?;
		}

		Ok(AstFilter {
			token,
			node: BumpBox::new_in(node, self.bump()),
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{Ast, InfixOperator},
			parse::Parser,
		},
		bump::Bump,
		token::{keyword::Keyword, token::TokenKind, tokenize},
	};

	#[test]
	fn test_simple_comparison() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "filter {price > 100}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let filter = parser.parse_filter().unwrap();

		assert_eq!(filter.token.kind, TokenKind::Keyword(Keyword::Filter));

		let node = filter.node.as_infix();
		assert_eq!(node.left.as_identifier().text(), "price");
		assert!(matches!(node.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(node.right.as_literal_number().value(), "100");
	}

	#[test]
	fn test_nested_expression() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "filter {(price + fee) > 100}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(node.right.as_literal_number().value(), "100");

		let nested = node.left.as_tuple().nodes[0].as_infix();
		assert_eq!(nested.left.as_identifier().text(), "price");
		assert!(matches!(nested.operator, InfixOperator::Add(_)));
		assert_eq!(nested.right.as_identifier().text(), "fee");
	}

	#[test]
	fn test_filter_without_braces() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "filter price > 100").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert_eq!(node.left.as_identifier().text(), "price");
		assert!(matches!(node.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(node.right.as_literal_number().value(), "100");
	}

	#[test]
	fn test_keyword() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "filter {value > 100}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert_eq!(node.left.as_identifier().text(), "value");
		assert!(matches!(node.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(node.right.as_literal_number().value(), "100");
	}

	#[test]
	fn test_logical_and() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "filter {price > 100 and qty < 50}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::And(_)));

		let left = node.left.as_infix();
		assert_eq!(left.left.as_identifier().text(), "price");
		assert!(matches!(left.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(left.right.as_literal_number().value(), "100");

		let right = node.right.as_infix();
		assert_eq!(right.left.as_identifier().text(), "qty");
		assert!(matches!(right.operator, InfixOperator::LessThan(_)));
		assert_eq!(right.right.as_literal_number().value(), "50");
	}

	#[test]
	fn test_logical_or() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "filter {active == true or premium == true}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::Or(_)));

		let left = node.left.as_infix();
		assert_eq!(left.left.as_identifier().text(), "active");
		assert!(matches!(left.operator, InfixOperator::Equal(_)));

		let right = node.right.as_infix();
		assert_eq!(right.left.as_identifier().text(), "premium");
		assert!(matches!(right.operator, InfixOperator::Equal(_)));
	}

	#[test]
	fn test_logical_xor() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "filter {active == true xor guest == true}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::Xor(_)));

		let left = node.left.as_infix();
		assert_eq!(left.left.as_identifier().text(), "active");
		assert!(matches!(left.operator, InfixOperator::Equal(_)));

		let right = node.right.as_infix();
		assert_eq!(right.left.as_identifier().text(), "guest");
		assert!(matches!(right.operator, InfixOperator::Equal(_)));
	}

	#[test]
	fn test_comptokenize_logical_chain() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "filter {active == true and price > 100 or premium == true}")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::Or(_)));

		let left_and = node.left.as_infix();
		assert!(matches!(left_and.operator, InfixOperator::And(_)));

		let right_or = node.right.as_infix();
		assert_eq!(right_or.left.as_identifier().text(), "premium");
		assert!(matches!(right_or.operator, InfixOperator::Equal(_)));
	}

	#[test]
	fn test_filter_with_braces() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "filter { price > 100 }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let filter = parser.parse_filter().unwrap();

		assert_eq!(filter.token.kind, TokenKind::Keyword(Keyword::Filter));

		let node = filter.node.as_infix();
		assert_eq!(node.left.as_identifier().text(), "price");
		assert!(matches!(node.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(node.right.as_literal_number().value(), "100");
	}

	#[test]
	fn test_filter_comptokenize_expression_with_braces() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "filter { (price + fee) > 100 and active == true }")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::And(_)));

		let left = node.left.as_infix();
		assert!(matches!(left.operator, InfixOperator::GreaterThan(_)));
		assert_eq!(left.right.as_literal_number().value(), "100");

		let nested = left.left.as_tuple().nodes[0].as_infix();
		assert_eq!(nested.left.as_identifier().text(), "price");
		assert!(matches!(nested.operator, InfixOperator::Add(_)));
		assert_eq!(nested.right.as_identifier().text(), "fee");

		let right = node.right.as_infix();
		assert_eq!(right.left.as_identifier().text(), "active");
		assert!(matches!(right.operator, InfixOperator::Equal(_)));
	}

	#[test]
	fn test_filter_without_braces_logical() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "filter active == true and price > 100").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::And(_)));

		let left = node.left.as_infix();
		assert_eq!(left.left.as_identifier().text(), "active");

		let right = node.right.as_infix();
		assert_eq!(right.left.as_identifier().text(), "price");
	}

	#[test]
	fn test_filter_with_braces_logical_operators() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, "filter { active == true or premium == true }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let filter = parser.parse_filter().unwrap();

		let node = filter.node.as_infix();
		assert!(matches!(node.operator, InfixOperator::Or(_)));

		let left = node.left.as_infix();
		assert_eq!(left.left.as_identifier().text(), "active");
		assert!(matches!(left.operator, InfixOperator::Equal(_)));

		let right = node.right.as_infix();
		assert_eq!(right.left.as_identifier().text(), "premium");
		assert!(matches!(right.operator, InfixOperator::Equal(_)));
	}

	#[test]
	fn test_filter_empty_braces() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "filter { }").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse_filter().unwrap();
		assert!(matches!(&*result.node, Ast::Nop));
	}
}
