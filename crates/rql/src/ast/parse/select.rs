// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::operation::select_missing_braces;
use reifydb_type::return_error;

use crate::{
	ast::{ast::AstMap, parse::Parser},
	token::keyword::Keyword,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_select(&mut self) -> crate::Result<AstMap<'bump>> {
		let token = self.consume_keyword(Keyword::Select)?;

		let (nodes, has_braces) = self.parse_expressions(true, false)?;

		if !has_braces {
			return_error!(select_missing_braces(token.fragment.to_owned()));
		}

		Ok(AstMap {
			token,
			nodes,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{
		ast::ast::{Ast, AstInfix, InfixOperator},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_select_constant_number() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SELECT {1}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SELECT {1 + 2, 4 * 3}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SELECT {*}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Wildcard(_)));
	}

	#[test]
	fn test_select_columns() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SELECT {name, age}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
	fn test_select_colon_alias() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SELECT {a: 1}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
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
		// Colon syntax is converted to AS operator internally: expr AS alias
		let left = left.as_literal_number();
		assert_eq!(left.value(), "1");

		assert!(matches!(operator, InfixOperator::As(_)));

		let right = right.as_identifier();
		assert_eq!(right.text(), "a");
	}

	#[test]
	fn test_select_colon_syntax() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SELECT {total: price * quantity}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);

		let infix = map.nodes[0].as_infix();
		assert!(matches!(infix.operator, InfixOperator::As(_)));

		let left_infix = infix.left.as_infix();
		assert!(matches!(left_infix.operator, InfixOperator::Multiply(_)));
		assert_eq!(left_infix.left.as_identifier().text(), "price");
		assert_eq!(left_infix.right.as_identifier().text(), "quantity");

		let right = infix.right.as_identifier();
		assert_eq!(right.text(), "total");
	}

	#[test]
	fn test_select_mixed_case() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "select {name}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].as_identifier().text(), "name");
	}

	#[test]
	fn test_select_without_braces_fails() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "SELECT 1").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "SELECT_002");
	}
}
