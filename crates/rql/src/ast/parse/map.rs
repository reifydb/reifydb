// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{ast::AstMap, parse::Parser},
	error::{OperationKind, RqlError},
	token::keyword::Keyword,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_map(&mut self) -> Result<AstMap<'bump>> {
		let token = self.consume_keyword(Keyword::Map)?;

		let (nodes, has_braces) = self.parse_expressions(true, false)?;

		// Always require braces
		if !has_braces {
			return Err(RqlError::OperatorMissingBraces {
				kind: OperationKind::Map,
				fragment: token.fragment.to_owned(),
			}
			.into());
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
	fn test_constant_number() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP {1}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);

		let number = map.nodes[0].as_literal_number();
		assert_eq!(number.value(), "1");
	}

	#[test]
	fn test_multiple_expressions() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP {1 + 2, 4 * 3}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
	fn test_star() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP {*}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Wildcard(_)));
	}

	#[test]
	fn test_keyword() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP {value}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].as_identifier().text(), "value");
	}

	#[test]
	fn test_single_column() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP {name}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].as_identifier().text(), "name");
	}

	#[test]
	fn test_multiple_columns() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP {name, age}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
	fn test_colon_alias() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP {a: 1}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
	fn test_single_expression_with_braces() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP {1}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);

		let number = map.nodes[0].as_literal_number();
		assert_eq!(number.value(), "1");
	}

	#[test]
	fn test_without_braces_fails() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP 1").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "MAP_002");
	}

	#[test]
	fn test_single_column_with_braces() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP {name}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].as_identifier().text(), "name");
	}

	#[test]
	fn test_colon_syntax_single() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP {col: 1 + 2}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);

		let infix = map.nodes[0].as_infix();
		assert!(matches!(infix.operator, InfixOperator::As(_)));

		let left_infix = infix.left.as_infix();
		assert!(matches!(left_infix.operator, InfixOperator::Add(_)));
		assert_eq!(left_infix.left.as_literal_number().value(), "1");
		assert_eq!(left_infix.right.as_literal_number().value(), "2");

		let right = infix.right.as_identifier();
		assert_eq!(right.text(), "col");
	}

	#[test]
	fn test_colon_syntax_with_braces() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP {name: id, age: years}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 2);

		let first_infix = map.nodes[0].as_infix();
		assert!(matches!(first_infix.operator, InfixOperator::As(_)));
		assert_eq!(first_infix.left.as_identifier().text(), "id");
		assert_eq!(first_infix.right.as_identifier().text(), "name");

		let second_infix = map.nodes[1].as_infix();
		assert!(matches!(second_infix.operator, InfixOperator::As(_)));
		assert_eq!(second_infix.left.as_identifier().text(), "years");
		assert_eq!(second_infix.right.as_identifier().text(), "age");
	}

	#[test]
	fn test_colon_syntax_comptokenize_expression() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP {total: price * quantity}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
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
	fn test_mixed_syntax() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "MAP {name, total: price * quantity, age}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 3);

		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].as_identifier().text(), "name");

		let middle_infix = map.nodes[1].as_infix();
		assert!(matches!(middle_infix.operator, InfixOperator::As(_)));
		assert_eq!(middle_infix.right.as_identifier().text(), "total");

		assert!(matches!(map.nodes[2], Ast::Identifier(_)));
		assert_eq!(map.nodes[2].as_identifier().text(), "age");
	}
}
