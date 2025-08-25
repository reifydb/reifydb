// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::ast,
	result::error::diagnostic::ast::multiple_expressions_without_braces,
	return_error,
};

use crate::ast::{
	Ast, AstInfix, AstMap, InfixOperator,
	parse::{Parser, Precedence},
	tokenize::{
		Keyword,
		Operator::{CloseCurly, Colon, OpenCurly},
		Separator::Comma,
		TokenKind,
	},
};

impl Parser {
	pub(crate) fn parse_map(&mut self) -> crate::Result<AstMap> {
		let token = self.consume_keyword(Keyword::Map)?;

		// Check if we have an opening brace
		let has_braces = self.current()?.is_operator(OpenCurly);

		if has_braces {
			self.advance()?; // consume opening brace
		}

		let mut nodes = Vec::new();
		loop {
			// Try to parse colon-based syntax first (e.g., "col:
			// expression")
			if let Ok(alias_expr) = self.try_parse_colon_alias() {
				nodes.push(alias_expr);
			} else {
				nodes.push(self.parse_node(Precedence::None)?);
			}

			if self.is_eof() {
				break;
			}

			// consume comma and continue
			if self.current()?.is_separator(Comma) {
				self.advance()?;
			} else if has_braces
				&& self.current()?.is_operator(CloseCurly)
			{
				// If we have braces, look for closing brace
				self.advance()?; // consume closing brace
				break;
			} else {
				break;
			}
		}

		if nodes.len() > 1 && !has_braces {
			return_error!(multiple_expressions_without_braces(
				token.fragment
			));
		}

		Ok(AstMap {
			token,
			nodes,
		})
	}

	/// Try to parse "identifier: expression" syntax and convert it to
	/// "expression AS identifier"
	fn try_parse_colon_alias(&mut self) -> crate::Result<Ast> {
		let len = self.tokens.len();

		// Look ahead to see if we have "identifier: expression" pattern
		if len < 2 {
			return_error!(ast::unsupported_token_error(
				self.current()?.clone().fragment
			));
		}

		// Check if next token is identifier
		match &self.tokens[len - 1].kind {
			TokenKind::Identifier => {}
			_ => return_error!(ast::unsupported_token_error(
				self.current()?.clone().fragment
			)),
		};

		// Check if second token is colon
		if !self.tokens[len - 2].is_operator(Colon) {
			return_error!(ast::unsupported_token_error(
				self.current()?.clone().fragment
			));
		}

		let identifier = self.parse_as_identifier()?;
		let colon_token = self.advance()?; // consume colon

		let expression = self.parse_node(Precedence::None)?;

		Ok(Ast::Infix(AstInfix {
			token: expression.token().clone(),
			left: Box::new(expression),
			operator: InfixOperator::As(colon_token),
			right: Box::new(Ast::Identifier(identifier)),
		}))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ast::{Ast, AstInfix, InfixOperator, tokenize::tokenize};

	#[test]
	fn test_constant_number() {
		let tokens = tokenize("MAP 1").unwrap();
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
	fn test_multiple_expressions() {
		let tokens = tokenize("MAP {1 + 2, 4 * 3}").unwrap();
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
	fn test_star() {
		let tokens = tokenize("MAP *").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Wildcard(_)));
	}

	#[test]
	fn test_keyword() {
		let tokens = tokenize("MAP value").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].value(), "value");
	}

	#[test]
	fn test_single_column() {
		let tokens = tokenize("MAP name").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].value(), "name");
	}

	#[test]
	fn test_multiple_columns() {
		let tokens = tokenize("MAP {name, age}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 2);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].value(), "name");

		assert!(matches!(map.nodes[1], Ast::Identifier(_)));
		assert_eq!(map.nodes[1].value(), "age");
	}

	#[test]
	fn test_as() {
		let tokens = tokenize("map 1 as a").unwrap();
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
		assert_eq!(right.value(), "a");
	}

	#[test]
	fn test_single_expression_with_braces() {
		let tokens = tokenize("MAP {1}").unwrap();
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
	fn test_multiple_expressions_without_braces_fails() {
		let tokens = tokenize("MAP 1, 2").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse();

		assert!(
			result.is_err(),
			"Expected error for multiple expressions without braces"
		);
	}

	#[test]
	fn test_single_column_with_braces() {
		let tokens = tokenize("MAP {name}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].value(), "name");
	}

	#[test]
	fn test_colon_syntax_single() {
		let tokens = tokenize("MAP col: 1 + 2").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 1);

		// Should be parsed as "1 + 2 as col"
		let infix = map.nodes[0].as_infix();
		assert!(matches!(infix.operator, InfixOperator::As(_)));

		// Left side should be "1 + 2"
		let left_infix = infix.left.as_infix();
		assert!(matches!(left_infix.operator, InfixOperator::Add(_)));
		assert_eq!(left_infix.left.as_literal_number().value(), "1");
		assert_eq!(left_infix.right.as_literal_number().value(), "2");

		// Right side should be identifier "col"
		let right = infix.right.as_identifier();
		assert_eq!(right.value(), "col");
	}

	#[test]
	fn test_colon_syntax_with_braces() {
		let tokens = tokenize("MAP {name: id, age: years}").unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 2);

		// First expression: "id as name"
		let first_infix = map.nodes[0].as_infix();
		assert!(matches!(first_infix.operator, InfixOperator::As(_)));
		assert_eq!(first_infix.left.as_identifier().value(), "id");
		assert_eq!(first_infix.right.as_identifier().value(), "name");

		// Second expression: "years as age"
		let second_infix = map.nodes[1].as_infix();
		assert!(matches!(second_infix.operator, InfixOperator::As(_)));
		assert_eq!(second_infix.left.as_identifier().value(), "years");
		assert_eq!(second_infix.right.as_identifier().value(), "age");
	}

	#[test]
	fn test_colon_syntax_comptokenize_expression() {
		let tokens = tokenize("MAP total: price * quantity").unwrap();
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
	fn test_mixed_syntax() {
		let tokens =
			tokenize("MAP {name, total: price * quantity, age}")
				.unwrap();
		let mut parser = Parser::new(tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let map = result.first_unchecked().as_map();
		assert_eq!(map.nodes.len(), 3);

		// First: plain identifier
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].value(), "name");

		// Second: colon syntax
		let middle_infix = map.nodes[1].as_infix();
		assert!(matches!(middle_infix.operator, InfixOperator::As(_)));
		assert_eq!(middle_infix.right.as_identifier().value(), "total");

		// Third: plain identifier
		assert!(matches!(map.nodes[2], Ast::Identifier(_)));
		assert_eq!(map.nodes[2].value(), "age");
	}
}
