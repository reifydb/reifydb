// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::{
		ast::AstMap,
		parse::{Parser, reject_duplicate_aliases},
	},
	error::OperationKind,
	token::keyword::Keyword,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_map(&mut self) -> Result<AstMap<'bump>> {
		let (token, nodes, rql) =
			self.parse_keyword_with_braced_expressions(Keyword::Map, OperationKind::Map)?;
		reject_duplicate_aliases(&nodes)?;
		Ok(AstMap {
			token,
			nodes,
			rql,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use super::*;
	use crate::{
		ast::ast::{Ast, AstInfix, AstLiteral, InfixOperator},
		token::tokenize,
	};

	#[test]
	fn test_constant_number() {
		let bump = Bump::new();
		let source = "MAP {1}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let Some(Ast::Map(map)) = result.nodes.first() else {
			panic!("not map")
		};
		assert_eq!(map.nodes.len(), 1);

		let Ast::Literal(AstLiteral::Number(number)) = &map.nodes[0] else {
			panic!("not number")
		};
		assert_eq!(number.value(), "1");
	}

	#[test]
	fn test_multiple_expressions() {
		let bump = Bump::new();
		let source = "MAP {1 + 2, 4 * 3}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let Some(Ast::Map(map)) = result.nodes.first() else {
			panic!("not map")
		};
		assert_eq!(map.nodes.len(), 2);

		let Ast::Infix(first) = &map.nodes[0] else {
			panic!("not infix")
		};
		let Ast::Literal(AstLiteral::Number(left_number)) = &*first.left else {
			panic!("not number")
		};
		assert_eq!(left_number.value(), "1");
		assert!(matches!(first.operator, InfixOperator::Add(_)));
		let Ast::Literal(AstLiteral::Number(right_number)) = &*first.right else {
			panic!("not number")
		};
		assert_eq!(right_number.value(), "2");

		let Ast::Infix(second) = &map.nodes[1] else {
			panic!("not infix")
		};
		let Ast::Literal(AstLiteral::Number(left_number)) = &*second.left else {
			panic!("not number")
		};
		assert_eq!(left_number.value(), "4");
		assert!(matches!(second.operator, InfixOperator::Multiply(_)));
		let Ast::Literal(AstLiteral::Number(right_number)) = &*second.right else {
			panic!("not number")
		};
		assert_eq!(right_number.value(), "3");
	}

	#[test]
	fn test_star() {
		let bump = Bump::new();
		let source = "MAP {*}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let Some(Ast::Map(map)) = result.nodes.first() else {
			panic!("not map")
		};
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Wildcard(_)));
	}

	#[test]
	fn test_keyword() {
		let bump = Bump::new();
		let source = "MAP {value}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Map(map)) = result.nodes.first() else {
			panic!("not map")
		};
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].as_identifier().text(), "value");
	}

	#[test]
	fn test_single_column() {
		let bump = Bump::new();
		let source = "MAP {name}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Map(map)) = result.nodes.first() else {
			panic!("not map")
		};
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].as_identifier().text(), "name");
	}

	#[test]
	fn test_multiple_columns() {
		let bump = Bump::new();
		let source = "MAP {name, age}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Map(map)) = result.nodes.first() else {
			panic!("not map")
		};
		assert_eq!(map.nodes.len(), 2);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].as_identifier().text(), "name");

		assert!(matches!(map.nodes[1], Ast::Identifier(_)));
		assert_eq!(map.nodes[1].as_identifier().text(), "age");
	}

	#[test]
	fn test_colon_alias() {
		let bump = Bump::new();
		let source = "MAP {a: 1}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Map(map)) = result.nodes.first() else {
			panic!("not map")
		};
		assert_eq!(map.nodes.len(), 1);

		let Ast::Infix(AstInfix {
			left,
			operator,
			right,
			..
		}) = &map.nodes[0]
		else {
			panic!("not infix")
		};
		// Colon syntax lowers to the AS operator, so the projection is an infix.
		let Ast::Literal(AstLiteral::Number(left)) = &**left else {
			panic!("not number")
		};
		assert_eq!(left.value(), "1");

		assert!(matches!(operator, InfixOperator::As(_)));

		let right = right.as_identifier();
		assert_eq!(right.text(), "a");
	}

	#[test]
	fn test_single_expression_with_braces() {
		let bump = Bump::new();
		let source = "MAP {1}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let Some(Ast::Map(map)) = result.nodes.first() else {
			panic!("not map")
		};
		assert_eq!(map.nodes.len(), 1);

		let Ast::Literal(AstLiteral::Number(number)) = &map.nodes[0] else {
			panic!("not number")
		};
		assert_eq!(number.value(), "1");
	}

	#[test]
	fn test_without_braces_fails() {
		let bump = Bump::new();
		let source = "MAP 1";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "MAP_002");
	}

	#[test]
	fn test_single_column_with_braces() {
		let bump = Bump::new();
		let source = "MAP {name}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Map(map)) = result.nodes.first() else {
			panic!("not map")
		};
		assert_eq!(map.nodes.len(), 1);
		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].as_identifier().text(), "name");
	}

	#[test]
	fn test_colon_syntax_single() {
		let bump = Bump::new();
		let source = "MAP {col: 1 + 2}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let Some(Ast::Map(map)) = result.nodes.first() else {
			panic!("not map")
		};
		assert_eq!(map.nodes.len(), 1);

		let Ast::Infix(infix) = &map.nodes[0] else {
			panic!("not infix")
		};
		assert!(matches!(infix.operator, InfixOperator::As(_)));

		let Ast::Infix(left_infix) = &*infix.left else {
			panic!("not infix")
		};
		assert!(matches!(left_infix.operator, InfixOperator::Add(_)));
		let Ast::Literal(AstLiteral::Number(left_number)) = &*left_infix.left else {
			panic!("not number")
		};
		assert_eq!(left_number.value(), "1");
		let Ast::Literal(AstLiteral::Number(right_number)) = &*left_infix.right else {
			panic!("not number")
		};
		assert_eq!(right_number.value(), "2");

		let right = infix.right.as_identifier();
		assert_eq!(right.text(), "col");
	}

	#[test]
	fn test_colon_syntax_with_braces() {
		let bump = Bump::new();
		let source = "MAP {name: id, age: years}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Map(map)) = result.nodes.first() else {
			panic!("not map")
		};
		assert_eq!(map.nodes.len(), 2);

		let Ast::Infix(first_infix) = &map.nodes[0] else {
			panic!("not infix")
		};
		assert!(matches!(first_infix.operator, InfixOperator::As(_)));
		assert_eq!(first_infix.left.as_identifier().text(), "id");
		assert_eq!(first_infix.right.as_identifier().text(), "name");

		let Ast::Infix(second_infix) = &map.nodes[1] else {
			panic!("not infix")
		};
		assert!(matches!(second_infix.operator, InfixOperator::As(_)));
		assert_eq!(second_infix.left.as_identifier().text(), "years");
		assert_eq!(second_infix.right.as_identifier().text(), "age");
	}

	#[test]
	fn test_colon_syntax_comptokenize_expression() {
		let bump = Bump::new();
		let source = "MAP {total: price * quantity}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Map(map)) = result.nodes.first() else {
			panic!("not map")
		};
		assert_eq!(map.nodes.len(), 1);

		let Ast::Infix(infix) = &map.nodes[0] else {
			panic!("not infix")
		};
		assert!(matches!(infix.operator, InfixOperator::As(_)));

		let Ast::Infix(left_infix) = &*infix.left else {
			panic!("not infix")
		};
		assert!(matches!(left_infix.operator, InfixOperator::Multiply(_)));
		assert_eq!(left_infix.left.as_identifier().text(), "price");
		assert_eq!(left_infix.right.as_identifier().text(), "quantity");

		let right = infix.right.as_identifier();
		assert_eq!(right.text(), "total");
	}

	#[test]
	fn test_mixed_syntax() {
		let bump = Bump::new();
		let source = "MAP {name, total: price * quantity, age}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Map(map)) = result.nodes.first() else {
			panic!("not map")
		};
		assert_eq!(map.nodes.len(), 3);

		assert!(matches!(map.nodes[0], Ast::Identifier(_)));
		assert_eq!(map.nodes[0].as_identifier().text(), "name");

		let Ast::Infix(middle_infix) = &map.nodes[1] else {
			panic!("not infix")
		};
		assert!(matches!(middle_infix.operator, InfixOperator::As(_)));
		assert_eq!(middle_infix.right.as_identifier().text(), "total");

		assert!(matches!(map.nodes[2], Ast::Identifier(_)));
		assert_eq!(map.nodes[2].as_identifier().text(), "age");
	}
}
