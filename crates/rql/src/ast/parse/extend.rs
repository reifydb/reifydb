// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::{ast::AstExtend, parse::Parser},
	error::OperationKind,
	token::keyword::Keyword,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_extend(&mut self) -> Result<AstExtend<'bump>> {
		let (token, nodes, rql) =
			self.parse_keyword_with_braced_expressions(Keyword::Extend, OperationKind::Extend)?;
		Ok(AstExtend {
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
		ast::ast::{Ast, AstLiteral, InfixOperator},
		token::tokenize,
	};

	#[test]
	fn test_extend_constant_number() {
		let bump = Bump::new();
		let source = "EXTEND {1}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let statement = result.pop().unwrap();
		let ast_node = statement.nodes.first().unwrap();

		let Ast::Extend(extend) = ast_node else {
			panic!("not extend")
		};
		assert_eq!(extend.nodes.len(), 1);

		let Ast::Literal(AstLiteral::Number(number)) = &extend.nodes[0] else {
			panic!("not number")
		};
		assert_eq!(number.value(), "1");
	}

	#[test]
	fn test_extend_colon_syntax() {
		let bump = Bump::new();
		let source = "EXTEND {total: price * quantity}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Extend(extend)) = result.nodes.first() else {
			panic!("not extend")
		};
		assert_eq!(extend.nodes.len(), 1);

		let Ast::Infix(infix) = &extend.nodes[0] else {
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
	fn test_extend_multiple_columns() {
		let bump = Bump::new();
		let source = "EXTEND {total: price * quantity, tax: price * 0.1}";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let Some(Ast::Extend(extend)) = result.nodes.first() else {
			panic!("not extend")
		};
		assert_eq!(extend.nodes.len(), 2);

		let Ast::Infix(first_infix) = &extend.nodes[0] else {
			panic!("not infix")
		};
		assert!(matches!(first_infix.operator, InfixOperator::As(_)));
		assert_eq!(first_infix.right.as_identifier().text(), "total");

		let Ast::Infix(second_infix) = &extend.nodes[1] else {
			panic!("not infix")
		};
		assert!(matches!(second_infix.operator, InfixOperator::As(_)));
		assert_eq!(second_infix.right.as_identifier().text(), "tax");
	}

	#[test]
	fn test_extend_without_braces_fails() {
		let bump = Bump::new();
		let source = "EXTEND 1";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);

		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "EXTEND_003");
	}
}
