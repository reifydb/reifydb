// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{ast::AstExtend, parse::Parser},
	error::{OperationKind, RqlError},
	token::keyword::Keyword,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_extend(&mut self) -> crate::Result<AstExtend<'bump>> {
		let token = self.consume_keyword(Keyword::Extend)?;

		let (nodes, has_braces) = self.parse_expressions(true, false)?;

		if !has_braces {
			return Err(RqlError::OperatorMissingBraces {
				kind: OperationKind::Extend,
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		Ok(AstExtend {
			token,
			nodes,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{ast::ast::InfixOperator, bump::Bump, token::tokenize};

	#[test]
	fn test_extend_constant_number() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "EXTEND {1}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let statement = result.pop().unwrap();
		let ast_node = statement.first_unchecked();

		let extend = ast_node.as_extend();
		assert_eq!(extend.nodes.len(), 1);

		let number = extend.nodes[0].as_literal_number();
		assert_eq!(number.value(), "1");
	}

	#[test]
	fn test_extend_colon_syntax() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "EXTEND {total: price * quantity}").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let extend = result.first_unchecked().as_extend();
		assert_eq!(extend.nodes.len(), 1);

		let infix = extend.nodes[0].as_infix();
		assert!(matches!(infix.operator, InfixOperator::As(_)));

		let left_infix = infix.left.as_infix();
		assert!(matches!(left_infix.operator, InfixOperator::Multiply(_)));
		assert_eq!(left_infix.left.as_identifier().text(), "price");
		assert_eq!(left_infix.right.as_identifier().text(), "quantity");

		let right = infix.right.as_identifier();
		assert_eq!(right.text(), "total");
	}

	#[test]
	fn test_extend_multiple_columns() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "EXTEND {total: price * quantity, tax: price * 0.1}")
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, "", tokens);
		let mut result = parser.parse().unwrap();

		let result = result.pop().unwrap();
		let extend = result.first_unchecked().as_extend();
		assert_eq!(extend.nodes.len(), 2);

		let first_infix = extend.nodes[0].as_infix();
		assert!(matches!(first_infix.operator, InfixOperator::As(_)));
		assert_eq!(first_infix.right.as_identifier().text(), "total");

		let second_infix = extend.nodes[1].as_infix();
		assert!(matches!(second_infix.operator, InfixOperator::As(_)));
		assert_eq!(second_infix.right.as_identifier().text(), "tax");
	}

	#[test]
	fn test_extend_without_braces_fails() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "EXTEND 1").unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, "", tokens);

		let result = parser.parse().unwrap_err();
		assert_eq!(result.code, "EXTEND_002");
	}
}
