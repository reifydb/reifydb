// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	Result,
	ast::{
		ast::AstList,
		parse::{Parser, Precedence},
	},
	token::{
		operator::{Operator, Operator::CloseBracket},
		separator::Separator,
		token::TokenKind,
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_list(&mut self) -> Result<AstList<'bump>> {
		let token = self.consume_operator(Operator::OpenBracket)?;

		let mut nodes = Vec::with_capacity(4);
		loop {
			self.skip_new_line()?;

			let should_break = if let Ok(current) = self.current() {
				current.is_operator(CloseBracket)
			} else {
				true
			};

			if should_break {
				break;
			}

			self.consume_if(TokenKind::Separator(Separator::Comma))?;

			nodes.push(self.parse_node(Precedence::None)?);
		}

		self.consume_operator(CloseBracket)?;
		Ok(AstList {
			token,
			nodes,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use bumpalo::Bump;

	use crate::{
		ast::{
			ast::{Ast, AstLiteral},
			parse::Parser,
		},
		token::tokenize,
	};

	#[test]
	fn test_empty() {
		let bump = Bump::new();
		let source = "[]";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let Some(Ast::List(list)) = result.nodes.first() else {
			panic!("not list")
		};
		assert_eq!(list.len(), 0);
	}

	#[test]
	fn test_single() {
		let bump = Bump::new();
		let source = "[ 'ReifyDB' ]";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let Some(Ast::List(list)) = result.nodes.first() else {
			panic!("not list")
		};
		assert_eq!(list.len(), 1);

		let Ast::Literal(AstLiteral::Text(literal)) = &list[0] else {
			panic!("not text")
		};
		assert_eq!(literal.value(), "ReifyDB");
	}

	#[test]
	fn test_numbers() {
		let bump = Bump::new();
		let source = "[1, 2.2 , 2.34142]";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let Some(Ast::List(list)) = result.nodes.first() else {
			panic!("not list")
		};
		assert_eq!(list.len(), 3);

		let Ast::Literal(AstLiteral::Number(first)) = &list[0] else {
			panic!("not number")
		};
		assert_eq!(first.value(), "1");

		let Ast::Literal(AstLiteral::Number(second)) = &list[1] else {
			panic!("not number")
		};
		assert_eq!(second.value(), "2.2");

		let Ast::Literal(AstLiteral::Number(third)) = &list[2] else {
			panic!("not number")
		};
		assert_eq!(third.value(), "2.34142");
	}

	#[test]
	fn test_row() {
		let bump = Bump::new();
		let source = "[ { field: 'value' }]";
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let Some(Ast::List(list)) = result.nodes.first() else {
			panic!("not list")
		};
		assert_eq!(list.len(), 1);

		let Ast::Inline(row) = &list[0] else {
			panic!("not inline")
		};
		assert_eq!(row.keyed_values.len(), 1);

		assert_eq!(row.keyed_values[0].key.text(), "field");
		let Ast::Literal(AstLiteral::Text(value_text)) = &*row.keyed_values[0].value else {
			panic!("not text")
		};
		assert_eq!(value_text.value(), "value");
	}
}
