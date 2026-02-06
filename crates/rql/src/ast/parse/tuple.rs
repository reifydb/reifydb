// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::AstTuple,
		parse::{Parser, Precedence},
	},
	token::{
		operator::{Operator, Operator::CloseParen},
		separator::Separator::Comma,
		token::{Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_tuple(&mut self) -> crate::Result<AstTuple<'bump>> {
		let token = self.consume_operator(Operator::OpenParen)?;
		self.parse_tuple_call(token)
	}

	pub(crate) fn parse_tuple_call(&mut self, operator: Token<'bump>) -> crate::Result<AstTuple<'bump>> {
		let mut nodes = Vec::with_capacity(4);
		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(CloseParen) {
				break;
			}
			nodes.push(self.parse_node(Precedence::None)?);
			if self.consume_if(TokenKind::Separator(Comma))?.is_none() {
				break;
			};
		}

		self.consume_operator(CloseParen)?;
		Ok(AstTuple {
			token: operator,
			nodes,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{
				Ast::{Identifier, Infix, Literal},
				AstInfix,
				AstLiteral::Number,
				InfixOperator,
			},
			parse::parse,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_empty_tuple() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "()").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let node = result[0].first_unchecked().as_tuple();
		assert!(node.nodes.is_empty());
	}

	#[test]
	fn test_tuple_with_number() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "(9924)").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let node = result[0].first_unchecked().as_tuple();
		let Some(node) = node.nodes.first() else {
			panic!()
		};
		let Literal(Number(number)) = &node else {
			panic!()
		};
		assert_eq!(number.value(), "9924");
	}

	#[test]
	fn test_nested_tuple() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "(1 * ( 2 + 3 ))").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let node = result[0].first_unchecked().as_tuple();
		let Some(node) = node.nodes.first() else {
			panic!()
		};
		let Infix(AstInfix {
			left,
			right,
			..
		}) = &node
		else {
			panic!()
		};

		let Literal(Number(left)) = &left.as_ref() else {
			panic!()
		};
		assert_eq!(left.value(), "1");

		let node = right.as_tuple();
		let Some(node) = node.nodes.first() else {
			panic!()
		};
		let AstInfix {
			left,
			right,
			..
		} = &node.as_infix();

		let Literal(Number(left)) = &left.as_ref() else {
			panic!()
		};
		assert_eq!(left.value(), "2");

		let Literal(Number(right)) = &right.as_ref() else {
			panic!()
		};
		assert_eq!(right.value(), "3");
	}

	#[test]
	fn test_tuple_with_identifier() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "(u)").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let node = &result[0].first_unchecked().as_tuple();
		let Some(node) = node.nodes.first() else {
			panic!()
		};
		let Identifier(node) = node else {
			panic!()
		};
		assert_eq!(node.text(), "u");
	}

	#[test]
	fn test_tuple_with_identifier_and_type() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "(u: Bool)").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let node = result[0].first_unchecked().as_tuple();
		let Some(node) = node.nodes.first() else {
			panic!()
		};
		let Infix(AstInfix {
			left,
			right,
			..
		}) = &node
		else {
			panic!()
		};

		let identifier = &left.as_identifier();
		assert_eq!(identifier.text(), "u");

		let dat_type = &right.as_identifier();
		assert_eq!(dat_type.text(), "Bool");
	}

	#[test]
	fn test_tuple_with_multiple_identifiers() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "(u,v)").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let node = result[0].first_unchecked().as_tuple();

		let Some(Identifier(u_node)) = &node.nodes.first() else {
			panic!()
		};
		assert_eq!(u_node.text(), "u");

		let Some(Identifier(v_node)) = &node.nodes.last() else {
			panic!()
		};
		assert_eq!(v_node.text(), "v");
	}

	#[test]
	fn test_tuple_with_identifiers_and_types() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "(u: Bool, v: Text)").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let node = result[0].first_unchecked().as_tuple();

		let Some(u_node) = node.nodes.first() else {
			panic!()
		};
		let Infix(AstInfix {
			left,
			right,
			..
		}) = &u_node
		else {
			panic!()
		};
		let Identifier(identifier) = &left.as_ref() else {
			panic!()
		};
		assert_eq!(identifier.text(), "u");
		let Identifier(ty) = &right.as_ref() else {
			panic!()
		};
		assert_eq!(ty.text(), "Bool");

		let Some(v_node) = node.nodes.last() else {
			panic!()
		};
		let Infix(AstInfix {
			left,
			right,
			..
		}) = &v_node
		else {
			panic!()
		};
		let Identifier(identifier) = &left.as_ref() else {
			panic!()
		};
		assert_eq!(identifier.text(), "v");
		let Identifier(identifier) = &right.as_ref() else {
			panic!()
		};
		assert_eq!(identifier.text(), "Text");
	}

	#[test]
	fn test_tuple_with_identifiers_and_declaration() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "(u = 1, v = 2)").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let node = result[0].first_unchecked().as_tuple();

		let Some(u_node) = node.nodes.first() else {
			panic!()
		};
		let Infix(AstInfix {
			left,
			operator,
			right,
			..
		}) = &u_node
		else {
			panic!()
		};
		let Identifier(identifier) = &left.as_ref() else {
			panic!()
		};
		assert_eq!(identifier.text(), "u");
		assert!(matches!(operator, InfixOperator::Assign(_)));
		let Literal(Number(number)) = right.as_ref() else {
			panic!()
		};
		assert_eq!(number.value(), "1");

		let Some(v_node) = node.nodes.last() else {
			panic!()
		};
		let Infix(AstInfix {
			left,
			operator,
			right,
			..
		}) = &v_node
		else {
			panic!()
		};
		let Identifier(identifier) = &left.as_ref() else {
			panic!()
		};
		assert_eq!(identifier.text(), "v");
		assert!(matches!(operator, InfixOperator::Assign(_)));
		let Literal(Number(number)) = right.as_ref() else {
			panic!()
		};
		assert_eq!(number.value(), "2");
	}

	#[test]
	fn test_multiline_tuple() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"(
        u: Bool,
        v: Text
        )"#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let node = result[0].first_unchecked().as_tuple();

		let Some(u_node) = node.nodes.first() else {
			panic!()
		};
		let Infix(AstInfix {
			left,
			right,
			..
		}) = &u_node
		else {
			panic!()
		};
		let Identifier(identifier) = &left.as_ref() else {
			panic!()
		};
		assert_eq!(identifier.text(), "u");
		let Identifier(identifier) = &right.as_ref() else {
			panic!()
		};
		assert_eq!(identifier.text(), "Bool");

		let Some(v_node) = node.nodes.last() else {
			panic!()
		};
		let Infix(AstInfix {
			left,
			right,
			..
		}) = &v_node
		else {
			panic!()
		};
		let Identifier(identifier) = &left.as_ref() else {
			panic!()
		};
		assert_eq!(identifier.text(), "v");
		let Identifier(identifier) = &right.as_ref() else {
			panic!()
		};
		assert_eq!(identifier.text(), "Text");
	}

	#[test]
	fn test_regression() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "(-1 -2)").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let node = result[0].first_unchecked().as_tuple();
		assert_eq!(node.nodes.len(), 1);

		let infix = node.nodes[0].as_infix();

		let left_number = infix.left.as_literal_number();
		assert_eq!(left_number.value(), "-1");

		assert!(matches!(infix.operator, InfixOperator::Subtract(_)));

		let right_number = infix.right.as_literal_number();
		assert_eq!(right_number.value(), "2");
	}
}
