// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{Ast, AstLiteral, AstLiteralNumber, AstPrefix, AstPrefixOperator},
		parse::{Parser, Precedence},
	},
	bump::{BumpBox, BumpFragment},
	diagnostic::AstError,
	token::{
		operator::Operator,
		token::{Literal::Number, Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_prefix(&mut self) -> crate::Result<Ast<'bump>> {
		let operator = self.parse_prefix_operator()?;

		// Determine precedence based on operator type
		let precedence = match &operator {
			AstPrefixOperator::Not(_) => Precedence::Assignment,
			_ => Precedence::Prefix,
		};

		let expr = self.parse_node(precedence)?;

		if matches!(operator, AstPrefixOperator::Negate(_)) {
			if let Ast::Literal(AstLiteral::Number(literal)) = &expr {
				let text = self.bump().alloc_str(&format!("-{}", literal.0.fragment.text()));
				return Ok(Ast::Literal(AstLiteral::Number(AstLiteralNumber(Token {
					kind: TokenKind::Literal(Number),
					fragment: BumpFragment::Statement {
						column: operator.token().fragment.column(),
						line: operator.token().fragment.line(),
						text,
					},
				}))));
			}
		}

		Ok(Ast::Prefix(AstPrefix {
			operator,
			node: BumpBox::new_in(expr, self.bump()),
		}))
	}

	fn parse_prefix_operator(&mut self) -> crate::Result<AstPrefixOperator<'bump>> {
		let token = self.advance()?;
		match token.kind {
			TokenKind::Operator(operator) => match operator {
				Operator::Plus => Ok(AstPrefixOperator::Plus(token)),
				Operator::Minus => Ok(AstPrefixOperator::Negate(token)),
				Operator::Bang => Ok(AstPrefixOperator::Not(token)),
				Operator::Not => Ok(AstPrefixOperator::Not(token)),
				_ => {
					return Err(AstError::UnsupportedToken {
						fragment: token.fragment.to_owned(),
					}
					.into());
				}
			},
			_ => {
				return Err(AstError::UnsupportedToken {
					fragment: token.fragment.to_owned(),
				}
				.into());
			}
		}
	}
}

#[cfg(test)]
pub mod tests {
	use std::ops::Deref;

	use crate::{
		ast::{
			ast::{
				Ast, Ast::Literal, AstLiteral, AstLiteralNumber, AstPrefix, AstPrefixOperator,
				InfixOperator,
			},
			parse::parse,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_negative_number() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "-2").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Number(AstLiteralNumber(token))) = &result[0].first_unchecked() else {
			panic!()
		};
		assert_eq!(token.fragment.text(), "-2");
	}

	#[test]
	fn test_group_plus() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "+(2)").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::Prefix(AstPrefix {
			operator,
			node,
		}) = result[0].first_unchecked()
		else {
			panic!()
		};
		assert!(matches!(*operator, AstPrefixOperator::Plus(_)));

		let Ast::Tuple(tuple) = node.deref() else {
			panic!()
		};
		let Literal(AstLiteral::Number(node)) = &tuple.nodes.first().unwrap() else {
			panic!()
		};
		assert_eq!(node.value(), "2");
	}

	#[test]
	fn test_group_negate() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "-(2)").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::Prefix(AstPrefix {
			operator,
			node,
		}) = result[0].first_unchecked()
		else {
			panic!()
		};
		assert!(matches!(*operator, AstPrefixOperator::Negate(_)));

		let Ast::Tuple(tuple) = node.deref() else {
			panic!()
		};
		let Literal(AstLiteral::Number(node)) = &tuple.nodes.first().unwrap() else {
			panic!()
		};
		assert_eq!(node.value(), "2");
	}

	#[test]
	fn test_group_negate_negative_number() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "-(-2)").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::Prefix(AstPrefix {
			operator,
			node,
		}) = result[0].first_unchecked()
		else {
			panic!()
		};
		assert!(matches!(*operator, AstPrefixOperator::Negate(_)));

		let Ast::Tuple(tuple) = node.deref() else {
			panic!()
		};
		let Literal(AstLiteral::Number(AstLiteralNumber(token))) = &tuple.nodes.first().unwrap() else {
			panic!()
		};
		assert_eq!(token.fragment.text(), "-2");
	}

	#[test]
	fn test_not_false() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "!false").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::Prefix(AstPrefix {
			operator,
			node,
		}) = result[0].first_unchecked()
		else {
			panic!()
		};
		assert!(matches!(*operator, AstPrefixOperator::Not(_)));

		let Literal(AstLiteral::Boolean(node)) = node.deref() else {
			panic!()
		};
		assert!(!node.value());
	}

	#[test]
	fn test_not_word_false() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "not false").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Ast::Prefix(AstPrefix {
			operator,
			node,
		}) = result[0].first_unchecked()
		else {
			panic!()
		};
		assert!(matches!(*operator, AstPrefixOperator::Not(_)));

		let Literal(AstLiteral::Boolean(node)) = node.deref() else {
			panic!()
		};
		assert!(!node.value());
	}

	#[test]
	fn test_not_comparison_precedence() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "not x == 5").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		// Should parse as: not (x == 5), not (not x) == 5
		let Ast::Prefix(AstPrefix {
			operator,
			node,
		}) = result[0].first_unchecked()
		else {
			panic!("Expected prefix expression, got {:?}", result[0].first_unchecked())
		};
		assert!(matches!(*operator, AstPrefixOperator::Not(_)));

		// The inner expression should be a comparison (x == 5)
		let Ast::Infix(inner) = node.deref() else {
			panic!("Expected infix comparison inside NOT, got {:?}", node.deref())
		};

		// Verify it's an equality comparison
		assert!(matches!(inner.operator, InfixOperator::Equal(_)));

		// Left side should be identifier 'x'
		let Ast::Identifier(left_id) = inner.left.deref() else {
			panic!("Expected identifier on left side")
		};
		assert_eq!(left_id.text(), "x");

		// Right side should be number '5'
		let Literal(AstLiteral::Number(right_num)) = inner.right.deref() else {
			panic!("Expected number on right side")
		};
		assert_eq!(right_num.value(), "5");
	}
}
