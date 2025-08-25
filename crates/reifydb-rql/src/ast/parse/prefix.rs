// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{OwnedFragment, diagnostic::ast, return_error};

use crate::ast::{
	Ast, AstLiteral, AstLiteralNumber, AstPrefix, AstPrefixOperator, Token,
	TokenKind,
	parse::{Parser, Precedence},
	tokenize::{Literal::Number, Operator},
};

impl Parser {
	pub(crate) fn parse_prefix(&mut self) -> crate::Result<Ast> {
		let operator = self.parse_prefix_operator()?;

		// NOT operator should have lower precedence than comparison
		// operator to allow expressions like "not price == 150" to
		// parse as "not (price == 150)"
		let precedence = match operator {
			AstPrefixOperator::Not(_) => Precedence::Assignment, /* Much lower than comparisons */
			_ => Precedence::Prefix,                             /* Keep existing high
			                                                       * precedence for +/-
			                                                       * operator */
		};

		let expr = self.parse_node(precedence)?;

		if matches!(operator, AstPrefixOperator::Negate(_)) {
			if let Ast::Literal(AstLiteral::Number(literal)) = &expr
			{
				return Ok(Ast::Literal(AstLiteral::Number(
					AstLiteralNumber(Token {
						kind: TokenKind::Literal(
							Number,
						),
						fragment: {
							OwnedFragment::Statement {
								column: operator
									.token()
									.fragment
									.column(),
								line: operator
									.token()
									.fragment
									.line(),
								text: format!(
									"-{}",
									literal.0
										.fragment
										.fragment()
								),
							}
						},
					}),
				)));
			}
		}

		Ok(Ast::Prefix(AstPrefix {
			operator,
			node: Box::new(expr),
		}))
	}

	fn parse_prefix_operator(
		&mut self,
	) -> crate::Result<AstPrefixOperator> {
		let token = self.advance()?;
		match &token.kind {
			TokenKind::Operator(operator) => match operator {
				Operator::Plus => {
					Ok(AstPrefixOperator::Plus(token))
				}
				Operator::Minus => {
					Ok(AstPrefixOperator::Negate(token))
				}
				Operator::Bang => {
					Ok(AstPrefixOperator::Not(token))
				}
				Operator::Not => {
					Ok(AstPrefixOperator::Not(token))
				}
				_ => return_error!(
					ast::unsupported_token_error(
						token.fragment
					)
				),
			},
			_ => return_error!(ast::unsupported_token_error(
				token.fragment
			)),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Deref;

	use crate::ast::{
		Ast, Ast::Literal, AstLiteral, AstLiteralNumber, AstPrefix,
		AstPrefixOperator, parse::parse, tokenize::tokenize,
	};

	#[test]
	fn test_negative_number() {
		let tokens = tokenize("-2").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let Literal(AstLiteral::Number(AstLiteralNumber(token))) =
			&result[0].first_unchecked()
		else {
			panic!()
		};
		assert_eq!(token.value(), "-2");
	}

	#[test]
	fn test_group_plus() {
		let tokens = tokenize("+(2)").unwrap();
		let result = parse(tokens).unwrap();
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
		let Literal(AstLiteral::Number(node)) =
			&tuple.nodes.first().unwrap()
		else {
			panic!()
		};
		assert_eq!(node.value(), "2");
	}

	#[test]
	fn test_group_negate() {
		let tokens = tokenize("-(2)").unwrap();
		let result = parse(tokens).unwrap();
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
		let Literal(AstLiteral::Number(node)) =
			&tuple.nodes.first().unwrap()
		else {
			panic!()
		};
		assert_eq!(node.value(), "2");
	}

	#[test]
	fn test_group_negate_negative_number() {
		let tokens = tokenize("-(-2)").unwrap();
		let result = parse(tokens).unwrap();
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
		let Literal(AstLiteral::Number(AstLiteralNumber(token))) =
			&tuple.nodes.first().unwrap()
		else {
			panic!()
		};
		assert_eq!(token.value(), "-2");
	}

	#[test]
	fn test_not_false() {
		let tokens = tokenize("!false").unwrap();
		let result = parse(tokens).unwrap();
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
		let tokens = tokenize("not false").unwrap();
		let result = parse(tokens).unwrap();
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
		let tokens = tokenize("not x == 5").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		// Should parse as: not (x == 5), not (not x) == 5
		let Ast::Prefix(AstPrefix {
			operator,
			node,
		}) = result[0].first_unchecked()
		else {
			panic!(
				"Expected prefix expression, got {:?}",
				result[0].first_unchecked()
			)
		};
		assert!(matches!(*operator, AstPrefixOperator::Not(_)));

		// The inner expression should be a comparison (x == 5)
		let Ast::Infix(inner) = node.deref() else {
			panic!(
				"Expected infix comparison inside NOT, got {:?}",
				node.deref()
			)
		};

		// Verify it's an equality comparison
		assert!(matches!(
			inner.operator,
			crate::ast::InfixOperator::Equal(_)
		));

		// Left side should be identifier 'x'
		let Ast::Identifier(left_id) = inner.left.deref() else {
			panic!("Expected identifier on left side")
		};
		assert_eq!(left_id.value(), "x");

		// Right side should be number '5'
		let Literal(AstLiteral::Number(right_num)) =
			inner.right.deref()
		else {
			panic!("Expected number on right side")
		};
		assert_eq!(right_num.value(), "5");
	}
}
