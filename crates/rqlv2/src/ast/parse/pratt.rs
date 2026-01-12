// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Pratt parser implementation with precedence climbing.

use super::{
	Parser,
	error::{ParseError, ParseErrorKind},
};
use crate::{
	ast::{Expr, expr::*},
	token::{Keyword, Operator, Punctuation, Token, TokenKind},
};

/// Operator precedence levels (higher = binds tighter).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Precedence {
	None = 0,
	Assignment = 1, // =, as, :
	LogicOr = 2,    // OR, XOR, ||
	LogicAnd = 3,   // AND, &&
	Comparison = 4, // ==, !=, <, <=, >, >=, IN, BETWEEN
	Term = 5,       // +, -
	Factor = 6,     // *, /, %
	Prefix = 7,     // -, NOT, !
	Call = 8,       // ()
	Primary = 9,    // ., ::, ->
}

impl Precedence {
	/// Get precedence for a token in infix position.
	pub fn for_token(token: &Token) -> Self {
		match token.kind {
			TokenKind::Operator(op) => match op {
				Operator::Equal => Precedence::Assignment,
				Operator::As => Precedence::Assignment,
				Operator::Colon => Precedence::Assignment,

				Operator::Or | Operator::DoublePipe | Operator::Xor => Precedence::LogicOr,
				Operator::And | Operator::DoubleAmpersand => Precedence::LogicAnd,

				Operator::DoubleEqual
				| Operator::BangEqual
				| Operator::LeftAngle
				| Operator::LeftAngleEqual
				| Operator::RightAngle
				| Operator::RightAngleEqual => Precedence::Comparison,

				// NOT needs Comparison precedence for NOT IN / NOT BETWEEN
				Operator::Not => Precedence::Comparison,

				Operator::Plus | Operator::Minus => Precedence::Term,
				Operator::Asterisk | Operator::Slash | Operator::Percent => Precedence::Factor,

				Operator::Dot | Operator::DoubleColon | Operator::Arrow => Precedence::Primary,

				_ => Precedence::None,
			},
			TokenKind::Punctuation(Punctuation::OpenParen) => Precedence::Call,
			TokenKind::Keyword(Keyword::In) => Precedence::Comparison,
			TokenKind::Keyword(Keyword::Between) => Precedence::Comparison,
			_ => Precedence::None,
		}
	}
}

impl<'bump, 'src> Parser<'bump, 'src> {
	/// Parse an expression with the given minimum precedence.
	pub fn parse_expr(&mut self, min_precedence: Precedence) -> Result<&'bump Expr<'bump>, ParseError> {
		// Parse prefix (primary) expression
		let mut left = self.parse_prefix()?;

		// Parse infix expressions while precedence allows
		while !self.is_eof() {
			let current = self.current();
			let precedence = Precedence::for_token(current);

			if precedence <= min_precedence {
				break;
			}

			left = self.parse_infix(left, precedence)?;
		}

		Ok(left)
	}

	/// Parse a prefix (primary) expression.
	fn parse_prefix(&mut self) -> Result<&'bump Expr<'bump>, ParseError> {
		let token = self.current();

		match token.kind {
			// Unary operators
			TokenKind::Operator(Operator::Minus) => self.parse_unary(UnaryOp::Neg),
			TokenKind::Operator(Operator::Plus) => self.parse_unary(UnaryOp::Plus),
			// NOT operator - check for NOT EXISTS special case
			TokenKind::Operator(Operator::Bang) | TokenKind::Operator(Operator::Not) => {
				// Check for NOT EXISTS pattern
				if self.peek().kind == TokenKind::Keyword(Keyword::Exists) {
					self.advance(); // consume NOT
					self.parse_exists(true)
				} else {
					self.parse_unary(UnaryOp::Not)
				}
			}

			// Grouping: (expr) or tuple
			TokenKind::Punctuation(Punctuation::OpenParen) => self.parse_paren_or_tuple(),

			// Collections
			TokenKind::Punctuation(Punctuation::OpenBracket) => self.parse_list(),
			TokenKind::Punctuation(Punctuation::OpenCurly) => self.parse_inline_or_subquery(),

			// Literals
			TokenKind::Literal(lit) => self.parse_literal(lit),

			// Identifiers
			TokenKind::Identifier => self.parse_identifier(),
			TokenKind::QuotedIdentifier => self.parse_quoted_identifier(),
			TokenKind::Variable => self.parse_variable(),

			// Keywords that start expressions
			TokenKind::Keyword(kw) => self.parse_keyword_expr(kw),

			// Wildcard
			TokenKind::Operator(Operator::Asterisk) => self.parse_wildcard(),

			_ => Err(self.error(ParseErrorKind::ExpectedExpression)),
		}
	}

	/// Parse an infix expression.
	fn parse_infix(
		&mut self,
		left: &'bump Expr<'bump>,
		precedence: Precedence,
	) -> Result<&'bump Expr<'bump>, ParseError> {
		let token = self.current();

		match token.kind {
			// Function call
			TokenKind::Punctuation(Punctuation::OpenParen) => self.parse_call(left),

			// BETWEEN
			TokenKind::Keyword(Keyword::Between) => self.parse_between(left),

			// IN
			TokenKind::Keyword(Keyword::In) => self.parse_in(left, false),

			// NOT IN - must come before general operator match
			TokenKind::Operator(Operator::Not) if self.peek().kind == TokenKind::Keyword(Keyword::In) => {
				self.parse_in(left, true)
			}

			// Binary operators
			TokenKind::Operator(op) => {
				let binary_op = self.token_to_binary_op(op)?;
				self.advance();
				let right = self.parse_expr(precedence)?;
				let span = left.span().merge(&right.span());
				Ok(self.alloc(Expr::Binary(BinaryExpr::new(binary_op, left, right, span))))
			}

			_ => Err(self.error(ParseErrorKind::UnexpectedToken)),
		}
	}

	/// Convert operator token to BinaryOp.
	fn token_to_binary_op(&self, op: Operator) -> Result<BinaryOp, ParseError> {
		Ok(match op {
			Operator::Plus => BinaryOp::Add,
			Operator::Minus => BinaryOp::Sub,
			Operator::Asterisk => BinaryOp::Mul,
			Operator::Slash => BinaryOp::Div,
			Operator::Percent => BinaryOp::Rem,
			Operator::Equal => BinaryOp::Assign,
			Operator::DoubleEqual => BinaryOp::Eq,
			Operator::BangEqual => BinaryOp::Ne,
			Operator::LeftAngle => BinaryOp::Lt,
			Operator::LeftAngleEqual => BinaryOp::Le,
			Operator::RightAngle => BinaryOp::Gt,
			Operator::RightAngleEqual => BinaryOp::Ge,
			Operator::And | Operator::DoubleAmpersand => BinaryOp::And,
			Operator::Or | Operator::DoublePipe => BinaryOp::Or,
			Operator::Xor => BinaryOp::Xor,
			Operator::Dot => BinaryOp::Dot,
			Operator::DoubleColon => BinaryOp::DoubleColon,
			Operator::Arrow => BinaryOp::Arrow,
			Operator::As => BinaryOp::As,
			Operator::Colon => BinaryOp::KeyValue,
			_ => return Err(self.error(ParseErrorKind::UnexpectedToken)),
		})
	}
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;

	use crate::{ast::Expr, ast::expr::{BinaryOp, UnaryOp, Literal}, token::tokenize};

	fn get_first_expr<'a>(stmt: crate::ast::Statement<'a>) -> &'a Expr<'a> {
		match stmt {
			crate::ast::Statement::Pipeline(p) => {
				assert!(!p.stages.is_empty());
				&p.stages[0]
			}
			crate::ast::Statement::Expression(e) => e.expr,
			_ => panic!("Expected Pipeline or Expression statement"),
		}
	}

	#[test]
	fn test_as_one() {
		let bump = Bump::new();
		let source = "1 as one";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::As);
				match binary.left {
					Expr::Literal(Literal::Integer { value, .. }) => assert_eq!(*value, "1"),
					_ => panic!("Expected integer literal on left"),
				}
				match binary.right {
					Expr::Identifier(id) => assert_eq!(id.name, "one"),
					_ => panic!("Expected identifier on right"),
				}
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_add() {
		let bump = Bump::new();
		let source = "1 + 2";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Add);
				match binary.left {
					Expr::Literal(Literal::Integer { value, .. }) => assert_eq!(*value, "1"),
					_ => panic!("Expected integer literal"),
				}
				match binary.right {
					Expr::Literal(Literal::Integer { value, .. }) => assert_eq!(*value, "2"),
					_ => panic!("Expected integer literal"),
				}
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_subtract() {
		let bump = Bump::new();
		let source = "1 - 2";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Sub);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_multiply() {
		let bump = Bump::new();
		let source = "1 * 2";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Mul);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_divide() {
		let bump = Bump::new();
		let source = "1 / 2";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Div);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_remainder() {
		let bump = Bump::new();
		let source = "1 % 2";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Rem);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_greater_than() {
		let bump = Bump::new();
		let source = "1 > 2";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Gt);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_greater_than_or_equal() {
		let bump = Bump::new();
		let source = "1 >= 2";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Ge);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_less_than() {
		let bump = Bump::new();
		let source = "1 < 2";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Lt);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_less_than_or_equal() {
		let bump = Bump::new();
		let source = "1 <= 2";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Le);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_equal() {
		let bump = Bump::new();
		let source = "1 == 2";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Eq);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_not_equal() {
		let bump = Bump::new();
		let source = "1 != 2";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Ne);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_and_operator() {
		let bump = Bump::new();
		let source = "true and false";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::And);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_or_operator() {
		let bump = Bump::new();
		let source = "true or false";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Or);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_xor_operator() {
		let bump = Bump::new();
		let source = "true xor false";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Xor);
			}
			_ => panic!("Expected binary expression"),
		}
	}

	// --- Prefix expression tests ---

	#[test]
	fn test_negative_number() {
		let bump = Bump::new();
		let source = "-2";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Unary(unary) => {
				assert_eq!(unary.op, UnaryOp::Neg);
				match unary.operand {
					Expr::Literal(Literal::Integer { value, .. }) => assert_eq!(*value, "2"),
					_ => panic!("Expected integer literal"),
				}
			}
			_ => panic!("Expected unary expression"),
		}
	}

	#[test]
	fn test_positive_number() {
		let bump = Bump::new();
		let source = "+2";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Unary(unary) => {
				assert_eq!(unary.op, UnaryOp::Plus);
			}
			_ => panic!("Expected unary expression"),
		}
	}

	#[test]
	fn test_not_operator() {
		let bump = Bump::new();
		let source = "not false";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Unary(unary) => {
				assert_eq!(unary.op, UnaryOp::Not);
				match unary.operand {
					Expr::Literal(Literal::Bool { value, .. }) => assert!(!value),
					_ => panic!("Expected boolean literal"),
				}
			}
			_ => panic!("Expected unary expression"),
		}
	}

	#[test]
	fn test_bang_not_operator() {
		let bump = Bump::new();
		let source = "!true";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Unary(unary) => {
				assert_eq!(unary.op, UnaryOp::Not);
			}
			_ => panic!("Expected unary expression"),
		}
	}

	// --- Literal tests ---

	#[test]
	fn test_literal_integer() {
		let bump = Bump::new();
		let source = "42";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Literal(Literal::Integer { value, .. }) => assert_eq!(*value, "42"),
			_ => panic!("Expected integer literal"),
		}
	}

	#[test]
	fn test_literal_float() {
		let bump = Bump::new();
		let source = "3.14";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Literal(Literal::Float { value, .. }) => assert_eq!(*value, "3.14"),
			_ => panic!("Expected float literal"),
		}
	}

	#[test]
	fn test_literal_string_single_quotes() {
		let bump = Bump::new();
		let source = "'hello'";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Literal(Literal::String { .. }) => {}
			_ => panic!("Expected string literal, got {:?}", expr),
		}
	}

	#[test]
	fn test_literal_string_double_quotes() {
		let bump = Bump::new();
		let source = "\"world\"";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Literal(Literal::String { .. }) => {}
			_ => panic!("Expected string literal, got {:?}", expr),
		}
	}

	#[test]
	fn test_literal_true() {
		let bump = Bump::new();
		let source = "true";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Literal(Literal::Bool { value, .. }) => assert!(*value),
			_ => panic!("Expected boolean literal"),
		}
	}

	#[test]
	fn test_literal_false() {
		let bump = Bump::new();
		let source = "false";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Literal(Literal::Bool { value, .. }) => assert!(!*value),
			_ => panic!("Expected boolean literal"),
		}
	}

	#[test]
	fn test_literal_undefined() {
		let bump = Bump::new();
		let source = "undefined";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		assert!(matches!(expr, Expr::Literal(Literal::Undefined { .. })));
	}

	// --- Tuple and List tests ---

	#[test]
	fn test_tuple_multiple() {
		let bump = Bump::new();
		let source = "(1, 2, 3)";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Tuple(tuple) => {
				assert_eq!(tuple.elements.len(), 3);
			}
			_ => panic!("Expected tuple"),
		}
	}

	#[test]
	fn test_tuple_empty() {
		let bump = Bump::new();
		let source = "()";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Tuple(tuple) => {
				assert_eq!(tuple.elements.len(), 0);
			}
			_ => panic!("Expected tuple"),
		}
	}

	#[test]
	fn test_list_expression() {
		let bump = Bump::new();
		let source = "[1, 2, 3]";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::List(list) => {
				assert_eq!(list.elements.len(), 3);
			}
			_ => panic!("Expected list"),
		}
	}

	#[test]
	fn test_list_empty() {
		let bump = Bump::new();
		let source = "[]";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::List(list) => {
				assert_eq!(list.elements.len(), 0);
			}
			_ => panic!("Expected list"),
		}
	}

	// --- Function call tests ---

	#[test]
	fn test_function_call_no_args() {
		let bump = Bump::new();
		let source = "now()";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Call(call) => {
				match call.function {
					Expr::Identifier(id) => assert_eq!(id.name, "now"),
					_ => panic!("Expected identifier for function"),
				}
				assert_eq!(call.arguments.len(), 0);
			}
			_ => panic!("Expected call expression"),
		}
	}

	#[test]
	fn test_function_call_single_arg() {
		let bump = Bump::new();
		let source = "abs(-5)";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Call(call) => {
				match call.function {
					Expr::Identifier(id) => assert_eq!(id.name, "abs"),
					_ => panic!("Expected identifier for function"),
				}
				assert_eq!(call.arguments.len(), 1);
			}
			_ => panic!("Expected call expression"),
		}
	}

	#[test]
	fn test_function_call_multiple_args() {
		let bump = Bump::new();
		let source = "concat('hello', ' ', 'world')";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Call(call) => {
				match call.function {
					Expr::Identifier(id) => assert_eq!(id.name, "concat"),
					_ => panic!("Expected identifier for function"),
				}
				assert_eq!(call.arguments.len(), 3);
			}
			_ => panic!("Expected call expression"),
		}
	}

	// --- Inline object tests ---

	#[test]
	fn test_inline_object() {
		let bump = Bump::new();
		let source = "{ name: 'Alice', age: 30 }";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Inline(inline) => {
				assert_eq!(inline.fields.len(), 2);
				assert_eq!(inline.fields[0].key, "name");
				assert_eq!(inline.fields[1].key, "age");
			}
			_ => panic!("Expected inline expression"),
		}
	}

	#[test]
	fn test_inline_object_empty() {
		let bump = Bump::new();
		let source = "{}";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Inline(inline) => {
				assert_eq!(inline.fields.len(), 0);
			}
			_ => panic!("Expected inline expression"),
		}
	}

	// --- Precedence tests ---

	#[test]
	fn test_precedence_mul_over_add() {
		let bump = Bump::new();
		let source = "1 + 2 * 3";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		// Should parse as 1 + (2 * 3)
		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Add);
				match binary.right {
					Expr::Binary(inner) => {
						assert_eq!(inner.op, BinaryOp::Mul);
					}
					_ => panic!("Expected multiplication on right"),
				}
			}
			_ => panic!("Expected binary expression"),
		}
	}

	#[test]
	fn test_precedence_and_over_or() {
		let bump = Bump::new();
		let source = "a or b and c";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		// Should parse as a or (b and c)
		match expr {
			Expr::Binary(binary) => {
				assert_eq!(binary.op, BinaryOp::Or);
				match binary.right {
					Expr::Binary(inner) => {
						assert_eq!(inner.op, BinaryOp::And);
					}
					_ => panic!("Expected AND on right"),
				}
			}
			_ => panic!("Expected binary expression"),
		}
	}

	// --- BETWEEN tests ---

	#[test]
	fn test_between_expression() {
		let bump = Bump::new();
		let source = "x between 1 and 10";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::Between(between) => {
				match between.value {
					Expr::Identifier(id) => assert_eq!(id.name, "x"),
					_ => panic!("Expected identifier"),
				}
			}
			_ => panic!("Expected between expression"),
		}
	}

	// --- IN tests ---

	#[test]
	fn test_in_expression() {
		let bump = Bump::new();
		let source = "x in [1, 2, 3]";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::In(in_expr) => {
				assert!(!in_expr.negated);
			}
			_ => panic!("Expected in expression"),
		}
	}

	#[test]
	fn test_not_in_expression() {
		let bump = Bump::new();
		let source = "x not in [1, 2, 3]";
		let result = tokenize(source, &bump).unwrap();
		let program = crate::ast::parse::parse(&bump, &result.tokens, source).unwrap();
		let stmt = program.statements.first().copied().unwrap();
		let expr = get_first_expr(stmt);

		match expr {
			Expr::In(in_expr) => {
				assert!(in_expr.negated);
			}
			_ => panic!("Expected in expression"),
		}
	}
}
