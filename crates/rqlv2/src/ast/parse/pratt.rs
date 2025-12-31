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
	Assignment = 1, // :=, as, :
	LogicOr = 2,    // OR, XOR, ||
	LogicAnd = 3,   // AND, &&
	Comparison = 4, // =, !=, <, <=, >, >=, IN, BETWEEN
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
				Operator::ColonEqual => Precedence::Assignment,
				Operator::As => Precedence::Assignment,
				Operator::Colon => Precedence::Assignment,

				Operator::Or | Operator::DoublePipe | Operator::Xor => Precedence::LogicOr,
				Operator::And | Operator::DoubleAmpersand => Precedence::LogicAnd,

				Operator::Equal
				| Operator::DoubleEqual
				| Operator::BangEqual
				| Operator::LeftAngle
				| Operator::LeftAngleEqual
				| Operator::RightAngle
				| Operator::RightAngleEqual => Precedence::Comparison,

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
			TokenKind::Operator(Operator::Bang) | TokenKind::Operator(Operator::Not) => {
				self.parse_unary(UnaryOp::Not)
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
			Operator::Equal | Operator::DoubleEqual => BinaryOp::Eq,
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
			Operator::ColonEqual => BinaryOp::Assign,
			Operator::Colon => BinaryOp::KeyValue,
			_ => return Err(self.error(ParseErrorKind::UnexpectedToken)),
		})
	}
}
