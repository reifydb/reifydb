// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Operator expression types.

use super::Expr;
use crate::token::Span;

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
	// Arithmetic
	Add,
	Sub,
	Mul,
	Div,
	Rem,

	// Comparison
	Eq,
	Ne,
	Lt,
	Le,
	Gt,
	Ge,

	// Logical
	And,
	Or,
	Xor,

	// Access
	Dot,         // field access: a.b
	DoubleColon, // namespace access: ns::table
	Arrow,       // arrow: a -> b

	// Assignment/binding
	As,             // alias: expr AS name
	Assign,         // assignment: $var = expr
	TypeAscription, // type: name: Type
}

impl BinaryOp {
	/// Get the operator symbol as a string.
	pub fn as_str(&self) -> &'static str {
		match self {
			BinaryOp::Add => "+",
			BinaryOp::Sub => "-",
			BinaryOp::Mul => "*",
			BinaryOp::Div => "/",
			BinaryOp::Rem => "%",
			BinaryOp::Eq => "=",
			BinaryOp::Ne => "!=",
			BinaryOp::Lt => "<",
			BinaryOp::Le => "<=",
			BinaryOp::Gt => ">",
			BinaryOp::Ge => ">=",
			BinaryOp::And => "AND",
			BinaryOp::Or => "OR",
			BinaryOp::Xor => "XOR",
			BinaryOp::Dot => ".",
			BinaryOp::DoubleColon => "::",
			BinaryOp::Arrow => "->",
			BinaryOp::As => "AS",
			BinaryOp::Assign => ":=",
			BinaryOp::TypeAscription => ":",
		}
	}
}

/// Binary expression: left op right
#[derive(Debug, Clone, Copy)]
pub struct BinaryExpr<'bump> {
	pub op: BinaryOp,
	pub left: &'bump Expr<'bump>,
	pub right: &'bump Expr<'bump>,
	pub span: Span,
}

impl<'bump> BinaryExpr<'bump> {
	/// Create a new binary expression.
	pub fn new(op: BinaryOp, left: &'bump Expr<'bump>, right: &'bump Expr<'bump>, span: Span) -> Self {
		Self {
			op,
			left,
			right,
			span,
		}
	}
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
	/// Negation: -x
	Neg,
	/// Logical NOT: NOT x, !x
	Not,
	/// Unary plus: +x (identity)
	Plus,
}

impl UnaryOp {
	/// Get the operator symbol as a string.
	pub fn as_str(&self) -> &'static str {
		match self {
			UnaryOp::Neg => "-",
			UnaryOp::Not => "NOT",
			UnaryOp::Plus => "+",
		}
	}
}

/// Unary expression: op operand
#[derive(Debug, Clone, Copy)]
pub struct UnaryExpr<'bump> {
	pub op: UnaryOp,
	pub operand: &'bump Expr<'bump>,
	pub span: Span,
}

impl<'bump> UnaryExpr<'bump> {
	/// Create a new unary expression.
	pub fn new(op: UnaryOp, operand: &'bump Expr<'bump>, span: Span) -> Self {
		Self {
			op,
			operand,
			span,
		}
	}
}
