// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Type;

/// Compiled expression with column indices resolved.
#[derive(Debug, Clone)]
pub enum Expr {
	/// Reference to a column by index
	ColumnRef(ColumnRef),

	/// Literal constant value
	Literal(Literal),

	/// Binary operation
	BinaryOp {
		op: BinaryOp,
		left: Box<Expr>,
		right: Box<Expr>,
	},

	/// Unary operation
	UnaryOp {
		op: UnaryOp,
		operand: Box<Expr>,
	},

	/// Reference to a scope variable
	VarRef(String),

	/// Field access on an expression (e.g., $user.id)
	FieldAccess {
		object: Box<Expr>,
		field: String,
	},

	/// Function call
	Call {
		function_name: String,
		arguments: Vec<Expr>,
	},
}

#[derive(Debug, Clone)]
pub struct ColumnRef {
	/// Index into Columns
	pub index: usize,
	/// Original name (for error messages)
	pub name: String,
}

/// Literal values for core types (Milestone 1)
#[derive(Debug, Clone)]
pub enum Literal {
	Null,
	Bool(bool),
	Int8(i64),
	Float8(f64),
	Utf8(String),
}

impl Literal {
	pub fn get_type(&self) -> Type {
		match self {
			Literal::Null => Type::Undefined,
			Literal::Bool(_) => Type::Boolean,
			Literal::Int8(_) => Type::Int8,
			Literal::Float8(_) => Type::Float8,
			Literal::Utf8(_) => Type::Utf8,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
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

	// Arithmetic (for computed columns)
	Add,
	Sub,
	Mul,
	Div,
}

impl std::fmt::Display for BinaryOp {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			BinaryOp::Eq => write!(f, "=="),
			BinaryOp::Ne => write!(f, "!="),
			BinaryOp::Lt => write!(f, "<"),
			BinaryOp::Le => write!(f, "<="),
			BinaryOp::Gt => write!(f, ">"),
			BinaryOp::Ge => write!(f, ">="),
			BinaryOp::And => write!(f, "and"),
			BinaryOp::Or => write!(f, "or"),
			BinaryOp::Add => write!(f, "+"),
			BinaryOp::Sub => write!(f, "-"),
			BinaryOp::Mul => write!(f, "*"),
			BinaryOp::Div => write!(f, "/"),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
	Not,
	Neg,
	IsNull,
	IsNotNull,
}

impl std::fmt::Display for UnaryOp {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			UnaryOp::Not => write!(f, "not"),
			UnaryOp::Neg => write!(f, "-"),
			UnaryOp::IsNull => write!(f, "is null"),
			UnaryOp::IsNotNull => write!(f, "is not null"),
		}
	}
}
