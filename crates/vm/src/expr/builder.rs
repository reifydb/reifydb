// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::types::{BinaryOp, ColumnRef, Expr, Literal, UnaryOp};
use crate::error::{Result, VmError};

/// Schema information for expression compilation
#[derive(Debug, Clone)]
pub struct ColumnSchema {
	pub name: String,
	pub index: usize,
}

/// Builder for constructing expressions ergonomically.
/// Expressions are compiled against a schema to resolve column names.
#[derive(Debug, Clone)]
pub enum ExprBuilder {
	Column(String),
	Literal(Literal),
	BinaryOp {
		op: BinaryOp,
		left: Box<ExprBuilder>,
		right: Box<ExprBuilder>,
	},
	UnaryOp {
		op: UnaryOp,
		operand: Box<ExprBuilder>,
	},
}

/// Create a column reference expression
pub fn col(name: &str) -> ExprBuilder {
	ExprBuilder::Column(name.to_string())
}

/// Create a literal expression
pub fn lit<T: Into<Literal>>(value: T) -> ExprBuilder {
	ExprBuilder::Literal(value.into())
}

// Literal conversions
impl From<bool> for Literal {
	fn from(v: bool) -> Self {
		Literal::Bool(v)
	}
}

impl From<i64> for Literal {
	fn from(v: i64) -> Self {
		Literal::Int8(v)
	}
}

impl From<i32> for Literal {
	fn from(v: i32) -> Self {
		Literal::Int8(v as i64)
	}
}

impl From<f64> for Literal {
	fn from(v: f64) -> Self {
		Literal::Float8(v)
	}
}

impl From<&str> for Literal {
	fn from(v: &str) -> Self {
		Literal::Utf8(v.to_string())
	}
}

impl From<String> for Literal {
	fn from(v: String) -> Self {
		Literal::Utf8(v)
	}
}

impl ExprBuilder {
	// Comparison operators
	pub fn eq(self, other: ExprBuilder) -> ExprBuilder {
		ExprBuilder::BinaryOp {
			op: BinaryOp::Eq,
			left: Box::new(self),
			right: Box::new(other),
		}
	}

	pub fn ne(self, other: ExprBuilder) -> ExprBuilder {
		ExprBuilder::BinaryOp {
			op: BinaryOp::Ne,
			left: Box::new(self),
			right: Box::new(other),
		}
	}

	pub fn gt(self, other: ExprBuilder) -> ExprBuilder {
		ExprBuilder::BinaryOp {
			op: BinaryOp::Gt,
			left: Box::new(self),
			right: Box::new(other),
		}
	}

	pub fn ge(self, other: ExprBuilder) -> ExprBuilder {
		ExprBuilder::BinaryOp {
			op: BinaryOp::Ge,
			left: Box::new(self),
			right: Box::new(other),
		}
	}

	pub fn lt(self, other: ExprBuilder) -> ExprBuilder {
		ExprBuilder::BinaryOp {
			op: BinaryOp::Lt,
			left: Box::new(self),
			right: Box::new(other),
		}
	}

	pub fn le(self, other: ExprBuilder) -> ExprBuilder {
		ExprBuilder::BinaryOp {
			op: BinaryOp::Le,
			left: Box::new(self),
			right: Box::new(other),
		}
	}

	// Logical operators
	pub fn and(self, other: ExprBuilder) -> ExprBuilder {
		ExprBuilder::BinaryOp {
			op: BinaryOp::And,
			left: Box::new(self),
			right: Box::new(other),
		}
	}

	pub fn or(self, other: ExprBuilder) -> ExprBuilder {
		ExprBuilder::BinaryOp {
			op: BinaryOp::Or,
			left: Box::new(self),
			right: Box::new(other),
		}
	}

	pub fn not(self) -> ExprBuilder {
		ExprBuilder::UnaryOp {
			op: UnaryOp::Not,
			operand: Box::new(self),
		}
	}

	// Arithmetic operators
	pub fn add(self, other: ExprBuilder) -> ExprBuilder {
		ExprBuilder::BinaryOp {
			op: BinaryOp::Add,
			left: Box::new(self),
			right: Box::new(other),
		}
	}

	pub fn sub(self, other: ExprBuilder) -> ExprBuilder {
		ExprBuilder::BinaryOp {
			op: BinaryOp::Sub,
			left: Box::new(self),
			right: Box::new(other),
		}
	}

	pub fn mul(self, other: ExprBuilder) -> ExprBuilder {
		ExprBuilder::BinaryOp {
			op: BinaryOp::Mul,
			left: Box::new(self),
			right: Box::new(other),
		}
	}

	pub fn div(self, other: ExprBuilder) -> ExprBuilder {
		ExprBuilder::BinaryOp {
			op: BinaryOp::Div,
			left: Box::new(self),
			right: Box::new(other),
		}
	}

	pub fn neg(self) -> ExprBuilder {
		ExprBuilder::UnaryOp {
			op: UnaryOp::Neg,
			operand: Box::new(self),
		}
	}

	// Null checks
	pub fn is_null(self) -> ExprBuilder {
		ExprBuilder::UnaryOp {
			op: UnaryOp::IsNull,
			operand: Box::new(self),
		}
	}

	pub fn is_not_null(self) -> ExprBuilder {
		ExprBuilder::UnaryOp {
			op: UnaryOp::IsNotNull,
			operand: Box::new(self),
		}
	}

	/// Compile the expression builder to a resolved Expr.
	/// Resolves column names to indices using the provided schema.
	pub fn compile(self, schema: &[ColumnSchema]) -> Result<Expr> {
		match self {
			ExprBuilder::Column(name) => {
				let col_schema = schema.iter().find(|c| c.name == name).ok_or_else(|| {
					VmError::ColumnNotFound {
						name: name.clone(),
					}
				})?;

				Ok(Expr::ColumnRef(ColumnRef {
					index: col_schema.index,
					name,
				}))
			}
			ExprBuilder::Literal(lit) => Ok(Expr::Literal(lit)),
			ExprBuilder::BinaryOp {
				op,
				left,
				right,
			} => Ok(Expr::BinaryOp {
				op,
				left: Box::new(left.compile(schema)?),
				right: Box::new(right.compile(schema)?),
			}),
			ExprBuilder::UnaryOp {
				op,
				operand,
			} => Ok(Expr::UnaryOp {
				op,
				operand: Box::new(operand.compile(schema)?),
			}),
		}
	}
}
