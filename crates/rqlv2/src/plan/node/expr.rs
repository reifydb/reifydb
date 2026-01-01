// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Plan expressions - resolved and ready for execution.

use crate::{
	plan::{Column, Function, Plan, Type, Variable},
	token::Span,
};

/// Plan expression - resolved and ready for execution.
#[derive(Debug, Clone, Copy)]
pub enum PlanExpr<'bump> {
	// === Literals ===
	/// Null literal
	LiteralNull(Span),
	/// Boolean literal
	LiteralBool(bool, Span),
	/// Integer literal
	LiteralInt(i64, Span),
	/// Float literal
	LiteralFloat(f64, Span),
	/// String literal
	LiteralString(&'bump str, Span),
	/// Binary literal
	LiteralBytes(&'bump [u8], Span),

	// === References ===
	/// Resolved column reference
	Column(Column<'bump>),
	/// Resolved variable reference
	Variable(&'bump Variable<'bump>),
	/// Rownum pseudo-column
	Rownum(Span),
	/// Wildcard (for projection)
	Wildcard(Span),

	// === Operators ===
	/// Binary operation
	Binary {
		op: BinaryPlanOp,
		left: &'bump PlanExpr<'bump>,
		right: &'bump PlanExpr<'bump>,
		span: Span,
	},
	/// Unary operation
	Unary {
		op: UnaryPlanOp,
		operand: &'bump PlanExpr<'bump>,
		span: Span,
	},

	// === Special ===
	/// BETWEEN: expr BETWEEN low AND high
	Between {
		expr: &'bump PlanExpr<'bump>,
		low: &'bump PlanExpr<'bump>,
		high: &'bump PlanExpr<'bump>,
		negated: bool,
		span: Span,
	},
	/// IN: expr IN list
	In {
		expr: &'bump PlanExpr<'bump>,
		list: &'bump [&'bump PlanExpr<'bump>],
		negated: bool,
		span: Span,
	},
	/// CAST: cast(expr, type)
	Cast {
		expr: &'bump PlanExpr<'bump>,
		target_type: Type,
		span: Span,
	},
	/// Function call
	Call {
		function: &'bump Function<'bump>,
		arguments: &'bump [&'bump PlanExpr<'bump>],
		span: Span,
	},
	/// Aggregate function call
	Aggregate {
		function: &'bump Function<'bump>,
		arguments: &'bump [&'bump PlanExpr<'bump>],
		distinct: bool,
		span: Span,
	},
	/// Conditional: if/then/else
	Conditional {
		condition: &'bump PlanExpr<'bump>,
		then_expr: &'bump PlanExpr<'bump>,
		else_expr: &'bump PlanExpr<'bump>,
		span: Span,
	},
	/// Subquery (becomes a nested plan)
	Subquery(&'bump Plan<'bump>),
	/// List expression [a, b, c]
	List(&'bump [&'bump PlanExpr<'bump>], Span),
	/// Tuple expression (a, b, c)
	Tuple(&'bump [&'bump PlanExpr<'bump>], Span),
	/// Inline record { key: value, ... }
	Record(&'bump [(&'bump str, &'bump PlanExpr<'bump>)], Span),
	/// Aliased expression: expr AS name
	Alias {
		expr: &'bump PlanExpr<'bump>,
		alias: &'bump str,
		span: Span,
	},
}

impl<'bump> PlanExpr<'bump> {
	/// Get the span of this expression.
	pub fn span(&self) -> Span {
		match self {
			PlanExpr::LiteralNull(s) => *s,
			PlanExpr::LiteralBool(_, s) => *s,
			PlanExpr::LiteralInt(_, s) => *s,
			PlanExpr::LiteralFloat(_, s) => *s,
			PlanExpr::LiteralString(_, s) => *s,
			PlanExpr::LiteralBytes(_, s) => *s,
			PlanExpr::Column(c) => c.span(),
			PlanExpr::Variable(v) => v.span,
			PlanExpr::Rownum(s) => *s,
			PlanExpr::Wildcard(s) => *s,
			PlanExpr::Binary {
				span,
				..
			} => *span,
			PlanExpr::Unary {
				span,
				..
			} => *span,
			PlanExpr::Between {
				span,
				..
			} => *span,
			PlanExpr::In {
				span,
				..
			} => *span,
			PlanExpr::Cast {
				span,
				..
			} => *span,
			PlanExpr::Call {
				span,
				..
			} => *span,
			PlanExpr::Aggregate {
				span,
				..
			} => *span,
			PlanExpr::Conditional {
				span,
				..
			} => *span,
			PlanExpr::Subquery(p) => p.span(),
			PlanExpr::List(_, s) => *s,
			PlanExpr::Tuple(_, s) => *s,
			PlanExpr::Record(_, s) => *s,
			PlanExpr::Alias {
				span,
				..
			} => *span,
		}
	}
}

/// Binary operators for plan expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryPlanOp {
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

	// String
	Concat,
}

/// Unary operators for plan expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryPlanOp {
	Neg,
	Not,
	Plus,
}
