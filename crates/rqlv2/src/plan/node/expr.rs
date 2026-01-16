// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Plan expressions - resolved and ready for execution.

use reifydb_type::value::r#type::Type;

use crate::{
	plan::{
		Plan,
		types::{Column, Function, Variable},
	},
	token::span::Span,
};

/// Plan expression - resolved and ready for execution.
#[derive(Debug, Clone, Copy)]
pub enum PlanExpr<'bump> {
	// === Literals ===
	/// Undefined literal
	LiteralUndefined(Span),
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
	/// EXISTS subquery - returns boolean
	Exists {
		subquery: &'bump Plan<'bump>,
		negated: bool,
		span: Span,
	},
	/// IN with subquery
	InSubquery {
		expr: &'bump PlanExpr<'bump>,
		subquery: &'bump Plan<'bump>,
		negated: bool,
		span: Span,
	},
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
	/// Field access: expr.field (e.g., $user.id)
	FieldAccess {
		base: &'bump PlanExpr<'bump>,
		field: &'bump str,
		span: Span,
	},
	/// Script function call (user-defined function)
	CallScriptFunction {
		name: &'bump str,
		arguments: &'bump [&'bump PlanExpr<'bump>],
		span: Span,
	},
}

impl<'bump> PlanExpr<'bump> {
	/// Get the span of this expression.
	pub fn span(&self) -> Span {
		match self {
			PlanExpr::LiteralUndefined(s) => *s,
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
			PlanExpr::Exists {
				span,
				..
			} => *span,
			PlanExpr::InSubquery {
				span,
				..
			} => *span,
			PlanExpr::List(_, s) => *s,
			PlanExpr::Tuple(_, s) => *s,
			PlanExpr::Record(_, s) => *s,
			PlanExpr::Alias {
				span,
				..
			} => *span,
			PlanExpr::FieldAccess {
				span,
				..
			} => *span,
			PlanExpr::CallScriptFunction {
				span,
				..
			} => *span,
		}
	}

	/// Check if this expression contains any script function calls.
	///
	/// This is used to determine whether bytecode-based evaluation is needed.
	pub fn contains_script_function_call(&self) -> bool {
		match self {
			// Leaves - no script function calls
			PlanExpr::LiteralUndefined(_)
			| PlanExpr::LiteralBool(_, _)
			| PlanExpr::LiteralInt(_, _)
			| PlanExpr::LiteralFloat(_, _)
			| PlanExpr::LiteralString(_, _)
			| PlanExpr::LiteralBytes(_, _)
			| PlanExpr::Column(_)
			| PlanExpr::Variable(_)
			| PlanExpr::Rownum(_)
			| PlanExpr::Wildcard(_) => false,

			// Script function call - this is what we're looking for
			PlanExpr::CallScriptFunction {
				..
			} => true,

			// Recursive cases - check children
			PlanExpr::Binary {
				left,
				right,
				..
			} => left.contains_script_function_call() || right.contains_script_function_call(),
			PlanExpr::Unary {
				operand,
				..
			} => operand.contains_script_function_call(),
			PlanExpr::Between {
				expr,
				low,
				high,
				..
			} => {
				expr.contains_script_function_call()
					|| low.contains_script_function_call()
					|| high.contains_script_function_call()
			}
			PlanExpr::In {
				expr,
				list,
				..
			} => {
				expr.contains_script_function_call()
					|| list.iter().any(|e| e.contains_script_function_call())
			}
			PlanExpr::Cast {
				expr,
				..
			} => expr.contains_script_function_call(),
			PlanExpr::Call {
				arguments,
				..
			} => arguments.iter().any(|e| e.contains_script_function_call()),
			PlanExpr::Aggregate {
				arguments,
				..
			} => arguments.iter().any(|e| e.contains_script_function_call()),
			PlanExpr::Conditional {
				condition,
				then_expr,
				else_expr,
				..
			} => {
				condition.contains_script_function_call()
					|| then_expr.contains_script_function_call()
					|| else_expr.contains_script_function_call()
			}
			PlanExpr::Subquery(_) => false, // Subqueries are separate execution contexts
			PlanExpr::Exists {
				..
			} => false, // Subqueries are separate execution contexts
			PlanExpr::InSubquery {
				expr,
				..
			} => expr.contains_script_function_call(),
			PlanExpr::List(items, _) | PlanExpr::Tuple(items, _) => {
				items.iter().any(|e| e.contains_script_function_call())
			}
			PlanExpr::Record(fields, _) => fields.iter().any(|(_, e)| e.contains_script_function_call()),
			PlanExpr::Alias {
				expr,
				..
			} => expr.contains_script_function_call(),
			PlanExpr::FieldAccess {
				base,
				..
			} => base.contains_script_function_call(),
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
