// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Special expression types (BETWEEN, IN, CAST, CALL, etc.).

use super::Expr;
use crate::{ast::Statement, token::Span};

/// BETWEEN expression: x BETWEEN low AND high
#[derive(Debug, Clone, Copy)]
pub struct BetweenExpr<'bump> {
	pub value: &'bump Expr<'bump>,
	pub lower: &'bump Expr<'bump>,
	pub upper: &'bump Expr<'bump>,
	pub span: Span,
}

impl<'bump> BetweenExpr<'bump> {
	/// Create a new between expression.
	pub fn new(
		value: &'bump Expr<'bump>,
		lower: &'bump Expr<'bump>,
		upper: &'bump Expr<'bump>,
		span: Span,
	) -> Self {
		Self {
			value,
			lower,
			upper,
			span,
		}
	}
}

/// IN expression: x IN [values] or x NOT IN [values]
#[derive(Debug, Clone, Copy)]
pub struct InExpr<'bump> {
	pub value: &'bump Expr<'bump>,
	pub list: &'bump Expr<'bump>,
	pub negated: bool,
	pub span: Span,
}

impl<'bump> InExpr<'bump> {
	/// Create a new in expression.
	pub fn new(value: &'bump Expr<'bump>, list: &'bump Expr<'bump>, negated: bool, span: Span) -> Self {
		Self {
			value,
			list,
			negated,
			span,
		}
	}
}

/// CAST expression: CAST(expr, type)
#[derive(Debug, Clone, Copy)]
pub struct CastExpr<'bump> {
	pub expr: &'bump Expr<'bump>,
	pub target_type: &'bump Expr<'bump>,
	pub span: Span,
}

impl<'bump> CastExpr<'bump> {
	/// Create a new cast expression.
	pub fn new(expr: &'bump Expr<'bump>, target_type: &'bump Expr<'bump>, span: Span) -> Self {
		Self {
			expr,
			target_type,
			span,
		}
	}
}

/// Function call: func(args) or namespace::func(args)
#[derive(Debug, Clone, Copy)]
pub struct CallExpr<'bump> {
	/// Function reference (can be qualified: ns::func)
	pub function: &'bump Expr<'bump>,
	pub arguments: &'bump [Expr<'bump>],
	pub span: Span,
}

impl<'bump> CallExpr<'bump> {
	/// Create a new call expression.
	pub fn new(function: &'bump Expr<'bump>, arguments: &'bump [Expr<'bump>], span: Span) -> Self {
		Self {
			function,
			arguments,
			span,
		}
	}
}

/// APPLY expression: APPLY operator expressions
#[derive(Debug, Clone, Copy)]
pub struct ApplyExpr<'bump> {
	pub operator: &'bump str,
	pub expressions: &'bump [Expr<'bump>],
	pub span: Span,
}

impl<'bump> ApplyExpr<'bump> {
	/// Create a new apply expression.
	pub fn new(operator: &'bump str, expressions: &'bump [Expr<'bump>], span: Span) -> Self {
		Self {
			operator,
			expressions,
			span,
		}
	}
}

/// Subquery: { FROM ... | ... }
#[derive(Debug, Clone, Copy)]
pub struct SubQueryExpr<'bump> {
	pub pipeline: &'bump [Expr<'bump>],
	pub span: Span,
}

impl<'bump> SubQueryExpr<'bump> {
	/// Create a new subquery expression.
	pub fn new(pipeline: &'bump [Expr<'bump>], span: Span) -> Self {
		Self {
			pipeline,
			span,
		}
	}
}

/// Conditional expression: if cond { then } else if cond { ... } else { else }
///
/// In an expression-oriented language, `if` is an expression that produces a value.
/// The value is the result of the last expression in the taken branch.
#[derive(Debug, Clone, Copy)]
pub struct IfExpr<'bump> {
	pub condition: &'bump Expr<'bump>,
	pub then_branch: &'bump [Statement<'bump>],
	pub else_ifs: &'bump [ElseIf<'bump>],
	pub else_branch: Option<&'bump [Statement<'bump>]>,
	pub span: Span,
}

impl<'bump> IfExpr<'bump> {
	/// Create a new if expression.
	pub fn new(
		condition: &'bump Expr<'bump>,
		then_branch: &'bump [Statement<'bump>],
		else_ifs: &'bump [ElseIf<'bump>],
		else_branch: Option<&'bump [Statement<'bump>]>,
		span: Span,
	) -> Self {
		Self {
			condition,
			then_branch,
			else_ifs,
			else_branch,
			span,
		}
	}
}

/// Else-if branch
#[derive(Debug, Clone, Copy)]
pub struct ElseIf<'bump> {
	pub condition: &'bump Expr<'bump>,
	pub body: &'bump [Statement<'bump>],
	pub span: Span,
}

impl<'bump> ElseIf<'bump> {
	/// Create a new else-if branch.
	pub fn new(condition: &'bump Expr<'bump>, body: &'bump [Statement<'bump>], span: Span) -> Self {
		Self {
			condition,
			body,
			span,
		}
	}
}

/// Loop expression: loop { body }
///
/// In an expression-oriented language, `loop` produces a value via `break value`.
/// If no value is provided to break, the loop returns undefined.
#[derive(Debug, Clone, Copy)]
pub struct LoopExpr<'bump> {
	pub body: &'bump [Statement<'bump>],
	pub span: Span,
}

impl<'bump> LoopExpr<'bump> {
	/// Create a new loop expression.
	pub fn new(body: &'bump [Statement<'bump>], span: Span) -> Self {
		Self {
			body,
			span,
		}
	}
}

/// For loop expression: for $var in iterable { body }
///
/// Returns the collected values or undefined.
#[derive(Debug, Clone, Copy)]
pub struct ForExpr<'bump> {
	/// Variable name (without $)
	pub variable: &'bump str,
	pub iterable: ForIterable<'bump>,
	pub body: &'bump [Statement<'bump>],
	pub span: Span,
}

impl<'bump> ForExpr<'bump> {
	/// Create a new for expression.
	pub fn new(
		variable: &'bump str,
		iterable: ForIterable<'bump>,
		body: &'bump [Statement<'bump>],
		span: Span,
	) -> Self {
		Self {
			variable,
			iterable,
			body,
			span,
		}
	}
}

/// For loop iterable - can be a single expression or a pipeline.
#[derive(Debug, Clone, Copy)]
pub enum ForIterable<'bump> {
	/// Single expression (e.g., `$array`, `range(1, 10)`)
	Expr(&'bump Expr<'bump>),
	/// Pipeline stages (e.g., `from table | filter x > 0`)
	Pipeline(&'bump [Expr<'bump>]),
}

/// EXISTS expression: EXISTS(subquery) or NOT EXISTS(subquery)
#[derive(Debug, Clone, Copy)]
pub struct ExistsExpr<'bump> {
	pub subquery: &'bump Expr<'bump>,
	pub negated: bool,
	pub span: Span,
}

impl<'bump> ExistsExpr<'bump> {
	/// Create a new EXISTS expression.
	pub fn new(subquery: &'bump Expr<'bump>, negated: bool, span: Span) -> Self {
		Self {
			subquery,
			negated,
			span,
		}
	}
}
