// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Special expression types (BETWEEN, IN, CAST, CALL, etc.).

use super::Expr;
use crate::token::Span;

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

/// Conditional expression (in expression context): if cond then else
#[derive(Debug, Clone, Copy)]
pub struct IfExpr<'bump> {
	pub condition: &'bump Expr<'bump>,
	pub then_branch: &'bump Expr<'bump>,
	pub else_ifs: &'bump [ElseIf<'bump>],
	pub else_branch: Option<&'bump Expr<'bump>>,
	pub span: Span,
}

impl<'bump> IfExpr<'bump> {
	/// Create a new if expression.
	pub fn new(
		condition: &'bump Expr<'bump>,
		then_branch: &'bump Expr<'bump>,
		else_ifs: &'bump [ElseIf<'bump>],
		else_branch: Option<&'bump Expr<'bump>>,
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
	pub then_branch: &'bump Expr<'bump>,
	pub span: Span,
}

impl<'bump> ElseIf<'bump> {
	/// Create a new else-if branch.
	pub fn new(condition: &'bump Expr<'bump>, then_branch: &'bump Expr<'bump>, span: Span) -> Self {
		Self {
			condition,
			then_branch,
			span,
		}
	}
}
