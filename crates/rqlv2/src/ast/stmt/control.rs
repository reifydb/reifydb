// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Control flow statement types (if, loop, for, break, continue, return).

use super::{Expr, Statement};
use crate::token::Span;

/// If statement: if cond { then } else if cond { ... } else { else }
#[derive(Debug, Clone, Copy)]
pub struct IfStmt<'bump> {
	pub condition: &'bump Expr<'bump>,
	pub then_branch: &'bump [Statement<'bump>],
	pub else_ifs: &'bump [ElseIfBranch<'bump>],
	pub else_branch: Option<&'bump [Statement<'bump>]>,
	pub span: Span,
}

impl<'bump> IfStmt<'bump> {
	/// Create a new if statement.
	pub fn new(
		condition: &'bump Expr<'bump>,
		then_branch: &'bump [Statement<'bump>],
		else_ifs: &'bump [ElseIfBranch<'bump>],
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

/// Else-if branch in an if statement.
#[derive(Debug, Clone, Copy)]
pub struct ElseIfBranch<'bump> {
	pub condition: &'bump Expr<'bump>,
	pub body: &'bump [Statement<'bump>],
	pub span: Span,
}

impl<'bump> ElseIfBranch<'bump> {
	/// Create a new else-if branch.
	pub fn new(condition: &'bump Expr<'bump>, body: &'bump [Statement<'bump>], span: Span) -> Self {
		Self {
			condition,
			body,
			span,
		}
	}
}

/// Loop statement: loop { body }
#[derive(Debug, Clone, Copy)]
pub struct LoopStmt<'bump> {
	pub body: &'bump [Statement<'bump>],
	pub span: Span,
}

impl<'bump> LoopStmt<'bump> {
	/// Create a new loop statement.
	pub fn new(body: &'bump [Statement<'bump>], span: Span) -> Self {
		Self {
			body,
			span,
		}
	}
}

/// For loop: for $var in iterable { body }
#[derive(Debug, Clone, Copy)]
pub struct ForStmt<'bump> {
	/// Variable name (without $)
	pub variable: &'bump str,
	pub iterable: &'bump Expr<'bump>,
	pub body: &'bump [Statement<'bump>],
	pub span: Span,
}

impl<'bump> ForStmt<'bump> {
	/// Create a new for statement.
	pub fn new(
		variable: &'bump str,
		iterable: &'bump Expr<'bump>,
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

/// Break statement: break
#[derive(Debug, Clone, Copy)]
pub struct BreakStmt {
	pub span: Span,
}

impl BreakStmt {
	/// Create a new break statement.
	pub fn new(span: Span) -> Self {
		Self {
			span,
		}
	}
}

/// Continue statement: continue
#[derive(Debug, Clone, Copy)]
pub struct ContinueStmt {
	pub span: Span,
}

impl ContinueStmt {
	/// Create a new continue statement.
	pub fn new(span: Span) -> Self {
		Self {
			span,
		}
	}
}

/// Return statement: return or return expr
#[derive(Debug, Clone, Copy)]
pub struct ReturnStmt<'bump> {
	pub value: Option<&'bump Expr<'bump>>,
	pub span: Span,
}

impl<'bump> ReturnStmt<'bump> {
	/// Create a new return statement.
	pub fn new(value: Option<&'bump Expr<'bump>>, span: Span) -> Self {
		Self {
			value,
			span,
		}
	}
}
