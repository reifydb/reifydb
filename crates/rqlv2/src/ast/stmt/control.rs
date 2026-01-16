// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Control flow statement types (break, continue, return).
//!
//! Note: `if`, `loop`, and `for` are now unified as expressions
//! (`IfExpr`, `LoopExpr`, `ForExpr`) in the expression module.

use super::Expr;
use crate::token::span::Span;

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
