// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Statement types for the unified AST.

pub mod binding;
pub mod control;
pub mod ddl;
pub mod dml;

pub use binding::*;
pub use control::*;
pub use ddl::*;
pub use dml::*;

use super::{Expr, Pipeline};
use crate::token::Span;

/// Top-level statement.
#[derive(Debug, Clone, Copy)]
pub enum Statement<'bump> {
	// === Query/Pipeline ===
	/// A pipeline of query operations (FROM ... | FILTER ... | MAP ...)
	Pipeline(Pipeline<'bump>),

	// === Control Flow ===
	/// Let binding: let $name := expr
	Let(LetStmt<'bump>),
	/// Assignment to existing variable: $name := expr
	Assign(AssignStmt<'bump>),
	/// Function definition: def name(params) { body }
	Def(DefStmt<'bump>),
	/// If/else: if cond { then } else { else }
	If(IfStmt<'bump>),
	/// Loop: loop { body }
	Loop(LoopStmt<'bump>),
	/// For loop: for $var in iterable { body }
	For(ForStmt<'bump>),
	/// Break from loop
	Break(BreakStmt),
	/// Continue to next iteration
	Continue(ContinueStmt),
	/// Return from function
	Return(ReturnStmt<'bump>),

	// === DDL ===
	/// CREATE statement
	Create(CreateStmt<'bump>),
	/// ALTER statement
	Alter(AlterStmt<'bump>),
	/// DROP statement
	Drop(DropStmt<'bump>),

	// === DML ===
	/// INSERT statement
	Insert(InsertStmt<'bump>),
	/// UPDATE statement
	Update(UpdateStmt<'bump>),
	/// DELETE statement
	Delete(DeleteStmt<'bump>),

	// === Meta ===
	/// DESCRIBE statement
	Describe(DescribeStmt<'bump>),

	// === Expression Statement ===
	/// Bare expression (for implicit return or side effects)
	Expression(ExprStmt<'bump>),
}

impl<'bump> Statement<'bump> {
	/// Get the span of this statement.
	pub fn span(&self) -> Span {
		match self {
			Statement::Pipeline(p) => p.span,
			Statement::Let(l) => l.span,
			Statement::Assign(a) => a.span,
			Statement::Def(d) => d.span,
			Statement::If(i) => i.span,
			Statement::Loop(l) => l.span,
			Statement::For(f) => f.span,
			Statement::Break(b) => b.span,
			Statement::Continue(c) => c.span,
			Statement::Return(r) => r.span,
			Statement::Create(c) => c.span(),
			Statement::Alter(a) => a.span(),
			Statement::Drop(d) => d.span,
			Statement::Insert(i) => i.span,
			Statement::Update(u) => u.span,
			Statement::Delete(d) => d.span,
			Statement::Describe(d) => d.span,
			Statement::Expression(e) => e.span,
		}
	}
}

/// Expression statement - bare expression.
#[derive(Debug, Clone, Copy)]
pub struct ExprStmt<'bump> {
	pub expr: &'bump Expr<'bump>,
	pub span: Span,
}

impl<'bump> ExprStmt<'bump> {
	/// Create a new expression statement.
	pub fn new(expr: &'bump Expr<'bump>, span: Span) -> Self {
		Self {
			expr,
			span,
		}
	}
}

/// DESCRIBE statement.
#[derive(Debug, Clone, Copy)]
pub struct DescribeStmt<'bump> {
	pub target: &'bump Expr<'bump>,
	pub span: Span,
}

impl<'bump> DescribeStmt<'bump> {
	/// Create a new describe statement.
	pub fn new(target: &'bump Expr<'bump>, span: Span) -> Self {
		Self {
			target,
			span,
		}
	}
}
