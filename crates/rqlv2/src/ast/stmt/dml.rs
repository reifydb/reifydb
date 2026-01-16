// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DML statement types (INSERT, UPDATE, DELETE).

use super::Expr;
use crate::token::span::Span;

/// INSERT statement.
#[derive(Debug, Clone, Copy)]
pub struct InsertStmt<'bump> {
	pub namespace: Option<&'bump str>,
	pub table: &'bump str,
	pub columns: Option<&'bump [&'bump str]>,
	pub source: InsertSource<'bump>,
	pub span: Span,
}

impl<'bump> InsertStmt<'bump> {
	/// Create a new INSERT statement.
	pub fn new(
		namespace: Option<&'bump str>,
		table: &'bump str,
		columns: Option<&'bump [&'bump str]>,
		source: InsertSource<'bump>,
		span: Span,
	) -> Self {
		Self {
			namespace,
			table,
			columns,
			source,
			span,
		}
	}
}

/// Source of INSERT values.
#[derive(Debug, Clone, Copy)]
pub enum InsertSource<'bump> {
	/// VALUES clause with explicit rows
	Values(&'bump [&'bump [Expr<'bump>]]),
	/// Query/pipeline as source
	Query(&'bump [Expr<'bump>]),
}

/// UPDATE statement.
#[derive(Debug, Clone, Copy)]
pub struct UpdateStmt<'bump> {
	pub namespace: Option<&'bump str>,
	pub table: &'bump str,
	pub assignments: &'bump [UpdateAssignment<'bump>],
	pub filter: Option<&'bump Expr<'bump>>,
	pub span: Span,
}

impl<'bump> UpdateStmt<'bump> {
	/// Create a new UPDATE statement.
	pub fn new(
		namespace: Option<&'bump str>,
		table: &'bump str,
		assignments: &'bump [UpdateAssignment<'bump>],
		filter: Option<&'bump Expr<'bump>>,
		span: Span,
	) -> Self {
		Self {
			namespace,
			table,
			assignments,
			filter,
			span,
		}
	}
}

/// Assignment in UPDATE statement.
#[derive(Debug, Clone, Copy)]
pub struct UpdateAssignment<'bump> {
	pub column: &'bump str,
	pub value: &'bump Expr<'bump>,
}

impl<'bump> UpdateAssignment<'bump> {
	/// Create a new update assignment.
	pub fn new(column: &'bump str, value: &'bump Expr<'bump>) -> Self {
		Self {
			column,
			value,
		}
	}
}

/// DELETE statement.
#[derive(Debug, Clone, Copy)]
pub struct DeleteStmt<'bump> {
	pub namespace: Option<&'bump str>,
	pub table: &'bump str,
	pub filter: Option<&'bump Expr<'bump>>,
	pub span: Span,
}

impl<'bump> DeleteStmt<'bump> {
	/// Create a new DELETE statement.
	pub fn new(
		namespace: Option<&'bump str>,
		table: &'bump str,
		filter: Option<&'bump Expr<'bump>>,
		span: Span,
	) -> Self {
		Self {
			namespace,
			table,
			filter,
			span,
		}
	}
}
