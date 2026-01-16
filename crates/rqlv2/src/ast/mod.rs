// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Unified AST for RQL querying and scripting.
//!
//! This module provides a bump-allocated AST that supports:
//! - All RQL query constructs (FROM, FILTER, MAP, JOIN, etc.)
//! - Scripting constructs (let, def, if, loop, for, etc.)
//! - DDL statements (CREATE, ALTER, DROP)
//! - DML statements (INSERT, UPDATE, DELETE)

pub mod explain;
pub mod expr;
pub mod parse;
pub mod stmt;

use crate::{
	ast::{expr::Expr, stmt::Statement},
	token::span::Span,
};

/// A complete program - the root of the AST.
///
/// Lifetime `'bump` ties all AST nodes to the bump allocator.
#[derive(Debug, Clone, Copy)]
pub struct Program<'bump> {
	pub statements: &'bump [Statement<'bump>],
	pub span: Span,
}

impl<'bump> Program<'bump> {
	/// Create a new program with the given statements.
	pub fn new(statements: &'bump [Statement<'bump>], span: Span) -> Self {
		Self {
			statements,
			span,
		}
	}

	/// Check if this program is empty.
	pub fn is_empty(&self) -> bool {
		self.statements.is_empty()
	}

	/// Get the number of statements.
	pub fn len(&self) -> usize {
		self.statements.len()
	}
}

/// A pipeline of query stages connected by `|`.
#[derive(Debug, Clone, Copy)]
pub struct Pipeline<'bump> {
	pub stages: &'bump [Expr<'bump>],
	pub span: Span,
}

impl<'bump> Pipeline<'bump> {
	/// Create a new pipeline with the given stages.
	pub fn new(stages: &'bump [Expr<'bump>], span: Span) -> Self {
		Self {
			stages,
			span,
		}
	}

	/// Check if this pipeline is empty.
	pub fn is_empty(&self) -> bool {
		self.stages.is_empty()
	}

	/// Get the number of stages.
	pub fn len(&self) -> usize {
		self.stages.len()
	}
}
