// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Binding statement types (let, def, assign).

use super::{Expr, Statement};
use crate::token::span::Span;

/// Let binding: let $name = expr
#[derive(Debug, Clone, Copy)]
pub struct LetStmt<'bump> {
	/// Variable name (without $)
	pub name: &'bump str,
	pub value: LetValue<'bump>,
	pub span: Span,
}

impl<'bump> LetStmt<'bump> {
	/// Create a new let statement.
	pub fn new(name: &'bump str, value: LetValue<'bump>, span: Span) -> Self {
		Self {
			name,
			value,
			span,
		}
	}
}

/// Value in a let statement - can be expression or pipeline.
#[derive(Debug, Clone, Copy)]
pub enum LetValue<'bump> {
	/// Scalar expression
	Expr(&'bump Expr<'bump>),
	/// Pipeline (multiple stages)
	Pipeline(&'bump [Expr<'bump>]),
}

impl<'bump> LetValue<'bump> {
	/// Get the span of this let value.
	pub fn span(&self) -> Span {
		match self {
			LetValue::Expr(e) => e.span(),
			LetValue::Pipeline(p) => {
				if p.is_empty() {
					Span::default()
				} else {
					p.first().unwrap().span().merge(&p.last().unwrap().span())
				}
			}
		}
	}
}

/// Assignment to existing variable: $name = expr
#[derive(Debug, Clone, Copy)]
pub struct AssignStmt<'bump> {
	/// Variable name (without $)
	pub name: &'bump str,
	pub value: &'bump Expr<'bump>,
	pub span: Span,
}

impl<'bump> AssignStmt<'bump> {
	/// Create a new assignment statement.
	pub fn new(name: &'bump str, value: &'bump Expr<'bump>, span: Span) -> Self {
		Self {
			name,
			value,
			span,
		}
	}
}

/// Function definition: def name(params) { body }
#[derive(Debug, Clone, Copy)]
pub struct DefStmt<'bump> {
	pub name: &'bump str,
	pub parameters: &'bump [Parameter<'bump>],
	pub body: &'bump [Statement<'bump>],
	pub span: Span,
}

impl<'bump> DefStmt<'bump> {
	/// Create a new function definition.
	pub fn new(
		name: &'bump str,
		parameters: &'bump [Parameter<'bump>],
		body: &'bump [Statement<'bump>],
		span: Span,
	) -> Self {
		Self {
			name,
			parameters,
			body,
			span,
		}
	}
}

/// Function parameter.
#[derive(Debug, Clone, Copy)]
pub struct Parameter<'bump> {
	pub name: &'bump str,
	pub param_type: Option<&'bump str>,
	pub span: Span,
}

impl<'bump> Parameter<'bump> {
	/// Create a new parameter.
	pub fn new(name: &'bump str, param_type: Option<&'bump str>, span: Span) -> Self {
		Self {
			name,
			param_type,
			span,
		}
	}
}
