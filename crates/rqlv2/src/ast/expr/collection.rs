// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Collection expression types.

use super::Expr;
use crate::token::Span;

/// List expression: [a, b, c]
#[derive(Debug, Clone, Copy)]
pub struct ListExpr<'bump> {
	pub elements: &'bump [Expr<'bump>],
	pub span: Span,
}

impl<'bump> ListExpr<'bump> {
	/// Create a new list expression.
	pub fn new(elements: &'bump [Expr<'bump>], span: Span) -> Self {
		Self {
			elements,
			span,
		}
	}

	/// Check if this list is empty.
	pub fn is_empty(&self) -> bool {
		self.elements.is_empty()
	}

	/// Get the number of elements.
	pub fn len(&self) -> usize {
		self.elements.len()
	}
}

/// Tuple expression: (a, b, c)
#[derive(Debug, Clone, Copy)]
pub struct TupleExpr<'bump> {
	pub elements: &'bump [Expr<'bump>],
	pub span: Span,
}

impl<'bump> TupleExpr<'bump> {
	/// Create a new tuple expression.
	pub fn new(elements: &'bump [Expr<'bump>], span: Span) -> Self {
		Self {
			elements,
			span,
		}
	}

	/// Check if this tuple is empty.
	pub fn is_empty(&self) -> bool {
		self.elements.is_empty()
	}

	/// Get the number of elements.
	pub fn len(&self) -> usize {
		self.elements.len()
	}
}

/// Inline object/record: { key: value, ... }
#[derive(Debug, Clone, Copy)]
pub struct InlineExpr<'bump> {
	pub fields: &'bump [InlineField<'bump>],
	pub span: Span,
}

impl<'bump> InlineExpr<'bump> {
	/// Create a new inline expression.
	pub fn new(fields: &'bump [InlineField<'bump>], span: Span) -> Self {
		Self {
			fields,
			span,
		}
	}

	/// Check if this inline is empty.
	pub fn is_empty(&self) -> bool {
		self.fields.is_empty()
	}

	/// Get the number of fields.
	pub fn len(&self) -> usize {
		self.fields.len()
	}
}

/// Inline field: key: value
#[derive(Debug, Clone, Copy)]
pub struct InlineField<'bump> {
	pub key: &'bump str,
	pub value: &'bump Expr<'bump>,
}

impl<'bump> InlineField<'bump> {
	/// Create a new inline field.
	pub fn new(key: &'bump str, value: &'bump Expr<'bump>) -> Self {
		Self {
			key,
			value,
		}
	}
}
