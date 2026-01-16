// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::token::span::Span;

/// Simple identifier: column_name, table_name
#[derive(Debug, Clone, Copy)]
pub struct Identifier<'bump> {
	pub name: &'bump str,
	pub span: Span,
}

impl<'bump> Identifier<'bump> {
	/// Create a new identifier.
	pub fn new(name: &'bump str, span: Span) -> Self {
		Self {
			name,
			span,
		}
	}
}

/// Qualified identifier: namespace.table, table.column
#[derive(Debug, Clone, Copy)]
pub struct QualifiedIdent<'bump> {
	/// Parts of the qualified identifier (e.g., ["namespace", "table", "column"])
	pub parts: &'bump [&'bump str],
	pub span: Span,
}

impl<'bump> QualifiedIdent<'bump> {
	/// Create a new qualified identifier.
	pub fn new(parts: &'bump [&'bump str], span: Span) -> Self {
		Self {
			parts,
			span,
		}
	}

	/// Get the first part (namespace/schema).
	pub fn namespace(&self) -> Option<&'bump str> {
		if self.parts.len() > 1 {
			Some(self.parts[0])
		} else {
			None
		}
	}

	/// Get the last part (name).
	pub fn name(&self) -> &'bump str {
		self.parts.last().copied().unwrap_or("")
	}
}

/// Variable reference: $name, $123
#[derive(Debug, Clone, Copy)]
pub struct Variable<'bump> {
	/// Variable name (without the $ prefix)
	pub name: &'bump str,
	pub span: Span,
}

impl<'bump> Variable<'bump> {
	/// Create a new variable reference.
	pub fn new(name: &'bump str, span: Span) -> Self {
		Self {
			name,
			span,
		}
	}
}

/// Wildcard: *
#[derive(Debug, Clone, Copy)]
pub struct WildcardExpr {
	pub span: Span,
}

impl WildcardExpr {
	/// Create a new wildcard expression.
	pub fn new(span: Span) -> Self {
		Self {
			span,
		}
	}
}

/// ROWNUM pseudo-column
#[derive(Debug, Clone, Copy)]
pub struct RownumExpr {
	pub span: Span,
}

impl RownumExpr {
	/// Create a new rownum expression.
	pub fn new(span: Span) -> Self {
		Self {
			span,
		}
	}
}

/// $env environment reference
#[derive(Debug, Clone, Copy)]
pub struct EnvironmentExpr {
	pub span: Span,
}

impl EnvironmentExpr {
	/// Create a new environment expression.
	pub fn new(span: Span) -> Self {
		Self {
			span,
		}
	}
}
