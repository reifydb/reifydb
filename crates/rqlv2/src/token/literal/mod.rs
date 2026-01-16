// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Literal value kinds and scanners.

pub mod number;
pub mod temporal;
pub mod text;

/// Literal value kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LiteralKind {
	/// Integer literal (raw text in span: 42, 0xFF, 0b1010, 0o777).
	Integer,

	/// Float literal (raw text in span: 3.14, 1e10).
	Float,

	/// String literal (raw content between quotes, no escape processing).
	String,

	/// Boolean true.
	True,

	/// Boolean false.
	False,

	/// Undefined/null literal.
	Undefined,

	/// Temporal literal (date/time starting with @, span excludes @).
	/// Examples: @2024-01-15, @2024-01-15T10:30:00, @10:30:00
	Temporal,
}
