// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Literal expression types.

use crate::token::span::Span;

/// Literal values.
#[derive(Debug, Clone, Copy)]
pub enum Literal<'bump> {
	/// Integer literal: 42, 0xFF, 0b1010
	Integer {
		/// Raw text of the integer (for preserving original format)
		value: &'bump str,
		span: Span,
	},
	/// Float literal: 3.14, 1e10
	Float {
		/// Raw text of the float
		value: &'bump str,
		span: Span,
	},
	/// String literal: 'hello', "world"
	String {
		/// String content (without quotes)
		value: &'bump str,
		span: Span,
	},
	/// Boolean: true, false
	Bool {
		value: bool,
		span: Span,
	},
	/// Null/undefined
	Undefined {
		span: Span,
	},
	/// Temporal: @2024-01-15, @10:30:00
	Temporal {
		/// Raw text of the temporal value
		value: &'bump str,
		span: Span,
	},
}

impl<'bump> Literal<'bump> {
	/// Get the span of this literal.
	pub fn span(&self) -> Span {
		match self {
			Literal::Integer {
				span,
				..
			} => *span,
			Literal::Float {
				span,
				..
			} => *span,
			Literal::String {
				span,
				..
			} => *span,
			Literal::Bool {
				span,
				..
			} => *span,
			Literal::Undefined {
				span,
			} => *span,
			Literal::Temporal {
				span,
				..
			} => *span,
		}
	}

	/// Create an integer literal.
	pub fn integer(value: &'bump str, span: Span) -> Self {
		Literal::Integer {
			value,
			span,
		}
	}

	/// Create a float literal.
	pub fn float(value: &'bump str, span: Span) -> Self {
		Literal::Float {
			value,
			span,
		}
	}

	/// Create a string literal.
	pub fn string(value: &'bump str, span: Span) -> Self {
		Literal::String {
			value,
			span,
		}
	}

	/// Create a boolean literal.
	pub fn bool(value: bool, span: Span) -> Self {
		Literal::Bool {
			value,
			span,
		}
	}

	/// Create an undefined literal.
	pub fn undefined(span: Span) -> Self {
		Literal::Undefined {
			span,
		}
	}

	/// Create a temporal literal.
	pub fn temporal(value: &'bump str, span: Span) -> Self {
		Literal::Temporal {
			value,
			span,
		}
	}
}
