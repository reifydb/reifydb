// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Parse error types.

use std::fmt;

use crate::token::{keyword::Keyword, operator::Operator, punctuation::Punctuation, span::Span};

/// Parse error.
#[derive(Debug, Clone)]
pub struct ParseError {
	pub kind: ParseErrorKind,
	pub span: Span,
}

impl fmt::Display for ParseError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{} at line {}, column {}", self.kind, self.span.line, self.span.column)
	}
}

impl std::error::Error for ParseError {}

/// Parse error kinds.
#[derive(Debug, Clone)]
pub enum ParseErrorKind {
	/// Expected a specific keyword.
	ExpectedKeyword(Keyword),
	/// Expected a specific operator.
	ExpectedOperator(Operator),
	/// Expected a specific punctuation.
	ExpectedPunctuation(Punctuation),
	/// Expected an identifier.
	ExpectedIdentifier,
	/// Expected a variable.
	ExpectedVariable,
	/// Expected an expression.
	ExpectedExpression,
	/// Unexpected token.
	UnexpectedToken,
	/// Feature not implemented.
	NotImplemented(&'static str),
	/// Custom error message.
	Custom(String),
}

impl fmt::Display for ParseErrorKind {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ParseErrorKind::ExpectedKeyword(kw) => write!(f, "expected keyword '{:?}'", kw),
			ParseErrorKind::ExpectedOperator(op) => write!(f, "expected operator '{:?}'", op),
			ParseErrorKind::ExpectedPunctuation(p) => write!(f, "expected punctuation '{:?}'", p),
			ParseErrorKind::ExpectedIdentifier => write!(f, "expected identifier"),
			ParseErrorKind::ExpectedVariable => write!(f, "expected variable"),
			ParseErrorKind::ExpectedExpression => write!(f, "expected expression"),
			ParseErrorKind::UnexpectedToken => write!(f, "unexpected token"),
			ParseErrorKind::NotImplemented(what) => write!(f, "{} parsing not yet implemented", what),
			ParseErrorKind::Custom(msg) => write!(f, "{}", msg),
		}
	}
}
