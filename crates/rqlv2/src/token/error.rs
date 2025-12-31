// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Lexer error types.

use core::fmt;

use super::span::Span;

/// Lexer error types with source location.
#[derive(Debug, Clone)]
pub enum LexError {
	/// Unexpected character encountered.
	UnexpectedChar {
		ch: char,
		line: u32,
		column: u32,
		span: Span,
	},

	/// Unterminated string literal.
	UnterminatedString {
		line: u32,
		column: u32,
		span: Span,
	},

	/// Unterminated quoted identifier.
	UnterminatedQuotedIdentifier {
		line: u32,
		column: u32,
		span: Span,
	},

	/// Invalid number literal.
	InvalidNumber {
		text: String,
		line: u32,
		column: u32,
		span: Span,
	},

	/// Empty variable name.
	EmptyVariable {
		line: u32,
		column: u32,
		span: Span,
	},
}

impl LexError {
	/// Get the span associated with this error.
	pub fn span(&self) -> Span {
		match self {
			LexError::UnexpectedChar {
				span,
				..
			} => *span,
			LexError::UnterminatedString {
				span,
				..
			} => *span,
			LexError::UnterminatedQuotedIdentifier {
				span,
				..
			} => *span,
			LexError::InvalidNumber {
				span,
				..
			} => *span,
			LexError::EmptyVariable {
				span,
				..
			} => *span,
		}
	}

	/// Get the line number.
	pub fn line(&self) -> u32 {
		match self {
			LexError::UnexpectedChar {
				line,
				..
			} => *line,
			LexError::UnterminatedString {
				line,
				..
			} => *line,
			LexError::UnterminatedQuotedIdentifier {
				line,
				..
			} => *line,
			LexError::InvalidNumber {
				line,
				..
			} => *line,
			LexError::EmptyVariable {
				line,
				..
			} => *line,
		}
	}

	/// Get the column number.
	pub fn column(&self) -> u32 {
		match self {
			LexError::UnexpectedChar {
				column,
				..
			} => *column,
			LexError::UnterminatedString {
				column,
				..
			} => *column,
			LexError::UnterminatedQuotedIdentifier {
				column,
				..
			} => *column,
			LexError::InvalidNumber {
				column,
				..
			} => *column,
			LexError::EmptyVariable {
				column,
				..
			} => *column,
		}
	}
}

impl fmt::Display for LexError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			LexError::UnexpectedChar {
				ch,
				line,
				column,
				..
			} => {
				write!(f, "unexpected character '{}' at {}:{}", ch, line, column)
			}
			LexError::UnterminatedString {
				line,
				column,
				..
			} => {
				write!(f, "unterminated string literal starting at {}:{}", line, column)
			}
			LexError::UnterminatedQuotedIdentifier {
				line,
				column,
				..
			} => {
				write!(f, "unterminated quoted identifier starting at {}:{}", line, column)
			}
			LexError::InvalidNumber {
				text,
				line,
				column,
				..
			} => {
				write!(f, "invalid number literal '{}' at {}:{}", text, line, column)
			}
			LexError::EmptyVariable {
				line,
				column,
				..
			} => {
				write!(f, "empty variable name at {}:{}", line, column)
			}
		}
	}
}

impl std::error::Error for LexError {}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_error_display() {
		let err = LexError::UnexpectedChar {
			ch: '@',
			line: 1,
			column: 5,
			span: Span::new(4, 5, 1, 5),
		};
		assert_eq!(err.to_string(), "unexpected character '@' at 1:5");
	}

	#[test]
	fn test_error_span() {
		let span = Span::new(10, 15, 2, 3);
		let err = LexError::UnterminatedString {
			line: 2,
			column: 3,
			span,
		};
		assert_eq!(err.span(), span);
		assert_eq!(err.line(), 2);
		assert_eq!(err.column(), 3);
	}
}
