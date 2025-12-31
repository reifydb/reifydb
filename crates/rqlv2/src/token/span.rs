// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Source location types for error reporting and source mapping.

/// Source location for error reporting.
/// Uses byte offsets for efficiency with UTF-8.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct Span {
	/// Byte offset from start of input (inclusive).
	pub start: u32,
	/// Byte offset from start of input (exclusive).
	pub end: u32,
	/// Line number (1-indexed).
	pub line: u32,
	/// Column number (1-indexed, bytes from line start).
	pub column: u32,
}

impl Span {
	/// Create a new span.
	#[inline]
	pub const fn new(start: u32, end: u32, line: u32, column: u32) -> Self {
		Self {
			start,
			end,
			line,
			column,
		}
	}

	/// Returns the byte length of this span.
	#[inline]
	pub const fn len(&self) -> u32 {
		self.end - self.start
	}

	/// Returns true if this span is empty.
	#[inline]
	pub const fn is_empty(&self) -> bool {
		self.start == self.end
	}

	/// Merge two spans into one that covers both.
	pub fn merge(&self, other: &Span) -> Span {
		Span {
			start: self.start.min(other.start),
			end: self.end.max(other.end),
			line: self.line.min(other.line),
			column: if self.line <= other.line {
				self.column
			} else {
				other.column
			},
		}
	}

	/// Extract the text from the original source.
	#[inline]
	pub fn text<'a>(&self, source: &'a str) -> &'a str {
		&source[self.start as usize..self.end as usize]
	}
}

/// A value with source location information.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Spanned<T> {
	pub value: T,
	pub span: Span,
}

impl<T> Spanned<T> {
	/// Create a new spanned value.
	#[inline]
	pub const fn new(value: T, span: Span) -> Self {
		Self {
			value,
			span,
		}
	}

	/// Map the inner value to a new type.
	#[inline]
	pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Spanned<U> {
		Spanned {
			value: f(self.value),
			span: self.span,
		}
	}
}

impl<T> core::ops::Deref for Spanned<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		&self.value
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_span_new() {
		let span = Span::new(0, 10, 1, 1);
		assert_eq!(span.start, 0);
		assert_eq!(span.end, 10);
		assert_eq!(span.line, 1);
		assert_eq!(span.column, 1);
	}

	#[test]
	fn test_span_len() {
		let span = Span::new(5, 15, 1, 6);
		assert_eq!(span.len(), 10);
	}

	#[test]
	fn test_span_is_empty() {
		let empty = Span::new(5, 5, 1, 6);
		let nonempty = Span::new(5, 10, 1, 6);
		assert!(empty.is_empty());
		assert!(!nonempty.is_empty());
	}

	#[test]
	fn test_span_merge() {
		let a = Span::new(0, 5, 1, 1);
		let b = Span::new(10, 20, 2, 5);
		let merged = a.merge(&b);
		assert_eq!(merged.start, 0);
		assert_eq!(merged.end, 20);
		assert_eq!(merged.line, 1);
		assert_eq!(merged.column, 1);
	}

	#[test]
	fn test_span_text() {
		let source = "hello world";
		let span = Span::new(0, 5, 1, 1);
		assert_eq!(span.text(source), "hello");
	}

	#[test]
	fn test_spanned() {
		let span = Span::new(0, 5, 1, 1);
		let spanned = Spanned::new(42, span);
		assert_eq!(*spanned, 42);
		assert_eq!(spanned.span, span);
	}

	#[test]
	fn test_spanned_map() {
		let span = Span::new(0, 5, 1, 1);
		let spanned = Spanned::new(42, span);
		let mapped = spanned.map(|x| x * 2);
		assert_eq!(*mapped, 84);
		assert_eq!(mapped.span, span);
	}
}
