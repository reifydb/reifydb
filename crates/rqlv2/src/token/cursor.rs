// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Cursor for input traversal during tokenization.

use std::{iter::Peekable, str::CharIndices};

use super::span::Span;

/// Cursor over input for tokenization.
pub struct Cursor<'a> {
	source: &'a str,
	chars: Peekable<CharIndices<'a>>,
	/// Current byte position.
	position: usize,
	/// Current line (1-indexed).
	line: u32,
	/// Current column (1-indexed, bytes from line start).
	column: u32,
}

impl<'a> Cursor<'a> {
	/// Create a new cursor for the given source.
	pub fn new(source: &'a str) -> Self {
		Self {
			source,
			chars: source.char_indices().peekable(),
			position: 0,
			line: 1,
			column: 1,
		}
	}

	/// Get the source being tokenized.
	#[inline]
	pub fn source(&self) -> &'a str {
		self.source
	}

	/// Current byte position.
	#[inline]
	pub fn position(&self) -> usize {
		self.position
	}

	/// Current line number (1-indexed).
	#[inline]
	pub fn line(&self) -> u32 {
		self.line
	}

	/// Current column number (1-indexed).
	#[inline]
	pub fn column(&self) -> u32 {
		self.column
	}

	/// Check if at end of input.
	#[inline]
	pub fn is_eof(&self) -> bool {
		self.position >= self.source.len()
	}

	/// Peek at current character without consuming.
	#[inline]
	pub fn peek(&mut self) -> Option<char> {
		self.chars.peek().map(|&(_, ch)| ch)
	}

	/// Peek at character n positions ahead (0 = current).
	pub fn peek_ahead(&self, n: usize) -> Option<char> {
		self.source[self.position..].chars().nth(n)
	}

	/// Peek at next n bytes as a string slice.
	pub fn peek_str(&self, n: usize) -> &'a str {
		let end = (self.position + n).min(self.source.len());
		// Ensure we don't split UTF-8
		let mut end = end;
		while end > self.position && end < self.source.len() && !self.source.is_char_boundary(end) {
			end -= 1;
		}
		&self.source[self.position..end]
	}

	/// Consume and return current character.
	pub fn advance(&mut self) -> Option<char> {
		if let Some((pos, ch)) = self.chars.next() {
			self.position = pos + ch.len_utf8();
			if ch == '\n' {
				self.line += 1;
				self.column = 1;
			} else {
				self.column += 1;
			}
			Some(ch)
		} else {
			None
		}
	}

	/// Consume characters while predicate is true, return consumed slice.
	pub fn advance_while<F: Fn(char) -> bool>(&mut self, predicate: F) -> &'a str {
		let start = self.position;
		while let Some(&(_, ch)) = self.chars.peek() {
			if !predicate(ch) {
				break;
			}
			self.advance();
		}
		&self.source[start..self.position]
	}

	/// Try to consume a specific string, returns true if successful.
	pub fn try_consume(&mut self, s: &str) -> bool {
		if self.peek_str(s.len()) == s {
			for _ in s.chars() {
				self.advance();
			}
			true
		} else {
			false
		}
	}

	/// Create a span from start position to current position.
	#[inline]
	pub fn span_from(&self, start: usize, start_line: u32, start_column: u32) -> Span {
		Span::new(start as u32, self.position as u32, start_line, start_column)
	}

	/// Skip whitespace and comments.
	pub fn skip_whitespace_and_comments(&mut self) {
		loop {
			match self.peek() {
				Some(' ') | Some('\t') | Some('\r') | Some('\n') => {
					self.advance();
				}
				Some('#') => {
					// Line comment - skip until end of line
					self.advance();
					while let Some(ch) = self.peek() {
						if ch == '\n' {
							break;
						}
						self.advance();
					}
				}
				_ => break,
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_cursor_new() {
		let cursor = Cursor::new("hello");
		assert_eq!(cursor.position(), 0);
		assert_eq!(cursor.line(), 1);
		assert_eq!(cursor.column(), 1);
		assert!(!cursor.is_eof());
	}

	#[test]
	fn test_cursor_peek() {
		let mut cursor = Cursor::new("abc");
		assert_eq!(cursor.peek(), Some('a'));
		assert_eq!(cursor.peek(), Some('a')); // Peek doesn't consume
	}

	#[test]
	fn test_cursor_advance() {
		let mut cursor = Cursor::new("abc");
		assert_eq!(cursor.advance(), Some('a'));
		assert_eq!(cursor.position(), 1);
		assert_eq!(cursor.column(), 2);
		assert_eq!(cursor.advance(), Some('b'));
		assert_eq!(cursor.advance(), Some('c'));
		assert_eq!(cursor.advance(), None);
		assert!(cursor.is_eof());
	}

	#[test]
	fn test_cursor_newline_tracking() {
		let mut cursor = Cursor::new("a\nb\nc");
		cursor.advance(); // 'a'
		assert_eq!(cursor.line(), 1);
		cursor.advance(); // '\n'
		assert_eq!(cursor.line(), 2);
		assert_eq!(cursor.column(), 1);
		cursor.advance(); // 'b'
		assert_eq!(cursor.column(), 2);
	}

	#[test]
	fn test_cursor_peek_ahead() {
		let cursor = Cursor::new("abcd");
		assert_eq!(cursor.peek_ahead(0), Some('a'));
		assert_eq!(cursor.peek_ahead(1), Some('b'));
		assert_eq!(cursor.peek_ahead(2), Some('c'));
		assert_eq!(cursor.peek_ahead(3), Some('d'));
		assert_eq!(cursor.peek_ahead(4), None);
	}

	#[test]
	fn test_cursor_peek_str() {
		let cursor = Cursor::new("hello world");
		assert_eq!(cursor.peek_str(5), "hello");
		assert_eq!(cursor.peek_str(11), "hello world");
		assert_eq!(cursor.peek_str(100), "hello world");
	}

	#[test]
	fn test_cursor_advance_while() {
		let mut cursor = Cursor::new("abc123def");
		let letters = cursor.advance_while(|c| c.is_ascii_alphabetic());
		assert_eq!(letters, "abc");
		assert_eq!(cursor.position(), 3);
	}

	#[test]
	fn test_cursor_try_consume() {
		let mut cursor = Cursor::new("==");
		assert!(cursor.try_consume("=="));
		assert!(cursor.is_eof());
	}

	#[test]
	fn test_cursor_try_consume_fail() {
		let mut cursor = Cursor::new("=!");
		assert!(!cursor.try_consume("=="));
		assert_eq!(cursor.position(), 0); // Position unchanged
	}

	#[test]
	fn test_cursor_skip_whitespace() {
		let mut cursor = Cursor::new("  \t\n  hello");
		cursor.skip_whitespace_and_comments();
		assert_eq!(cursor.peek(), Some('h'));
	}

	#[test]
	fn test_cursor_skip_comment() {
		let mut cursor = Cursor::new("# comment\nhello");
		cursor.skip_whitespace_and_comments();
		// Comment and following newline are both skipped
		assert_eq!(cursor.peek(), Some('h'));
	}

	#[test]
	fn test_cursor_span_from() {
		let mut cursor = Cursor::new("hello");
		let start = cursor.position();
		let start_line = cursor.line();
		let start_column = cursor.column();
		cursor.advance();
		cursor.advance();
		cursor.advance();
		let span = cursor.span_from(start, start_line, start_column);
		assert_eq!(span.start, 0);
		assert_eq!(span.end, 3);
		assert_eq!(span.line, 1);
		assert_eq!(span.column, 1);
	}
}
