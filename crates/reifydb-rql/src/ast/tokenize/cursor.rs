// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use OwnedFragment::Statement;
use reifydb_core::{OwnedFragment, StatementColumn, StatementLine};

/// A cursor over the input string that tracks position for tokenization
pub struct Cursor<'a> {
	input: &'a str,
	pos: usize,
	line: u32,
	column: u32,
	line_start: usize,
}

impl<'a> Cursor<'a> {
	/// Create a new cursor at the beginning of the input
	pub fn new(input: &'a str) -> Self {
		Self {
			input,
			pos: 0,
			line: 1,
			column: 1,
			line_start: 0,
		}
	}

	/// Check if we've reached the end of input
	pub fn is_eof(&self) -> bool {
		self.pos >= self.input.len()
	}

	/// Peek at the current character without consuming
	pub fn peek(&self) -> Option<char> {
		self.input[self.pos..].chars().next()
	}

	/// Peek at the next n bytes without consuming
	/// Note: This peeks at BYTES, not characters. Be careful with UTF-8!
	pub fn peek_str(&self, n: usize) -> &str {
		// Find a safe end position that doesn't split UTF-8 characters
		let mut end = (self.pos + n).min(self.input.len());

		// If we're not at the end of the string and not at a char
		// boundary, back up to the previous char boundary
		while end > self.pos
			&& end < self.input.len()
			&& !self.input.is_char_boundary(end)
		{
			end -= 1;
		}

		&self.input[self.pos..end]
	}

	/// Peek ahead n characters and return the character
	pub fn peek_ahead(&self, n: usize) -> Option<char> {
		self.input[self.pos..].chars().nth(n)
	}

	/// Consume and return the current character
	pub fn consume(&mut self) -> Option<char> {
		if let Some(ch) = self.peek() {
			self.pos += ch.len_utf8();
			if ch == '\n' {
				self.line += 1;
				self.column = 1;
				self.line_start = self.pos;
			} else {
				self.column += 1;
			}
			Some(ch)
		} else {
			None
		}
	}

	/// Consume characters while the predicate is true
	pub fn consume_while<F>(&mut self, mut predicate: F) -> &'a str
	where
		F: FnMut(char) -> bool,
	{
		let start = self.pos;
		while let Some(ch) = self.peek() {
			if !predicate(ch) {
				break;
			}
			self.consume();
		}
		&self.input[start..self.pos]
	}

	/// Consume a specific string if it matches at the current position
	pub fn consume_str(&mut self, s: &str) -> bool {
		if self.peek_str(s.len()) == s {
			for _ in 0..s.len() {
				self.consume();
			}
			true
		} else {
			false
		}
	}

	/// Consume a string case-insensitively
	pub fn consume_str_ignore_case(&mut self, s: &str) -> bool {
		let peek = self.peek_str(s.len());
		if peek.eq_ignore_ascii_case(s) {
			for _ in 0..peek.len() {
				self.consume();
			}
			true
		} else {
			false
		}
	}

	/// Skip whitespace characters
	pub fn skip_whitespace(&mut self) {
		self.consume_while(|ch| ch.is_whitespace());
	}

	/// Get the current position in the input
	pub fn pos(&self) -> usize {
		self.pos
	}

	/// Get the current line number
	pub fn line(&self) -> u32 {
		self.line
	}

	/// Get the current column number
	pub fn column(&self) -> u32 {
		self.column
	}

	/// Get remaining input from current position
	pub fn remaining(&self) -> &'a str {
		&self.input[self.pos..]
	}

	/// Get a slice of the input from a starting position to current
	pub fn slice_from(&self, start: usize) -> &'a str {
		&self.input[start..self.pos]
	}

	/// Create an OwnedFragment from a start position to current position
	pub fn make_fragment(
		&self,
		start_pos: usize,
		start_line: u32,
		start_column: u32,
	) -> OwnedFragment {
		Statement {
			text: self.input[start_pos..self.pos].to_string(),
			line: StatementLine(start_line),
			column: StatementColumn(start_column),
		}
	}

	/// Save current position state
	pub fn save_state(&self) -> CursorState {
		CursorState {
			pos: self.pos,
			line: self.line,
			column: self.column,
			line_start: self.line_start,
		}
	}

	/// Restore a previously saved position state
	pub fn restore_state(&mut self, state: CursorState) {
		self.pos = state.pos;
		self.line = state.line;
		self.column = state.column;
		self.line_start = state.line_start;
	}
}

/// Saved cursor state for backtracking
#[derive(Clone, Copy)]
pub struct CursorState {
	pos: usize,
	line: u32,
	column: u32,
	line_start: usize,
}
