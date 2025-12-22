// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_type::{Fragment, StatementColumn, StatementLine};

/// A cursor over the input string that tracks position for tokenization
pub struct Cursor<'a> {
	input: &'a str,
	pos: usize,
	line: u32,
	column: u32,
	line_start: usize,
	// Cache current character to avoid repeated UTF-8 validation
	current_char: Option<char>,
	current_char_len: usize,
}

impl<'a> Cursor<'a> {
	/// Create a new cursor at the beginning of the input
	pub fn new(input: &'a str) -> Self {
		let (current_char, current_char_len) = if input.is_empty() {
			(None, 0)
		} else {
			let ch = input.chars().next().unwrap();
			(Some(ch), ch.len_utf8())
		};

		Self {
			input,
			pos: 0,
			line: 1,
			column: 1,
			line_start: 0,
			current_char,
			current_char_len,
		}
	}

	/// Check if we've reached the end of input
	pub fn is_eof(&self) -> bool {
		self.current_char.is_none()
	}

	/// Peek at the current character without consuming
	pub fn peek(&self) -> Option<char> {
		self.current_char
	}

	/// Peek at the next n bytes without consuming
	/// Note: This peeks at BYTES, not characters. Be careful with UTF-8!
	pub fn peek_str(&self, n: usize) -> &str {
		// Find a safe end position that doesn't split UTF-8 characters
		let mut end = (self.pos + n).min(self.input.len());

		// If we're not at the end of the string and not at a char
		// boundary, back up to the previous char boundary
		while end > self.pos && end < self.input.len() && !self.input.is_char_boundary(end) {
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
		if let Some(ch) = self.current_char {
			self.pos += self.current_char_len;
			if ch == '\n' {
				self.line += 1;
				self.column = 1;
				self.line_start = self.pos;
			} else {
				self.column += 1;
			}

			// Update cached character
			if self.pos < self.input.len() {
				let remaining = &self.input[self.pos..];
				let next_char = remaining.chars().next().unwrap();
				self.current_char = Some(next_char);
				self.current_char_len = next_char.len_utf8();
			} else {
				self.current_char = None;
				self.current_char_len = 0;
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

	/// Skip whitespace characters - optimized for common ASCII cases
	pub fn skip_whitespace(&mut self) {
		// Fast path for ASCII whitespace (most common case)
		while let Some(ch) = self.current_char {
			match ch {
				' ' | '\t' | '\r' | '\n' => {
					self.consume();
				}
				_ => {
					// Fall back to full Unicode whitespace
					// check for non-ASCII
					if ch.is_whitespace() {
						self.consume();
					} else {
						break;
					}
				}
			}
		}
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

	/// Create a Fragment (owned) from a start position to current
	/// position
	pub fn make_fragment(&self, start_pos: usize, start_line: u32, start_column: u32) -> Fragment {
		Fragment::Statement {
			text: Arc::from(self.input[start_pos..self.pos].to_string()),
			line: StatementLine(start_line),
			column: StatementColumn(start_column),
		}
	}

	/// Create a fragment for UTF-8 text content (without surrounding
	/// quotes)
	pub fn make_utf8_fragment(
		&self,
		text_start: usize,
		text_end: usize,
		start_line: u32,
		start_column: u32,
	) -> Fragment {
		Fragment::Statement {
			text: Arc::from(self.input[text_start..text_end].to_string()),
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
			current_char: self.current_char,
			current_char_len: self.current_char_len,
		}
	}

	/// Restore a previously saved position state
	pub fn restore_state(&mut self, state: CursorState) {
		self.pos = state.pos;
		self.line = state.line;
		self.column = state.column;
		self.line_start = state.line_start;
		self.current_char = state.current_char;
		self.current_char_len = state.current_char_len;
	}

	/// Get a slice of the remaining input from current position
	pub fn remaining_input(&self) -> &'a str {
		&self.input[self.pos..]
	}
}

/// Saved cursor state for backtracking
#[derive(Clone, Copy)]
pub struct CursorState {
	pos: usize,
	line: u32,
	column: u32,
	line_start: usize,
	current_char: Option<char>,
	current_char_len: usize,
}
