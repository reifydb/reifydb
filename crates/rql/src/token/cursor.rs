// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::fragment::{StatementColumn, StatementLine};

use crate::bump::BumpFragment;

pub struct Cursor<'bump> {
	input: &'bump str,
	pos: usize,
	line: u32,
	column: u32,
	line_start: usize,

	current_char: Option<char>,
	current_char_len: usize,
}

impl<'bump> Cursor<'bump> {
	pub fn new(input: &'bump str) -> Self {
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

	pub fn is_eof(&self) -> bool {
		self.current_char.is_none()
	}

	pub fn peek(&self) -> Option<char> {
		self.current_char
	}

	pub fn peek_str(&self, n: usize) -> &str {
		let mut end = (self.pos + n).min(self.input.len());

		while end > self.pos && end < self.input.len() && !self.input.is_char_boundary(end) {
			end -= 1;
		}

		&self.input[self.pos..end]
	}

	pub fn peek_ahead(&self, n: usize) -> Option<char> {
		self.input[self.pos..].chars().nth(n)
	}

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

	pub fn consume_while<F>(&mut self, mut predicate: F) -> &'bump str
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

	fn is_system_column_ahead(&self) -> bool {
		const SYSTEM_COLUMNS: &[&str] = &["rownum", "created_at", "updated_at"];
		let remaining = &self.input[self.pos..];
		for col in SYSTEM_COLUMNS {
			let prefixed = format!("#{col}");
			if remaining.starts_with(&prefixed) {
				let next_char = remaining[prefixed.len()..].chars().next();
				if next_char.is_none()
					|| !(next_char.unwrap().is_alphanumeric() || next_char.unwrap() == '_')
				{
					return true;
				}
			}
		}
		false
	}

	pub fn skip_whitespace(&mut self) {
		while let Some(ch) = self.current_char {
			match ch {
				' ' | '\t' | '\r' | '\n' => {
					self.consume();
				}
				'#' => {
					if self.is_system_column_ahead() {
						break;
					}

					self.consume();
					while let Some(ch) = self.current_char {
						if ch == '\n' {
							self.consume();
							break;
						}
						self.consume();
					}
				}
				_ => {
					if ch.is_whitespace() {
						self.consume();
					} else {
						break;
					}
				}
			}
		}
	}

	pub fn pos(&self) -> usize {
		self.pos
	}

	pub fn line(&self) -> u32 {
		self.line
	}

	pub fn column(&self) -> u32 {
		self.column
	}

	pub fn make_fragment(&self, start_pos: usize, start_line: u32, start_column: u32) -> BumpFragment<'bump> {
		BumpFragment::Statement {
			text: &self.input[start_pos..self.pos],
			offset: start_pos,
			source_end: self.pos,
			line: StatementLine(start_line),
			column: StatementColumn(start_column),
		}
	}

	pub fn make_utf8_fragment(
		&self,
		text_start: usize,
		text_end: usize,
		start_line: u32,
		start_column: u32,
		token_offset: usize,
	) -> BumpFragment<'bump> {
		BumpFragment::Statement {
			text: &self.input[text_start..text_end],
			offset: token_offset,
			source_end: self.pos,
			line: StatementLine(start_line),
			column: StatementColumn(start_column),
		}
	}

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

	pub fn restore_state(&mut self, state: CursorState) {
		self.pos = state.pos;
		self.line = state.line;
		self.column = state.column;
		self.line_start = state.line_start;
		self.current_char = state.current_char;
		self.current_char_len = state.current_char_len;
	}

	pub fn remaining_input(&self) -> &'bump str {
		&self.input[self.pos..]
	}
}

#[derive(Clone, Copy)]
pub struct CursorState {
	pos: usize,
	line: u32,
	column: u32,
	line_start: usize,
	current_char: Option<char>,
	current_char_len: usize,
}
