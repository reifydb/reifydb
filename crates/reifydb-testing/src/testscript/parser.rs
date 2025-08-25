// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{collections::HashSet, fmt};

use crate::testscript::command::{Argument, Block, Command};

#[derive(Debug, Clone)]
pub struct ParseError {
	pub message: String,
	pub line: u32,
	pub column: usize,
	pub input: LocatedSpan,
	pub code: String,
}

impl fmt::Display for ParseError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"Parse error at line {}:{}: {}",
			self.line, self.column, self.message
		)
	}
}

impl std::error::Error for ParseError {}

#[derive(Debug, Clone)]
pub struct LocatedSpan {
	column: usize,
	line: u32,
	line_text: String,
}

impl LocatedSpan {
	fn new(
		_line_start: usize,
		column: usize,
		line: u32,
		line_text: String,
	) -> Self {
		LocatedSpan {
			column,
			line,
			line_text,
		}
	}

	pub fn location_line(&self) -> u32 {
		self.line
	}

	pub fn get_column(&self) -> usize {
		self.column
	}

	pub fn get_utf8_column(&self) -> usize {
		self.column
	}

	pub fn get_line_beginning(&self) -> &[u8] {
		self.line_text.as_bytes()
	}
}

pub(crate) fn parse(input: &str) -> Result<Vec<Block>, ParseError> {
	let mut parser = Parser::new(input);
	parser.parse_blocks()
}

#[cfg(test)]
pub(crate) fn parse_command(input: &str) -> Result<Command, ParseError> {
	let mut parser = Parser::new(input);
	parser.parse_command()
}

struct Parser<'a> {
	input: &'a str,
	pos: usize,
	line: u32,
	column: usize,
	line_start_pos: usize,
}

impl<'a> Parser<'a> {
	fn new(input: &'a str) -> Self {
		Parser {
			input,
			pos: 0,
			line: 1,
			column: 1,
			line_start_pos: 0,
		}
	}

	fn current_char(&self) -> Option<char> {
		self.input[self.pos..].chars().next()
	}

	fn peek_char(&self) -> Option<char> {
		self.current_char()
	}

	fn peek_str(&self, n: usize) -> &str {
		// This function returns n bytes from current position
		// It's only used for checking ASCII patterns like "---" and
		// "//"
		let end = (self.pos + n).min(self.input.len());

		// Make sure we don't split a UTF-8 character
		let mut safe_end = end;
		while safe_end > self.pos
			&& !self.input.is_char_boundary(safe_end)
		{
			safe_end -= 1;
		}

		&self.input[self.pos..safe_end]
	}

	fn advance(&mut self) -> Option<char> {
		if let Some(ch) = self.current_char() {
			self.pos += ch.len_utf8();
			if ch == '\n' {
				self.line += 1;
				self.column = 1;
				self.line_start_pos = self.pos;
			} else {
				self.column += 1;
			}
			Some(ch)
		} else {
			None
		}
	}

	fn skip_whitespace(&mut self) {
		while let Some(ch) = self.peek_char() {
			if ch.is_whitespace() && ch != '\n' {
				self.advance();
			} else {
				break;
			}
		}
	}

	fn skip_line(&mut self) {
		while let Some(ch) = self.peek_char() {
			if ch == '\n' {
				self.advance();
				break;
			}
			self.advance();
		}
	}

	fn is_at_end(&self) -> bool {
		self.pos >= self.input.len()
	}

	fn error(&self, message: impl Into<String>) -> ParseError {
		let line_end = self.input[self.line_start_pos..]
			.find('\n')
			.map(|i| self.line_start_pos + i)
			.unwrap_or(self.input.len());
		let line_text = &self.input[self.line_start_pos..line_end];

		ParseError {
			message: message.into(),
			line: self.line,
			column: self.column,
			input: LocatedSpan::new(
				self.line_start_pos,
				self.column,
				self.line,
				line_text.to_string(),
			),
			code: format!("{:?}", line_text),
		}
	}

	fn parse_blocks(&mut self) -> Result<Vec<Block>, ParseError> {
		let mut blocks = Vec::new();

		while !self.is_at_end() {
			if let Some(block) = self.parse_block()? {
				blocks.push(block);
			}
		}

		Ok(blocks)
	}

	fn parse_block(&mut self) -> Result<Option<Block>, ParseError> {
		let line_number = self.line;
		let literal_start = self.pos;

		// Parse commands
		let commands = self.parse_commands()?;

		// Capture literal
		let literal_end = self.pos;
		let literal =
			self.input[literal_start..literal_end].to_string();

		// Handle empty block at EOF
		if self.is_at_end() && commands.is_empty() {
			return Ok(Some(Block {
				literal,
				commands,
				line_number,
			}));
		}

		// If no commands and not at EOF, this isn't a valid block
		if commands.is_empty() {
			return Ok(None);
		}

		// Parse separator
		if !self.parse_separator()? {
			return Err(self.error("Expected --- separator"));
		}

		// Parse and skip output
		self.parse_output()?;

		Ok(Some(Block {
			literal,
			commands,
			line_number,
		}))
	}

	fn parse_commands(&mut self) -> Result<Vec<Command>, ParseError> {
		let mut commands = Vec::new();

		loop {
			// Skip empty and comment lines
			if self.skip_empty_or_comment_line() {
				continue;
			}

			// Check for EOF
			if self.is_at_end() {
				break;
			}

			// Check for separator
			if self.peek_str(3) == "---" {
				if !commands.is_empty() {
					break;
				}
			}

			// Check for leading whitespace (not allowed for
			// commands)
			if let Some(ch) = self.peek_char() {
				if ch.is_whitespace() && ch != '\n' {
					return Err(self.error(
						"Command cannot start with whitespace",
					));
				}
			}

			// Parse command
			match self.parse_command() {
				Ok(cmd) => commands.push(cmd),
				Err(e) => {
					// If we hit a separator but have no
					// commands, let parse_command error
					if self.peek_str(3) == "---"
						&& commands.is_empty()
					{
						return Err(e);
					}
					return Err(e);
				}
			}
		}

		Ok(commands)
	}

	fn parse_command(&mut self) -> Result<Command, ParseError> {
		let line_number = self.line;

		// Check for silencing (
		let silent = if self.peek_char() == Some('(') {
			self.advance();
			self.skip_whitespace();
			true
		} else {
			false
		};

		// Parse prefix and tags
		let mut tags = HashSet::new();
		let mut prefix = None;

		// Try to parse prefix (string followed by :)
		let saved_pos = self.pos;
		self.skip_whitespace();
		if let Ok(s) = self.parse_string() {
			self.skip_whitespace();
			if self.peek_char() == Some(':') {
				self.advance();
				self.skip_whitespace();
				prefix = Some(s);
			} else {
				// Backtrack
				self.pos = saved_pos;
			}
		}

		// Parse tags before command
		self.skip_whitespace();
		if let Some(parsed_tags) = self.parse_taglist()? {
			tags.extend(parsed_tags);
		}
		self.skip_whitespace();

		// Check for fail marker
		let fail = if self.peek_char() == Some('!') {
			self.advance();
			self.skip_whitespace();
			true
		} else {
			false
		};

		// Check for literal command (>)
		if self.peek_char() == Some('>') {
			self.advance();
			self.skip_whitespace();
			let name = self.parse_line_continuation()?;
			return Ok(Command {
				name,
				args: Vec::new(),
				tags,
				prefix,
				silent,
				fail,
				line_number,
			});
		}

		// Parse command name
		self.skip_whitespace();
		let name = self
			.parse_string()
			.map_err(|_| self.error("Expected command name"))?;

		// Parse arguments
		let mut args = Vec::new();
		loop {
			self.skip_whitespace();
			if self.peek_char() == Some('[') {
				// Might be trailing tags
				if let Some(parsed_tags) =
					self.parse_taglist()?
				{
					tags.extend(parsed_tags);
					break;
				}
			}

			// Check for end of command
			if silent && self.peek_char() == Some(')') {
				break;
			}

			if self.peek_char() == Some('#')
				|| self.peek_str(2) == "//"
			{
				break;
			}

			if self.peek_char() == Some('\n') || self.is_at_end() {
				break;
			}

			// Try to parse an argument
			let saved_pos = self.pos;
			let saved_line = self.line;
			let saved_column = self.column;
			let saved_line_start = self.line_start_pos;
			match self.parse_argument() {
				Ok(arg) => args.push(arg),
				Err(_) => {
					self.pos = saved_pos;
					self.line = saved_line;
					self.column = saved_column;
					self.line_start_pos = saved_line_start;
					break;
				}
			}
		}

		// Handle closing ) for silent commands
		if silent {
			self.skip_whitespace();
			if self.peek_char() != Some(')') {
				return Err(self.error(
					"Expected closing ) for silent command",
				));
			}
			self.advance();
		}

		// Skip trailing whitespace and comments
		self.skip_whitespace();
		if self.peek_char() == Some('#') || self.peek_str(2) == "//" {
			self.skip_line();
		} else if self.peek_char() == Some('\n') {
			self.advance();
		} else if !self.is_at_end() {
			return Err(self.error("Expected end of line"));
		}

		Ok(Command {
			name,
			args,
			tags,
			prefix,
			silent,
			fail,
			line_number,
		})
	}

	fn parse_argument(&mut self) -> Result<Argument, ParseError> {
		// Try key=value format first
		let saved_pos = self.pos;
		let saved_line = self.line;
		let saved_column = self.column;
		let saved_line_start = self.line_start_pos;

		self.skip_whitespace();
		if let Ok(key) = self.parse_string() {
			if self.peek_char() == Some('=') {
				self.advance();
				// Allow empty value after =
				self.skip_whitespace();
				let value = match self.parse_string() {
					Ok(v) => v,
					Err(_) => {
						// Check if next char is
						// whitespace or end - means
						// empty value
						match self.peek_char() {
							Some(ch) if ch
								.is_whitespace(
								) =>
							{
								String::new()
							}
							Some('[')
							| Some(')')
							| Some('#') | None => String::new(),
							_ if self.peek_str(
								2,
							) == "//" =>
							{
								String::new()
							}
							_ => {
								self.pos = saved_pos;
								self.line = saved_line;
								self.column = saved_column;
								self.line_start_pos = saved_line_start;
								return Err(self.error("Expected argument value after ="));
							}
						}
					}
				};
				return Ok(Argument {
					key: Some(key),
					value,
				});
			}
			// Just a value
			return Ok(Argument {
				key: None,
				value: key,
			});
		}

		self.pos = saved_pos;
		Err(self.error("Expected argument"))
	}

	fn parse_taglist(
		&mut self,
	) -> Result<Option<HashSet<String>>, ParseError> {
		if self.peek_char() != Some('[') {
			return Ok(None);
		}

		self.advance();
		let mut tags = HashSet::new();

		loop {
			self.skip_whitespace();

			if self.peek_char() == Some(']') {
				// Empty tag list is an error
				if tags.is_empty() {
					return Err(
						self.error("Empty tag list")
					);
				}
				self.advance();
				break;
			}

			self.skip_whitespace();
			let tag = self
				.parse_string()
				.map_err(|_| self.error("Expected tag name"))?;
			tags.insert(tag);

			self.skip_whitespace();
			if self.peek_char() == Some(',') {
				self.advance();
				self.skip_whitespace();
			} else if self.peek_char() == Some(' ') {
				self.skip_whitespace();
			}
		}

		Ok(Some(tags))
	}

	fn parse_string(&mut self) -> Result<String, ParseError> {
		// Note: Don't skip whitespace here - the caller should handle
		// that self.skip_whitespace();

		match self.peek_char() {
			Some('\'') => self.parse_quoted_string('\''),
			Some('"') => self.parse_quoted_string('"'),
			_ => self.parse_unquoted_string(),
		}
	}

	fn parse_unquoted_string(&mut self) -> Result<String, ParseError> {
		let mut result = String::new();

		// First character must be alphanumeric or _
		match self.peek_char() {
			Some(ch) if ch.is_alphanumeric() || ch == '_' => {
				result.push(ch);
				self.advance();
			}
			_ => return Err(self.error("Expected string")),
		}

		// Subsequent characters
		while let Some(ch) = self.peek_char() {
			if ch.is_alphanumeric() || "_-./@".contains(ch) {
				result.push(ch);
				self.advance();
			} else {
				break;
			}
		}

		Ok(result)
	}

	fn parse_quoted_string(
		&mut self,
		quote: char,
	) -> Result<String, ParseError> {
		let mut result = String::new();

		// Skip opening quote
		if self.peek_char() != Some(quote) {
			return Err(
				self.error(format!("Expected {} quote", quote))
			);
		}
		self.advance();

		while let Some(ch) = self.peek_char() {
			if ch == quote {
				self.advance();
				return Ok(result);
			} else if ch == '\\' {
				self.advance();
				match self.peek_char() {
					Some('\'') => {
						result.push('\'');
						self.advance();
					}
					Some('"') => {
						result.push('"');
						self.advance();
					}
					Some('\\') => {
						result.push('\\');
						self.advance();
					}
					Some('0') => {
						result.push('\0');
						self.advance();
					}
					Some('n') => {
						result.push('\n');
						self.advance();
					}
					Some('r') => {
						result.push('\r');
						self.advance();
					}
					Some('t') => {
						result.push('\t');
						self.advance();
					}
					Some('x') => {
						self.advance();
						let hex = self
							.parse_hex_digits(
								2, 2,
							)?;
						let byte = u8::from_str_radix(
							&hex, 16,
						)
						.map_err(|_| {
							self.error(
								"Invalid hex escape",
							)
						})?;
						result.push(char::from(byte));
					}
					Some('u') => {
						self.advance();
						if self.peek_char() != Some('{')
						{
							return Err(self
								.error(
									"Expected { after \\u",
								));
						}
						self.advance();
						let hex = self
							.parse_hex_digits(
								1, 6,
							)?;
						if self.peek_char() != Some('}')
						{
							return Err(self
								.error(
									"Expected } after unicode escape",
								));
						}
						self.advance();
						let codepoint =
							u32::from_str_radix(
								&hex, 16,
							)
							.map_err(
								|_| {
									self.error("Invalid unicode escape")
								},
							)?;
						let ch = char::from_u32(
							codepoint,
						)
						.ok_or_else(|| {
							self.error(
								"Invalid unicode codepoint",
							)
						})?;
						result.push(ch);
					}
					_ => {
						return Err(self.error(
							"Invalid escape sequence",
						));
					}
				}
			} else {
				result.push(ch);
				self.advance();
			}
		}

		Err(self.error(format!(
			"Unterminated string (missing {})",
			quote
		)))
	}

	fn parse_hex_digits(
		&mut self,
		min: usize,
		max: usize,
	) -> Result<String, ParseError> {
		let mut hex = String::new();
		for i in 0..max {
			match self.peek_char() {
				Some(ch) if ch.is_ascii_hexdigit() => {
					hex.push(ch);
					self.advance();
				}
				_ => {
					if i < min {
						return Err(self.error(
							format!(
								"Expected at least {} hex digits",
								min
							),
						));
					}
					break;
				}
			}
		}
		if hex.len() < min {
			return Err(self.error(format!(
				"Expected at least {} hex digits",
				min
			)));
		}
		Ok(hex)
	}

	fn skip_empty_or_comment_line(&mut self) -> bool {
		let saved_pos = self.pos;

		self.skip_whitespace();

		// Check for comment
		if self.peek_char() == Some('#') || self.peek_str(2) == "//" {
			self.skip_line();
			return true;
		}

		// Check for empty line
		if self.peek_char() == Some('\n') {
			self.advance();
			return true;
		}

		// Not an empty or comment line, restore position
		self.pos = saved_pos;
		false
	}

	fn parse_separator(&mut self) -> Result<bool, ParseError> {
		if self.peek_str(3) != "---" {
			return Ok(false);
		}

		self.advance(); // -
		self.advance(); // -
		self.advance(); // -

		// Must be followed by newline (with optional \r) or EOF
		match self.peek_char() {
			Some('\r') => {
				self.advance();
				if self.peek_char() == Some('\n') {
					self.advance();
				}
				Ok(true)
			}
			Some('\n') => {
				self.advance();
				Ok(true)
			}
			None => Ok(true),
			_ => Err(self.error(
				"Separator must be followed by newline or EOF",
			)),
		}
	}

	fn parse_output(&mut self) -> Result<(), ParseError> {
		// Special case: no output (immediate newline or EOF)
		if self.peek_char() == Some('\n') || self.is_at_end() {
			if self.peek_char() == Some('\n') {
				self.advance();
			}
			return Ok(());
		}

		// Read until double newline or EOF
		let mut last_was_newline = false;
		while !self.is_at_end() {
			let ch = self.advance().unwrap();
			if ch == '\n' {
				if last_was_newline {
					break;
				}
				last_was_newline = true;
			} else {
				last_was_newline = false;
			}
		}

		Ok(())
	}

	fn parse_line_continuation(&mut self) -> Result<String, ParseError> {
		let mut result = String::new();

		loop {
			// Read until end of line
			while let Some(ch) = self.peek_char() {
				if ch == '\n' {
					break;
				}
				result.push(ch);
				self.advance();
			}

			// Check for continuation
			if result.ends_with('\\') {
				result.pop(); // Remove the backslash
				if self.peek_char() == Some('\n') {
					self.advance(); // Skip newline
					continue;
				}
			}

			// Skip the final newline
			if self.peek_char() == Some('\n') {
				self.advance();
			}

			break;
		}

		Ok(result)
	}
}
