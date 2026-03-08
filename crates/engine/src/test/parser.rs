// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt;

#[derive(Debug)]
pub struct TestCase {
	pub name: String,
	pub body: String,
	pub line: usize,
}

#[derive(Debug)]
pub struct ParseError {
	pub message: String,
	pub line: usize,
}

impl fmt::Display for ParseError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "line {}: {}", self.line, self.message)
	}
}

impl std::error::Error for ParseError {}

pub fn parse(input: &str) -> Result<Vec<TestCase>, ParseError> {
	let mut cases = Vec::new();
	let chars: Vec<char> = input.chars().collect();
	let len = chars.len();
	let mut pos = 0;
	let mut line = 1;

	while pos < len {
		// Skip whitespace and comments
		skip_whitespace_and_comments(&chars, &mut pos, &mut line);
		if pos >= len {
			break;
		}

		let test_line = line;

		// Expect "test" keyword
		if !starts_with_keyword(&chars, pos, "test") {
			return Err(ParseError {
				message: format!("expected 'test' keyword, found '{}'", peek_word(&chars, pos)),
				line,
			});
		}
		pos += 4;

		// Skip whitespace
		skip_whitespace(&chars, &mut pos, &mut line);

		// Expect '('
		if pos >= len || chars[pos] != '(' {
			return Err(ParseError {
				message: "expected '(' after 'test'".to_string(),
				line,
			});
		}
		pos += 1;

		// Skip whitespace
		skip_whitespace(&chars, &mut pos, &mut line);

		// Parse quoted name
		if pos >= len || chars[pos] != '"' {
			return Err(ParseError {
				message: "expected '\"' for test name".to_string(),
				line,
			});
		}
		let name = parse_quoted_string(&chars, &mut pos, &mut line)?;

		// Skip whitespace
		skip_whitespace(&chars, &mut pos, &mut line);

		// Expect ')'
		if pos >= len || chars[pos] != ')' {
			return Err(ParseError {
				message: "expected ')' after test name".to_string(),
				line,
			});
		}
		pos += 1;

		// Skip whitespace
		skip_whitespace(&chars, &mut pos, &mut line);

		// Expect '{' and extract body
		if pos >= len || chars[pos] != '{' {
			return Err(ParseError {
				message: "expected '{' to start test body".to_string(),
				line,
			});
		}
		pos += 1; // consume opening brace
		let body = extract_body(&chars, &mut pos, &mut line)?;

		cases.push(TestCase {
			name,
			body,
			line: test_line,
		});
	}

	Ok(cases)
}

fn skip_whitespace(chars: &[char], pos: &mut usize, line: &mut usize) {
	while *pos < chars.len() && chars[*pos].is_whitespace() {
		if chars[*pos] == '\n' {
			*line += 1;
		}
		*pos += 1;
	}
}

fn skip_whitespace_and_comments(chars: &[char], pos: &mut usize, line: &mut usize) {
	loop {
		skip_whitespace(chars, pos, line);
		if *pos + 1 < chars.len() && chars[*pos] == '/' && chars[*pos + 1] == '/' {
			// Skip to end of line
			while *pos < chars.len() && chars[*pos] != '\n' {
				*pos += 1;
			}
		} else {
			break;
		}
	}
}

fn starts_with_keyword(chars: &[char], pos: usize, keyword: &str) -> bool {
	let kw: Vec<char> = keyword.chars().collect();
	if pos + kw.len() > chars.len() {
		return false;
	}
	for (i, c) in kw.iter().enumerate() {
		if chars[pos + i] != *c {
			return false;
		}
	}
	// Ensure it's not part of a longer identifier
	let after = pos + kw.len();
	if after < chars.len() && (chars[after].is_alphanumeric() || chars[after] == '_') {
		return false;
	}
	true
}

fn peek_word(chars: &[char], pos: usize) -> String {
	let mut end = pos;
	while end < chars.len() && (chars[end].is_alphanumeric() || chars[end] == '_') {
		end += 1;
	}
	if end == pos && pos < chars.len() {
		return chars[pos].to_string();
	}
	chars[pos..end].iter().collect()
}

fn parse_quoted_string(chars: &[char], pos: &mut usize, line: &mut usize) -> Result<String, ParseError> {
	// pos is at opening quote
	*pos += 1;
	let mut s = String::new();
	while *pos < chars.len() {
		let c = chars[*pos];
		if c == '\\' && *pos + 1 < chars.len() {
			*pos += 1;
			match chars[*pos] {
				'n' => s.push('\n'),
				't' => s.push('\t'),
				'\\' => s.push('\\'),
				'"' => s.push('"'),
				other => {
					s.push('\\');
					s.push(other);
				}
			}
			*pos += 1;
		} else if c == '"' {
			*pos += 1;
			return Ok(s);
		} else {
			if c == '\n' {
				*line += 1;
			}
			s.push(c);
			*pos += 1;
		}
	}
	Err(ParseError {
		message: "unterminated string literal".to_string(),
		line: *line,
	})
}

/// Extract body after opening '{' has been consumed. Handles nested braces,
/// string literals, and line comments.
fn extract_body(chars: &[char], pos: &mut usize, line: &mut usize) -> Result<String, ParseError> {
	let start_line = *line;
	let mut depth: usize = 1;
	let start = *pos;

	while *pos < chars.len() {
		let c = chars[*pos];

		match c {
			'"' | '\'' => {
				// Skip string literal
				let quote = c;
				*pos += 1;
				while *pos < chars.len() {
					if chars[*pos] == '\\' && *pos + 1 < chars.len() {
						*pos += 2; // skip escape sequence
					} else if chars[*pos] == quote {
						*pos += 1;
						break;
					} else {
						if chars[*pos] == '\n' {
							*line += 1;
						}
						*pos += 1;
					}
				}
			}
			'/' if *pos + 1 < chars.len() && chars[*pos + 1] == '/' => {
				// Skip line comment
				while *pos < chars.len() && chars[*pos] != '\n' {
					*pos += 1;
				}
			}
			'{' => {
				depth += 1;
				*pos += 1;
			}
			'}' => {
				depth -= 1;
				if depth == 0 {
					let body: String = chars[start..*pos].iter().collect();
					*pos += 1; // consume closing brace
					return Ok(body.trim().to_string());
				}
				*pos += 1;
			}
			'\n' => {
				*line += 1;
				*pos += 1;
			}
			_ => {
				*pos += 1;
			}
		}
	}

	Err(ParseError {
		message: "unclosed test body (missing '}')".to_string(),
		line: start_line,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_single_test() {
		let input = r#"test("hello") { SELECT 1 }"#;
		let cases = parse(input).unwrap();
		assert_eq!(cases.len(), 1);
		assert_eq!(cases[0].name, "hello");
		assert_eq!(cases[0].body, "SELECT 1");
		assert_eq!(cases[0].line, 1);
	}

	#[test]
	fn parse_multiple_tests() {
		let input = r#"
test("first") {
    CREATE NAMESPACE test
}

test("second") {
    SELECT 1
}
"#;
		let cases = parse(input).unwrap();
		assert_eq!(cases.len(), 2);
		assert_eq!(cases[0].name, "first");
		assert_eq!(cases[1].name, "second");
	}

	#[test]
	fn parse_nested_braces() {
		let input = r#"test("nested") { INSERT t [{ id: 1 }] }"#;
		let cases = parse(input).unwrap();
		assert_eq!(cases.len(), 1);
		assert_eq!(cases[0].body, "INSERT t [{ id: 1 }]");
	}

	#[test]
	fn parse_string_with_braces() {
		let input = r#"test("strings") { ASSERT { "hello { world }" == "hello { world }" } }"#;
		let cases = parse(input).unwrap();
		assert_eq!(cases.len(), 1);
		assert!(cases[0].body.contains("hello { world }"));
	}

	#[test]
	fn parse_comments_with_braces() {
		let input = r#"
// this is a top-level comment
test("comments") {
    // this { has braces }
    SELECT 1
}
"#;
		let cases = parse(input).unwrap();
		assert_eq!(cases.len(), 1);
		assert!(cases[0].body.contains("SELECT 1"));
	}

	#[test]
	fn parse_error_unclosed_body() {
		let input = r#"test("oops") { SELECT 1"#;
		let err = parse(input).unwrap_err();
		assert!(err.message.contains("unclosed"), "{}", err.message);
	}

	#[test]
	fn parse_error_missing_name() {
		let input = r#"test() { SELECT 1 }"#;
		let err = parse(input).unwrap_err();
		assert!(err.message.contains("\""), "{}", err.message);
	}

	#[test]
	fn parse_error_unexpected_token() {
		let input = r#"foo("test") { SELECT 1 }"#;
		let err = parse(input).unwrap_err();
		assert!(err.message.contains("expected 'test'"), "{}", err.message);
	}

	#[test]
	fn parse_empty_input() {
		let cases = parse("").unwrap();
		assert!(cases.is_empty());
	}

	#[test]
	fn parse_whitespace_only() {
		let cases = parse("   \n\n  ").unwrap();
		assert!(cases.is_empty());
	}

	#[test]
	fn parse_comments_only() {
		let cases = parse("// just a comment\n// another comment\n").unwrap();
		assert!(cases.is_empty());
	}

	#[test]
	fn parse_escaped_quotes_in_name() {
		let input = r#"test("test \"quoted\"") { SELECT 1 }"#;
		let cases = parse(input).unwrap();
		assert_eq!(cases[0].name, r#"test "quoted""#);
	}
}
