// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use Literal::Undefined;
use reifydb_core::OwnedFragment::Statement;

use super::cursor::Cursor;
use crate::ast::lex::{
	Keyword, Literal, Operator, ParameterKind, Separator, Token, TokenKind,
};

/// Scan for a keyword token
pub fn scan_keyword(cursor: &mut Cursor) -> Option<Token> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	// Keywords are case-insensitive and must be followed by a
	// non-identifier character
	const KEYWORDS: &[(&str, Keyword)] = &[
		("MAP", Keyword::Map),
		("BY", Keyword::By),
		("FROM", Keyword::From),
		("WHERE", Keyword::Where),
		("AGGREGATE", Keyword::Aggregate),
		("HAVING", Keyword::Having),
		("SORT", Keyword::Sort),
		("TAKE", Keyword::Take),
		("OFFSET", Keyword::Offset),
		("LEFT", Keyword::Left),
		("INNER", Keyword::Inner),
		("NATURAL", Keyword::Natural),
		("JOIN", Keyword::Join),
		("ON", Keyword::On),
		("USING", Keyword::Using),
		("UNION", Keyword::Union),
		("INTERSECT", Keyword::Intersect),
		("EXCEPT", Keyword::Except),
		("INSERT", Keyword::Insert),
		("INTO", Keyword::Into),
		("UPDATE", Keyword::Update),
		("SET", Keyword::Set),
		("DELETE", Keyword::Delete),
		("LET", Keyword::Let),
		("IF", Keyword::If),
		("ELSE", Keyword::Else),
		("END", Keyword::End),
		("LOOP", Keyword::Loop),
		("RETURN", Keyword::Return),
		("DEFINE", Keyword::Define),
		("FUNCTION", Keyword::Function),
		("CALL", Keyword::Call),
		("CAST", Keyword::Cast),
		("DESCRIBE", Keyword::Describe),
		("SHOW", Keyword::Show),
		("CREATE", Keyword::Create),
		("ALTER", Keyword::Alter),
		("DROP", Keyword::Drop),
		("FILTER", Keyword::Filter),
		("IN", Keyword::In),
		("BETWEEN", Keyword::Between),
		("LIKE", Keyword::Like),
		("IS", Keyword::Is),
		("WITH", Keyword::With),
		("SCHEMA", Keyword::Schema),
		("SEQUENCE", Keyword::Sequence),
		("SERIES", Keyword::Series),
		("TABLE", Keyword::Table),
		("POLICY", Keyword::Policy),
		("VIEW", Keyword::View),
		("DEFERRED", Keyword::Deferred),
		("TRANSACTIONAL", Keyword::Transactional),
		("INDEX", Keyword::Index),
		("UNIQUE", Keyword::Unique),
		("PRIMARY", Keyword::Primary),
		("KEY", Keyword::Key),
		("ASC", Keyword::Asc),
		("DESC", Keyword::Desc),
		("AUTO", Keyword::Auto),
		("INCREMENT", Keyword::Increment),
		("VALUE", Keyword::Value),
	];

	for (keyword_str, keyword) in KEYWORDS {
		let peek = cursor.peek_str(keyword_str.len());
		if peek.eq_ignore_ascii_case(keyword_str) {
			// Check that the next character is not an identifier
			// continuation
			let next_char = cursor.peek_ahead(keyword_str.len());
			if next_char.map_or(true, |ch| {
				!is_identifier_char(ch) && ch != '.'
			}) {
				cursor.consume_str_ignore_case(keyword_str);
				return Some(Token {
					kind: TokenKind::Keyword(*keyword),
					fragment: cursor.make_fragment(
						start_pos,
						start_line,
						start_column,
					),
				});
			}
		}
	}

	None
}

/// Scan for an operator token
pub fn scan_operator(cursor: &mut Cursor) -> Option<Token> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	// Check multi-character operators first
	const MULTI_CHAR_OPS: &[(&str, Operator)] = &[
		("<<", Operator::DoubleLeftAngle),
		("<=", Operator::LeftAngleEqual),
		(">>", Operator::DoubleRightAngle),
		(">=", Operator::RightAngleEqual),
		("::", Operator::DoubleColon),
		("->", Operator::Arrow),
		("..", Operator::DoubleDot),
		("&&", Operator::DoubleAmpersand),
		("||", Operator::DoublePipe),
		("==", Operator::DoubleEqual),
		("!=", Operator::BangEqual),
	];

	for (op_str, op) in MULTI_CHAR_OPS {
		if cursor.consume_str(op_str) {
			return Some(Token {
				kind: TokenKind::Operator(*op),
				fragment: cursor.make_fragment(
					start_pos,
					start_line,
					start_column,
				),
			});
		}
	}

	// Word operators (must be followed by non-identifier char)
	const WORD_OPS: &[(&str, Operator)] = &[
		("as", Operator::As),
		("and", Operator::And),
		("or", Operator::Or),
		("not", Operator::Not),
		("xor", Operator::Xor),
	];

	for (word, op) in WORD_OPS {
		let peek = cursor.peek_str(word.len());
		if peek.eq_ignore_ascii_case(word) {
			let next_char = cursor.peek_ahead(word.len());
			if next_char.map_or(true, |ch| !is_identifier_char(ch))
			{
				cursor.consume_str_ignore_case(word);
				return Some(Token {
					kind: TokenKind::Operator(*op),
					fragment: cursor.make_fragment(
						start_pos,
						start_line,
						start_column,
					),
				});
			}
		}
	}

	// Single character operators
	let op = match cursor.peek()? {
		'(' => Operator::OpenParen,
		')' => Operator::CloseParen,
		'[' => Operator::OpenBracket,
		']' => Operator::CloseBracket,
		'{' => Operator::OpenCurly,
		'}' => Operator::CloseCurly,
		'<' => Operator::LeftAngle,
		'>' => Operator::RightAngle,
		'.' => Operator::Dot,
		':' => Operator::Colon,
		'+' => Operator::Plus,
		'-' => Operator::Minus,
		'*' => Operator::Asterisk,
		'/' => Operator::Slash,
		'&' => Operator::Ampersand,
		'|' => Operator::Pipe,
		'^' => Operator::Caret,
		'%' => Operator::Percent,
		'=' => Operator::Equal,
		'!' => Operator::Bang,
		'?' => Operator::QuestionMark,
		_ => return None,
	};

	cursor.consume();
	Some(Token {
		kind: TokenKind::Operator(op),
		fragment: cursor.make_fragment(
			start_pos,
			start_line,
			start_column,
		),
	})
}

/// Scan for a separator token
pub fn scan_separator(cursor: &mut Cursor) -> Option<Token> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	let sep = match cursor.peek()? {
		';' => Separator::Semicolon,
		',' => Separator::Comma,
		'\n' => Separator::NewLine,
		_ => return None,
	};

	cursor.consume();
	Some(Token {
		kind: TokenKind::Separator(sep),
		fragment: cursor.make_fragment(
			start_pos,
			start_line,
			start_column,
		),
	})
}

/// Scan for a parameter token ($1, $name, etc.)
pub fn scan_parameter(cursor: &mut Cursor) -> Option<Token> {
	if cursor.peek() != Some('$') {
		return None;
	}

	let state = cursor.save_state();
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	cursor.consume(); // consume '$'

	// Check for positional parameter ($1, $2, etc.)
	if let Some(ch) = cursor.peek() {
		if ch.is_ascii_digit() {
			let num_str =
				cursor.consume_while(|c| c.is_ascii_digit());
			if let Ok(num) = num_str.parse::<u32>() {
				if num > 0 {
					return Some(Token {
                        kind: TokenKind::Parameter(ParameterKind::Positional(num)),
                        fragment: cursor.make_fragment(start_pos, start_line, start_column),
                    });
				}
			}
			// $0 is invalid, restore state
			cursor.restore_state(state);
			return None;
		}

		// Check for named parameter ($name, $user_id, etc.)
		if is_identifier_start(ch) {
			cursor.consume_while(is_identifier_char);
			return Some(Token {
				kind: TokenKind::Parameter(
					ParameterKind::Named,
				),
				fragment: cursor.make_fragment(
					start_pos,
					start_line,
					start_column,
				),
			});
		}
	}

	// Just a $ by itself, restore state
	cursor.restore_state(state);
	None
}

/// Scan for an identifier token
pub fn scan_identifier(cursor: &mut Cursor) -> Option<Token> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	if !cursor.peek().map_or(false, is_identifier_start) {
		return None;
	}

	cursor.consume_while(is_identifier_char);

	Some(Token {
		kind: TokenKind::Identifier,
		fragment: cursor.make_fragment(
			start_pos,
			start_line,
			start_column,
		),
	})
}

/// Scan for a literal token
pub fn scan_literal(cursor: &mut Cursor) -> Option<Token> {
	// Try each literal type
	scan_text(cursor)
		.or_else(|| scan_number(cursor))
		.or_else(|| scan_boolean(cursor))
		.or_else(|| scan_undefined(cursor))
		.or_else(|| scan_temporal(cursor))
}

/// Scan for a text literal ('...' or "...")
fn scan_text(cursor: &mut Cursor) -> Option<Token> {
	let quote = cursor.peek()?;
	if quote != '\'' && quote != '"' {
		return None;
	}

	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	cursor.consume(); // consume opening quote

	let text_start = cursor.pos();
	while let Some(ch) = cursor.peek() {
		if ch == quote {
			let text_end = cursor.pos();
			cursor.consume(); // consume closing quote

			// Create fragment with just the text content (no
			// quotes)
			let mut fragment = cursor.make_fragment(
				start_pos,
				start_line,
				start_column,
			);
			if let Statement {
				text,
				..
			} = &mut fragment
			{
				*text = cursor.slice_from(text_start)
					[..text_end - text_start]
					.to_string();
			}

			return Some(Token {
				kind: TokenKind::Literal(Literal::Text),
				fragment,
			});
		}
		cursor.consume();
	}

	None // Unterminated string
}

/// Scan for a number literal
fn scan_number(cursor: &mut Cursor) -> Option<Token> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	// Check for hex (0x...)
	if cursor.peek_str(2).eq_ignore_ascii_case("0x") {
		cursor.consume();
		cursor.consume();
		let hex_part = cursor
			.consume_while(|c| c.is_ascii_hexdigit() || c == '_');
		if !hex_part.is_empty()
			&& !hex_part.starts_with('_')
			&& !hex_part.ends_with('_')
			&& !hex_part.contains("__")
		{
			return Some(Token {
				kind: TokenKind::Literal(Literal::Number),
				fragment: cursor.make_fragment(
					start_pos,
					start_line,
					start_column,
				),
			});
		}
		return None;
	}

	// Check for binary (0b...)
	if cursor.peek_str(2).eq_ignore_ascii_case("0b") {
		cursor.consume();
		cursor.consume();
		let bin_part = cursor
			.consume_while(|c| c == '0' || c == '1' || c == '_');
		if !bin_part.is_empty()
			&& !bin_part.starts_with('_')
			&& !bin_part.ends_with('_')
			&& !bin_part.contains("__")
		{
			return Some(Token {
				kind: TokenKind::Literal(Literal::Number),
				fragment: cursor.make_fragment(
					start_pos,
					start_line,
					start_column,
				),
			});
		}
		return None;
	}

	// Check for octal (0o...)
	if cursor.peek_str(2).eq_ignore_ascii_case("0o") {
		cursor.consume();
		cursor.consume();
		let oct_part =
			cursor.consume_while(|c| c.is_digit(8) || c == '_');
		if !oct_part.is_empty()
			&& !oct_part.starts_with('_')
			&& !oct_part.ends_with('_')
			&& !oct_part.contains("__")
		{
			return Some(Token {
				kind: TokenKind::Literal(Literal::Number),
				fragment: cursor.make_fragment(
					start_pos,
					start_line,
					start_column,
				),
			});
		}
		return None;
	}

	// Decimal number (including float and scientific notation)
	let state = cursor.save_state();

	// Check for leading dot (.123)
	let has_leading_dot = cursor.peek() == Some('.');
	if has_leading_dot {
		cursor.consume();
		if !cursor.peek().map_or(false, |c| c.is_ascii_digit()) {
			cursor.restore_state(state);
			return None;
		}
		cursor.consume_while(|c| c.is_ascii_digit() || c == '_');
	} else {
		// Integer part
		if !cursor.peek().map_or(false, |c| c.is_ascii_digit()) {
			return None;
		}
		cursor.consume_while(|c| c.is_ascii_digit() || c == '_');

		// Optional fractional part
		if cursor.peek() == Some('.') {
			let next = cursor.peek_ahead(1);
			// Check if this is a decimal point (followed by digit)
			// or not
			if next.map_or(false, |c| c.is_ascii_digit()) {
				cursor.consume(); // consume '.'
				cursor.consume_while(|c| {
					c.is_ascii_digit() || c == '_'
				});
			} else if next.is_none()
				|| next.map_or(false, |c| {
					c.is_whitespace() || is_operator_char(c)
				}) {
				// This is "123." format
				cursor.consume(); // consume '.'
			}
		}
	}

	// Optional exponent
	if let Some(e) = cursor.peek() {
		if e == 'e' || e == 'E' {
			cursor.consume();
			if let Some(sign) = cursor.peek() {
				if sign == '+' || sign == '-' {
					cursor.consume();
				}
			}
			if !cursor.peek().map_or(false, |c| c.is_ascii_digit())
			{
				cursor.restore_state(state);
				return None;
			}
			cursor.consume_while(|c| {
				c.is_ascii_digit() || c == '_'
			});
		}
	}

	// Validate the number format
	let num_str = cursor.slice_from(start_pos);
	if is_valid_number_format(num_str) {
		Some(Token {
			kind: TokenKind::Literal(Literal::Number),
			fragment: cursor.make_fragment(
				start_pos,
				start_line,
				start_column,
			),
		})
	} else {
		cursor.restore_state(state);
		None
	}
}

/// Scan for a boolean literal (true/false)
fn scan_boolean(cursor: &mut Cursor) -> Option<Token> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	if cursor.peek_str(4).eq_ignore_ascii_case("true") {
		let next = cursor.peek_ahead(4);
		if next.map_or(true, |c| !is_identifier_char(c)) {
			cursor.consume_str_ignore_case("true");
			return Some(Token {
				kind: TokenKind::Literal(Literal::True),
				fragment: cursor.make_fragment(
					start_pos,
					start_line,
					start_column,
				),
			});
		}
	}

	if cursor.peek_str(5).eq_ignore_ascii_case("false") {
		let next = cursor.peek_ahead(5);
		if next.map_or(true, |c| !is_identifier_char(c)) {
			cursor.consume_str_ignore_case("false");
			return Some(Token {
				kind: TokenKind::Literal(Literal::False),
				fragment: cursor.make_fragment(
					start_pos,
					start_line,
					start_column,
				),
			});
		}
	}

	None
}

/// Scan for undefined literal
fn scan_undefined(cursor: &mut Cursor) -> Option<Token> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	if cursor.peek_str(9).eq_ignore_ascii_case("undefined") {
		let next = cursor.peek_ahead(9);
		if next.map_or(true, |c| !is_identifier_char(c)) {
			cursor.consume_str_ignore_case("undefined");
			return Some(Token {
				kind: TokenKind::Literal(Undefined),
				fragment: cursor.make_fragment(
					start_pos,
					start_line,
					start_column,
				),
			});
		}
	}

	None
}

/// Scan for temporal literal (dates/times)
fn scan_temporal(cursor: &mut Cursor) -> Option<Token> {
	if cursor.peek() != Some('@') {
		return None;
	}

	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	cursor.consume(); // consume '@'

	// Accept any sequence of characters that could be part of a temporal
	// literal This includes letters, digits, colons, hyphens, dots, +, -,
	// /, T, etc.
	let content = cursor.consume_while(|c| {
		c.is_ascii_alphanumeric()
			|| c == '-' || c == ':'
			|| c == '.' || c == '+'
			|| c == '/' || c == 'T'
	});

	if content.is_empty() {
		// Just @ without any content - backtrack
		// We already consumed @, so go back one position
		// Actually, we can't backtrack easily here, so just return None
		// The @ will be caught as an unexpected character
		return None;
	}

	// Create fragment with the content (excluding @)
	let mut fragment =
		cursor.make_fragment(start_pos, start_line, start_column);
	if let Statement {
		text,
		..
	} = &mut fragment
	{
		// Remove the @ prefix from the text
		*text = text[1..].to_string();
	}

	Some(Token {
		kind: TokenKind::Literal(Literal::Temporal),
		fragment,
	})
}

// Helper functions

fn is_identifier_start(ch: char) -> bool {
	ch.is_ascii_alphabetic() || ch == '_'
}

fn is_identifier_char(ch: char) -> bool {
	ch.is_ascii_alphanumeric() || ch == '_'
}

fn is_operator_char(ch: char) -> bool {
	matches!(
		ch,
		'(' | ')'
			| '[' | ']' | '{' | '}'
			| '<' | '>' | '.' | ':'
			| '+' | '-' | '*' | '/'
			| '&' | '|' | '^' | '%'
			| '=' | '!' | '?' | ','
			| ';'
	)
}

fn is_valid_number_format(s: &str) -> bool {
	// Basic validation - more detailed validation can be added if needed
	if s.is_empty() {
		return false;
	}

	// Check for invalid underscore patterns
	if s.starts_with('_') || s.ends_with('_') || s.contains("__") {
		return false;
	}

	// Check for underscore around decimal point
	if s.contains("._") || s.contains("_.") {
		return false;
	}

	// Must contain at least one digit
	s.chars().any(|c| c.is_ascii_digit())
}
