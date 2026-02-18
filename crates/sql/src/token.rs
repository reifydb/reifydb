// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::Error;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
	// Keywords
	Keyword(Keyword),
	// Identifiers
	Ident(String),
	// Literals
	Integer(i64),
	Float(f64),
	StringLit(String),
	// Operators & punctuation
	Asterisk,   // *
	Comma,      // ,
	Dot,        // .
	Semicolon,  // ;
	OpenParen,  // (
	CloseParen, // )
	Plus,       // +
	Minus,      // -
	Slash,      // /
	Percent,    // %
	Eq,         // =
	NotEq,      // <> or !=
	Lt,         // <
	Gt,         // >
	LtEq,       // <=
	GtEq,       // >=
}

#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
	Select,
	From,
	Where,
	And,
	Or,
	Not,
	As,
	Order,
	By,
	Asc,
	Desc,
	Limit,
	Offset,
	Group,
	Having,
	Distinct,
	Insert,
	Into,
	Values,
	Update,
	Set,
	Delete,
	Create,
	Table,
	Join,
	Inner,
	Left,
	Right,
	On,
	Null,
	True,
	False,
	Is,
	In,
	Between,
	Cast,
	Count,
	Sum,
	Avg,
	Min,
	Max,
	// SQL types
	Int,
	Int2,
	Int4,
	Int8,
	Smallint,
	Integer,
	Bigint,
	Float4,
	Float8,
	Real,
	Double,
	Precision,
	Boolean,
	Bool,
	Varchar,
	Text,
	Char,
	Utf8,
	Blob,
	Primary,
	Key,
	With,
	Recursive,
}

pub fn tokenize(sql: &str) -> Result<Vec<Token>, Error> {
	let mut tokens = Vec::new();
	let chars: Vec<char> = sql.chars().collect();
	let len = chars.len();
	let mut i = 0;

	while i < len {
		let c = chars[i];

		// Skip whitespace
		if c.is_ascii_whitespace() {
			i += 1;
			continue;
		}

		// Skip line comments (-- ...)
		if c == '-' && i + 1 < len && chars[i + 1] == '-' {
			while i < len && chars[i] != '\n' {
				i += 1;
			}
			continue;
		}

		// Operators and punctuation
		match c {
			'*' => {
				tokens.push(Token::Asterisk);
				i += 1;
				continue;
			}
			',' => {
				tokens.push(Token::Comma);
				i += 1;
				continue;
			}
			'.' => {
				tokens.push(Token::Dot);
				i += 1;
				continue;
			}
			';' => {
				tokens.push(Token::Semicolon);
				i += 1;
				continue;
			}
			'(' => {
				tokens.push(Token::OpenParen);
				i += 1;
				continue;
			}
			')' => {
				tokens.push(Token::CloseParen);
				i += 1;
				continue;
			}
			'+' => {
				tokens.push(Token::Plus);
				i += 1;
				continue;
			}
			'-' => {
				tokens.push(Token::Minus);
				i += 1;
				continue;
			}
			'/' => {
				tokens.push(Token::Slash);
				i += 1;
				continue;
			}
			'%' => {
				tokens.push(Token::Percent);
				i += 1;
				continue;
			}
			'=' => {
				tokens.push(Token::Eq);
				i += 1;
				continue;
			}
			'<' => {
				if i + 1 < len && chars[i + 1] == '=' {
					tokens.push(Token::LtEq);
					i += 2;
				} else if i + 1 < len && chars[i + 1] == '>' {
					tokens.push(Token::NotEq);
					i += 2;
				} else {
					tokens.push(Token::Lt);
					i += 1;
				}
				continue;
			}
			'>' => {
				if i + 1 < len && chars[i + 1] == '=' {
					tokens.push(Token::GtEq);
					i += 2;
				} else {
					tokens.push(Token::Gt);
					i += 1;
				}
				continue;
			}
			'!' => {
				if i + 1 < len && chars[i + 1] == '=' {
					tokens.push(Token::NotEq);
					i += 2;
					continue;
				}
				return Err(Error(format!("unexpected character '!' at position {i}")));
			}
			_ => {}
		}

		// String literals
		if c == '\'' {
			i += 1;
			let mut s = String::new();
			while i < len {
				if chars[i] == '\'' {
					// Check for escaped single quote ''
					if i + 1 < len && chars[i + 1] == '\'' {
						s.push('\'');
						i += 2;
					} else {
						break;
					}
				} else {
					s.push(chars[i]);
					i += 1;
				}
			}
			if i >= len {
				return Err(Error("unterminated string literal".into()));
			}
			i += 1; // skip closing quote
			tokens.push(Token::StringLit(s));
			continue;
		}

		// Numeric literals
		if c.is_ascii_digit() {
			let start = i;
			while i < len && chars[i].is_ascii_digit() {
				i += 1;
			}
			if i < len && chars[i] == '.' && i + 1 < len && chars[i + 1].is_ascii_digit() {
				i += 1; // skip dot
				while i < len && chars[i].is_ascii_digit() {
					i += 1;
				}
				let text: String = chars[start..i].iter().collect();
				let f: f64 = text.parse().map_err(|e| Error(format!("invalid float: {e}")))?;
				tokens.push(Token::Float(f));
			} else {
				let text: String = chars[start..i].iter().collect();
				let n: i64 = text.parse().map_err(|e| Error(format!("invalid integer: {e}")))?;
				tokens.push(Token::Integer(n));
			}
			continue;
		}

		// Identifiers and keywords
		if c.is_ascii_alphabetic() || c == '_' {
			let start = i;
			while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
				i += 1;
			}
			let word: String = chars[start..i].iter().collect();
			let upper = word.to_ascii_uppercase();
			let token = match upper.as_str() {
				"SELECT" => Token::Keyword(Keyword::Select),
				"FROM" => Token::Keyword(Keyword::From),
				"WHERE" => Token::Keyword(Keyword::Where),
				"AND" => Token::Keyword(Keyword::And),
				"OR" => Token::Keyword(Keyword::Or),
				"NOT" => Token::Keyword(Keyword::Not),
				"AS" => Token::Keyword(Keyword::As),
				"ORDER" => Token::Keyword(Keyword::Order),
				"BY" => Token::Keyword(Keyword::By),
				"ASC" => Token::Keyword(Keyword::Asc),
				"DESC" => Token::Keyword(Keyword::Desc),
				"LIMIT" => Token::Keyword(Keyword::Limit),
				"OFFSET" => Token::Keyword(Keyword::Offset),
				"GROUP" => Token::Keyword(Keyword::Group),
				"HAVING" => Token::Keyword(Keyword::Having),
				"DISTINCT" => Token::Keyword(Keyword::Distinct),
				"INSERT" => Token::Keyword(Keyword::Insert),
				"INTO" => Token::Keyword(Keyword::Into),
				"VALUES" => Token::Keyword(Keyword::Values),
				"UPDATE" => Token::Keyword(Keyword::Update),
				"SET" => Token::Keyword(Keyword::Set),
				"DELETE" => Token::Keyword(Keyword::Delete),
				"CREATE" => Token::Keyword(Keyword::Create),
				"TABLE" => Token::Keyword(Keyword::Table),
				"JOIN" => Token::Keyword(Keyword::Join),
				"INNER" => Token::Keyword(Keyword::Inner),
				"LEFT" => Token::Keyword(Keyword::Left),
				"RIGHT" => Token::Keyword(Keyword::Right),
				"ON" => Token::Keyword(Keyword::On),
				"NULL" => Token::Keyword(Keyword::Null),
				"TRUE" => Token::Keyword(Keyword::True),
				"FALSE" => Token::Keyword(Keyword::False),
				"IS" => Token::Keyword(Keyword::Is),
				"IN" => Token::Keyword(Keyword::In),
				"BETWEEN" => Token::Keyword(Keyword::Between),
				"CAST" => Token::Keyword(Keyword::Cast),
				"COUNT" => Token::Keyword(Keyword::Count),
				"SUM" => Token::Keyword(Keyword::Sum),
				"AVG" => Token::Keyword(Keyword::Avg),
				"MIN" => Token::Keyword(Keyword::Min),
				"MAX" => Token::Keyword(Keyword::Max),
				"INT" => Token::Keyword(Keyword::Int),
				"INT2" => Token::Keyword(Keyword::Int2),
				"INT4" => Token::Keyword(Keyword::Int4),
				"INT8" => Token::Keyword(Keyword::Int8),
				"SMALLINT" => Token::Keyword(Keyword::Smallint),
				"INTEGER" => Token::Keyword(Keyword::Integer),
				"BIGINT" => Token::Keyword(Keyword::Bigint),
				"FLOAT4" => Token::Keyword(Keyword::Float4),
				"FLOAT8" => Token::Keyword(Keyword::Float8),
				"REAL" => Token::Keyword(Keyword::Real),
				"DOUBLE" => Token::Keyword(Keyword::Double),
				"PRECISION" => Token::Keyword(Keyword::Precision),
				"BOOLEAN" => Token::Keyword(Keyword::Boolean),
				"BOOL" => Token::Keyword(Keyword::Bool),
				"VARCHAR" => Token::Keyword(Keyword::Varchar),
				"TEXT" => Token::Keyword(Keyword::Text),
				"CHAR" => Token::Keyword(Keyword::Char),
				"UTF8" => Token::Keyword(Keyword::Utf8),
				"BLOB" => Token::Keyword(Keyword::Blob),
				"PRIMARY" => Token::Keyword(Keyword::Primary),
				"KEY" => Token::Keyword(Keyword::Key),
				"WITH" => Token::Keyword(Keyword::With),
				"RECURSIVE" => Token::Keyword(Keyword::Recursive),
				_ => Token::Ident(word),
			};
			tokens.push(token);
			continue;
		}

		return Err(Error(format!("unexpected character '{c}' at position {i}")));
	}

	Ok(tokens)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_simple_select() {
		let tokens = tokenize("SELECT id, name FROM users").unwrap();
		assert_eq!(
			tokens,
			vec![
				Token::Keyword(Keyword::Select),
				Token::Ident("id".into()),
				Token::Comma,
				Token::Ident("name".into()),
				Token::Keyword(Keyword::From),
				Token::Ident("users".into()),
			]
		);
	}

	#[test]
	fn test_string_literal() {
		let tokens = tokenize("SELECT 'hello'").unwrap();
		assert_eq!(tokens, vec![Token::Keyword(Keyword::Select), Token::StringLit("hello".into()),]);
	}

	#[test]
	fn test_comparison_operators() {
		let tokens = tokenize("a <> b").unwrap();
		assert_eq!(tokens, vec![Token::Ident("a".into()), Token::NotEq, Token::Ident("b".into()),]);
	}

	#[test]
	fn test_numeric_literals() {
		let tokens = tokenize("42 3.14").unwrap();
		assert_eq!(tokens, vec![Token::Integer(42), Token::Float(3.14),]);
	}
}
