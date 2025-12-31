// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::LazyLock};

use super::{
	cursor::Cursor,
	identifier::is_identifier_char,
	token::{Token, TokenKind},
};

macro_rules! operator {
    (
        $( $value:ident => $tag:literal ),*
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum Operator {  $( $value ),* }

        impl Operator {
            pub fn as_str(&self) -> &'static str {
                match self {
                    $( Operator::$value => $tag ),*
                }
            }
        }
    };
}

operator! {
    OpenParen        => "(",
    CloseParen       => ")",
    OpenBracket      => "[",
    CloseBracket     => "]",
    OpenCurly        => "{",
    CloseCurly      => "}",
    LeftAngle        => "<",
    DoubleLeftAngle  => "<<",
    LeftAngleEqual   => "<=",
    RightAngle       => ">",
    DoubleRightAngle => ">>",
    RightAngleEqual  => ">=",
    Dot              => ".",
    Colon            => ":",
    DoubleColon      => "::",
    ColonEqual       => ":=",
    Arrow            => "->",
    DoubleDot        => "..",
    Plus             => "+",
    Minus            => "-",
    Asterisk         => "*",
    Slash            => "/",
    Ampersand        => "&",
    DoubleAmpersand  => "&&",
    Pipe             => "|",
    DoublePipe       => "||",
    Caret            => "^",
    Percent          => "%",
    Equal            => "=",
    DoubleEqual      => "==",
    Bang             => "!",
    BangEqual        => "!=",
    QuestionMark     => "?",
    As               => "as",
    And              => "and",
    Or               => "or",
    Not              => "not",
    Xor              => "xor"
}

static SINGLE_CHAR_OPERATORS: LazyLock<HashMap<char, Operator>> = LazyLock::new(|| {
	let mut map = HashMap::new();
	map.insert('(', Operator::OpenParen);
	map.insert(')', Operator::CloseParen);
	map.insert('[', Operator::OpenBracket);
	map.insert(']', Operator::CloseBracket);
	map.insert('{', Operator::OpenCurly);
	map.insert('}', Operator::CloseCurly);
	map.insert('<', Operator::LeftAngle);
	map.insert('>', Operator::RightAngle);
	map.insert('.', Operator::Dot);
	map.insert(':', Operator::Colon);
	map.insert('+', Operator::Plus);
	map.insert('-', Operator::Minus);
	map.insert('*', Operator::Asterisk);
	map.insert('/', Operator::Slash);
	map.insert('&', Operator::Ampersand);
	map.insert('|', Operator::Pipe);
	map.insert('^', Operator::Caret);
	map.insert('%', Operator::Percent);
	map.insert('=', Operator::Equal);
	map.insert('!', Operator::Bang);
	map.insert('?', Operator::QuestionMark);
	map
});

static WORD_OPERATORS: LazyLock<HashMap<&'static str, Operator>> = LazyLock::new(|| {
	let mut map = HashMap::new();
	map.insert("AS", Operator::As);
	map.insert("AND", Operator::And);
	map.insert("OR", Operator::Or);
	map.insert("NOT", Operator::Not);
	map.insert("XOR", Operator::Xor);
	map
});

/// Scan for an operator token
pub fn scan_operator(cursor: &mut Cursor) -> Option<Token> {
	let start_pos = cursor.pos();
	let start_line = cursor.line();
	let start_column = cursor.column();

	let ch = cursor.peek()?;

	// Check for multi-character operators first based on first character
	let multi_char_op = match ch {
		'<' => {
			if cursor.peek_str(2) == "<<" {
				cursor.consume_str("<<");
				Some(Operator::DoubleLeftAngle)
			} else if cursor.peek_str(2) == "<=" {
				cursor.consume_str("<=");
				Some(Operator::LeftAngleEqual)
			} else {
				None
			}
		}
		'>' => {
			if cursor.peek_str(2) == ">>" {
				cursor.consume_str(">>");
				Some(Operator::DoubleRightAngle)
			} else if cursor.peek_str(2) == ">=" {
				cursor.consume_str(">=");
				Some(Operator::RightAngleEqual)
			} else {
				None
			}
		}
		':' => {
			if cursor.peek_str(2) == "::" {
				cursor.consume_str("::");
				Some(Operator::DoubleColon)
			} else if cursor.peek_str(2) == ":=" {
				cursor.consume_str(":=");
				Some(Operator::ColonEqual)
			} else {
				None
			}
		}
		'-' => {
			if cursor.peek_str(2) == "->" {
				cursor.consume_str("->");
				Some(Operator::Arrow)
			} else {
				None
			}
		}
		'.' => {
			if cursor.peek_str(2) == ".." {
				cursor.consume_str("..");
				Some(Operator::DoubleDot)
			} else {
				None
			}
		}
		'&' => {
			if cursor.peek_str(2) == "&&" {
				cursor.consume_str("&&");
				Some(Operator::DoubleAmpersand)
			} else {
				None
			}
		}
		'|' => {
			if cursor.peek_str(2) == "||" {
				cursor.consume_str("||");
				Some(Operator::DoublePipe)
			} else {
				None
			}
		}
		'=' => {
			if cursor.peek_str(2) == "==" {
				cursor.consume_str("==");
				Some(Operator::DoubleEqual)
			} else {
				None
			}
		}
		'!' => {
			if cursor.peek_str(2) == "!=" {
				cursor.consume_str("!=");
				Some(Operator::BangEqual)
			} else {
				None
			}
		}
		_ => None,
	};

	if let Some(op) = multi_char_op {
		return Some(Token {
			kind: TokenKind::Operator(op),
			fragment: cursor.make_fragment(start_pos, start_line, start_column),
		});
	}

	// Check word operators for alphabetic characters
	if ch.is_ascii_alphabetic() {
		let remaining = cursor.remaining_input();
		let word_len =
			remaining.chars().take_while(|&c| is_identifier_char(c)).map(|c| c.len_utf8()).sum::<usize>();
		let word = &remaining[..word_len];
		let uppercase_word = word.to_uppercase();

		if let Some(&op) = WORD_OPERATORS.get(uppercase_word.as_str()) {
			let next_char = cursor.peek_ahead(word.chars().count());
			if next_char.map_or(true, |ch| !is_identifier_char(ch)) {
				for _ in 0..word.chars().count() {
					cursor.consume();
				}
				return Some(Token {
					kind: TokenKind::Operator(op),
					fragment: cursor.make_fragment(start_pos, start_line, start_column),
				});
			}
		}
		return None;
	}

	// Single character operators
	if let Some(&op) = SINGLE_CHAR_OPERATORS.get(&ch) {
		cursor.consume();
		Some(Token {
			kind: TokenKind::Operator(op),
			fragment: cursor.make_fragment(start_pos, start_line, start_column),
		})
	} else {
		None
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ast::tokenize::tokenize;

	#[test]
	fn test_parse_operator_invalid() {
		let tokens = tokenize("foobar rest").unwrap();
		// Should parse as identifier, not operator
		assert_eq!(tokens[0].kind, TokenKind::Identifier);
	}

	fn check_operator(op: Operator, symbol: &str) {
		let input_str = format!("{symbol} rest");
		let tokens = tokenize(&input_str).unwrap();

		assert!(tokens.len() >= 2);
		assert_eq!(TokenKind::Operator(op), tokens[0].kind, "type mismatch for symbol: {}", symbol);
		assert_eq!(tokens[0].fragment.text(), symbol);
		assert_eq!(tokens[0].fragment.column().0, 1);
		assert_eq!(tokens[0].fragment.line().0, 1);
	}

	macro_rules! generate_test {
        ($($name:ident => ($variant:ident, $symbol:literal)),*) => {
            $(
                #[test]
                fn $name() {
                    check_operator(Operator::$variant, $symbol);
                }
            )*
        };
    }

	generate_test! {
		test_operator_open_paren => (OpenParen, "("),
		test_operator_close_paren => (CloseParen, ")"),
		test_operator_open_bracket => (OpenBracket, "["),
		test_operator_close_bracket => (CloseBracket, "]"),
		test_operator_open_curly => (OpenCurly, "{"),
		test_operator_close_curly => (CloseCurly, "}"),

		test_operator_left_angle => (LeftAngle, "<"),
		test_operator_double_left_angle => (DoubleLeftAngle, "<<"),
		test_operator_left_angle_equal => (LeftAngleEqual, "<="),
		test_operator_right_angle => (RightAngle, ">"),
		test_operator_double_right_angle => (DoubleRightAngle, ">>"),
		test_operator_right_angle_equal => (RightAngleEqual, ">="),
		test_operator_dot => (Dot, "."),
		test_operator_colon => (Colon, ":"),
		test_operator_double_colon => (DoubleColon, "::"),
		test_operator_colon_equal => (ColonEqual, ":="),
		test_operator_arrow => (Arrow, "->"),
		test_operator_double_dot => (DoubleDot, ".."),
		test_operator_plus => (Plus, "+"),
		test_operator_minus => (Minus, "-"),
		test_operator_asterisk => (Asterisk, "*"),
		test_operator_slash => (Slash, "/"),
		test_operator_ampersand => (Ampersand, "&"),
		test_operator_double_ampersand => (DoubleAmpersand, "&&"),
		test_operator_pipe => (Pipe, "|"),
		test_operator_double_pipe => (DoublePipe, "||"),
		test_operator_caret => (Caret, "^"),
		test_operator_percent => (Percent, "%"),
		test_operator_equal => (Equal, "="),
		test_operator_double_equal => (DoubleEqual, "=="),
		test_operator_bang => (Bang, "!"),
		test_operator_bang_equal => (BangEqual, "!="),
		test_operator_question_mark => (QuestionMark, "?"),
		test_operator_as => (As, "as"),
		test_operator_and => (And, "and"),
		test_operator_or => (Or, "or"),
		test_operator_not => (Not, "not"),
		test_operator_xor => (Xor, "xor")
	}
}
