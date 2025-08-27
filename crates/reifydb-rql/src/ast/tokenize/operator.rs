// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

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
		assert_eq!(
			TokenKind::Operator(op),
			tokens[0].kind,
			"type mismatch for symbol: {}",
			symbol
		);
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
