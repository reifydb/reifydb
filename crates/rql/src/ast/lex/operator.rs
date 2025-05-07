// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::{Token, TokenKind};
use nom::branch::alt;
use nom::bytes::tag;
use nom::combinator::value;
use nom::{IResult, Input, Parser};
use nom_locate::LocatedSpan;

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
    QuestionMark     => "?"
}

pub(crate) fn parse_operator(input: LocatedSpan<&str>) -> IResult<LocatedSpan<&str>, Token> {
    let start = input;

    let parser = alt((
        alt((
            value(Operator::DoubleLeftAngle, tag("<<")),
            value(Operator::LeftAngleEqual, tag("<=")),
            value(Operator::DoubleRightAngle, tag(">>")),
            value(Operator::RightAngleEqual, tag(">=")),
            value(Operator::DoubleEqual, tag("==")),
            value(Operator::DoubleColon, tag("::")),
            value(Operator::Arrow, tag("->")),
            value(Operator::DoubleDot, tag("..")),
            value(Operator::DoubleAmpersand, tag("&&")),
            value(Operator::DoublePipe, tag("||")),
            value(Operator::BangEqual, tag("!=")),
            value(Operator::OpenParen, tag("(")),
            value(Operator::CloseParen, tag(")")),
        )),
        alt((
            value(Operator::OpenBracket, tag("[")),
            value(Operator::CloseBracket, tag("]")),
            value(Operator::LeftAngle, tag("<")),
            value(Operator::RightAngle, tag(">")),
            value(Operator::Dot, tag(".")),
            value(Operator::Colon, tag(":")),
            value(Operator::Plus, tag("+")),
            value(Operator::Minus, tag("-")),
            value(Operator::Asterisk, tag("*")),
            value(Operator::Slash, tag("/")),
            value(Operator::Ampersand, tag("&")),
            value(Operator::Pipe, tag("|")),
            value(Operator::Caret, tag("^")),
            value(Operator::Percent, tag("%")),
            value(Operator::Equal, tag("=")),
            value(Operator::Bang, tag("!")),
            value(Operator::QuestionMark, tag("?")),
        )),
    ));

    parser.map(|op| Token { kind: TokenKind::Operator(op), span: start.take(op.as_str().len()).into() }).parse(input)
}

#[cfg(test)]
mod tests {
    use crate::ast::lex::operator::{parse_operator, Operator};
    use crate::ast::lex::TokenKind;
    use nom_locate::LocatedSpan;

    #[test]
    fn test_parse_operator_invalid() {
        let input = LocatedSpan::new("foobar rest");
        let result = parse_operator(input);

        assert!(result.is_err(), "expected error parsing invalid operator, got: {:?}", result);
    }

    fn check_operator(op: Operator, symbol: &str) {
        let input_str = format!("{symbol} rest");
        let input = LocatedSpan::new(input_str.as_str());

        let result = parse_operator(input).unwrap();
        let (remaining, token) = result;

        assert_eq!(TokenKind::Operator(op), token.kind, "kind mismatch for symbol: {}", symbol);
        assert_eq!(token.span.fragment, symbol);
        assert_eq!(token.span.offset, 0);
        assert_eq!(token.span.line, 1);
        assert_eq!(remaining.fragment(), &format!(" rest"));
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
        test_operator_question_mark => (QuestionMark, "?")
    }
}
