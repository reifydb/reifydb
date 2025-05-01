// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Operator {
    OpenParen,        // (
    CloseParen,       // )
    OpenCurly,        // {
    CloseCurly,       // }
    OpenBracket,      // [
    CloseBracket,     // ]
    LeftAngle,        // <
    DoubleLeftAngle,  // <<
    LeftAngleEqual,   // <=
    RightAngle,       // >
    DoubleRightAngle, // >>
    RightAngleEqual,  // >=
    Dot,              // .
    Colon,            // :
    DoubleColon,      // ::
    Arrow,            // ->
    DoubleDot,        // ..
    Plus,             // +
    Minus,            // -
    Asterisk,         // *
    Slash,            // /
    Ampersand,        // &
    DoubleAmpersand,  // &&
    Pipe,             // |
    DoublePipe,       // ||
    Caret,            // ^
    Percent,          // %
    Equal,            // =
    DoubleEqual,      // ==
    Bang,             // !
    BangEqual,        // !=
    QuestionMark,     // ?
}
