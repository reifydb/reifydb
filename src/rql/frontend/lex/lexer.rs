// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::iter::Peekable;
use std::str::Chars;

/// In order to be able to create an AST.
/// The lexer preprocesses raw RQL strings into a sequence of
/// lexical tokens, which are passed on to the RQL parser.
pub struct Lexer<'a> {
    chars: Peekable<Chars<'a>>,
}

impl<'a> Lexer<'a> {
    /// Creates a new lexer for the given string.
    pub fn new(input: &'a str) -> Lexer<'a> {
        Lexer { chars: input.chars().peekable() }
    }
}
