// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use crate::rql::ast::ast::*;
use crate::rql::ast::lex::span::{Line, Offset, Span};
use crate::rql::ast::lex::{lex, Token, TokenKind};
pub use error::Error;

mod ast;
mod error;
mod lex;
mod parse;

pub fn parse(str: &str) -> Ast {
    let tokens = lex(str).unwrap();

    Ast::Block(AstBlock {
        token: Token { kind: TokenKind::EOF, span: Span { offset: Offset(0), line: Line(0), fragment: "".to_string() } },
        nodes: parse::parse(tokens).unwrap(),
    })
}
