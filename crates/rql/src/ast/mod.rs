// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use crate::ast::ast::*;
use crate::ast::lex::lex;
pub use error::Error;
use std::vec;

mod ast;
mod error;
mod lex;
mod parse;

pub fn parse(str: &str) -> Vec<AstStatement> {
    let tokens = lex(str).unwrap();
    vec![AstStatement(parse::parse(tokens).unwrap())]
}
