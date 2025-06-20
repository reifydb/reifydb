// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use crate::ast::ast::*;
use crate::ast::lex::lex;
pub use crate::ast::lex::{Token, TokenKind};
pub use error::Error;
use std::vec;

mod ast;
mod error;
mod lex;
mod parse;

pub fn parse(str: &str) -> Result<Vec<AstStatement>, Error> {
    let tokens = lex(str)?;
    let statement = parse::parse(tokens)?;
    Ok(vec![AstStatement(statement)])
}
