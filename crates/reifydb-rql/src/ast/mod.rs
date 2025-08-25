// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::lex;
pub use crate::ast::{
	ast::*,
	lex::{Token, TokenKind},
};
mod ast;
pub(crate) mod lex;
pub(crate) mod parse;
pub(crate) mod tokenize;

pub fn parse(str: &str) -> crate::Result<Vec<AstStatement>> {
	let tokens = lex(str)?;
	let statements = parse::parse(tokens)?;
	Ok(statements)
}
