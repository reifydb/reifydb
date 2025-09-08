// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use crate::ast::{
	ast::*,
	tokenize::{Token, TokenKind},
};
mod ast;
pub(crate) mod parse;
pub(crate) mod tokenize;
pub use parse::parse;
pub use tokenize::tokenize;

pub fn parse_str(str: &str) -> crate::Result<Vec<AstStatement>> {
	let tokens = tokenize(str)?;
	let statements = parse::parse(tokens)?;
	Ok(statements)
}
