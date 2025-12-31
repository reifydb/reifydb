// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use tracing::instrument;

pub use crate::ast::{
	ast::*,
	tokenize::{Token, TokenKind},
};
mod ast;
pub mod identifier;
pub(crate) mod parse;
pub(crate) mod tokenize;
pub use parse::parse;
pub use tokenize::tokenize;

#[instrument(name = "rql::parse", level = "trace", skip(str))]
pub fn parse_str(str: &str) -> crate::Result<Vec<AstStatement>> {
	let tokens = tokenize(str)?;
	let statements = parse::parse(tokens)?;
	Ok(statements)
}
