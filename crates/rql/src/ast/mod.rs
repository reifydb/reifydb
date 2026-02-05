// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use tracing::instrument;

use crate::{ast::ast::AstStatement, token::tokenize};

pub mod ast;
pub mod identifier;
pub(crate) mod parse;

#[instrument(name = "rql::parse", level = "trace", skip(str))]
pub fn parse_str(str: &str) -> crate::Result<Vec<AstStatement>> {
	let tokens = tokenize(str)?;
	let statements = parse::parse(tokens)?;
	Ok(statements)
}
