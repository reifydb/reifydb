// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use tracing::instrument;

use crate::{Result, ast::ast::AstStatement, bump::Bump, token::tokenize};

pub mod ast;
pub mod identifier;
pub(crate) mod parse;

#[instrument(name = "rql::parse", level = "trace", skip(bump, str))]
pub fn parse_str<'b>(bump: &'b Bump, str: &'b str) -> Result<Vec<AstStatement<'b>>> {
	let tokens = tokenize(bump, str)?;
	let statements = parse::parse(bump, str, tokens.into_iter().collect())?;
	Ok(statements)
}
