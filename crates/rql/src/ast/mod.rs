// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Abstract syntax tree for RQL. The tree is bump-allocated against the source lifetime, so its spans can point
//! back into the original text without copying it. AST shapes are public for external tooling (formatters, linters,
//! the explain renderer), which makes adding or renaming a node a breaking change for them.

use bumpalo::Bump;
use tracing::instrument;

use crate::{Result, ast::ast::AstStatement, token::tokenize};

#[allow(clippy::module_inception)]
pub mod ast;
pub mod identifier;
pub(crate) mod parse;

#[instrument(name = "rql::parse", level = "trace", skip(bump, str))]
pub fn parse_str<'b>(bump: &'b Bump, str: &'b str) -> Result<Vec<AstStatement<'b>>> {
	let tokens = tokenize(bump, str)?;
	let statements = parse::parse(bump, str, tokens.into_iter().collect())?;
	Ok(statements)
}
