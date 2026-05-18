// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Abstract syntax tree for RQL. The parser entry point `parse_str` tokenises a source string and emits an
//! `AstStatement` tree allocated in a bump arena tied to the source lifetime, so spans inside the AST can refer
//! back to fragments of the original text without copying.
//!
//! AST shapes are public so external tooling - formatters, linters, the explain renderer - can inspect parsed
//! queries without re-parsing them. Adding or renaming a public AST node is a breaking change for that tooling.

use tracing::instrument;

use crate::{Result, ast::ast::AstStatement, bump::Bump, token::tokenize};

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
