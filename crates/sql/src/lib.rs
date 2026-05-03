// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! SQL-to-RQL transpiler. Tokenises, parses, and emits a SQL statement into the equivalent RQL surface so callers
//! who arrive with a SQL query can submit it to an engine that natively speaks RQL. Coverage is intentionally narrow:
//! the goal is to support the SQL dialects existing tooling produces (SELECT, INSERT, UPDATE, DELETE, basic DDL),
//! not to be a full ANSI implementation.
//!
//! The crate produces RQL text rather than a planner-internal tree on purpose - the boundary stays at the source
//! level, so SQL behaviour can be verified by reading the emitted RQL and the resulting RQL diagnostics carry the
//! source span the user submitted.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]

use std::{error, fmt};

#[cfg_attr(not(debug_assertions), deny(warnings))]
pub mod ast;
pub mod emit;
pub mod parser;
pub mod token;

#[derive(Debug, Clone)]
pub struct Error(pub String);

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl error::Error for Error {}

pub fn transpile(sql: &str) -> Result<String, Error> {
	let tokens = token::tokenize(sql)?;
	let ast = parser::Parser::new(tokens).parse()?;
	let rql = emit::emit(&ast)?;
	Ok(rql)
}
