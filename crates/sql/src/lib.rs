// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod ast;
pub mod emit;
pub mod parser;
pub mod token;

#[derive(Debug, Clone)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl std::error::Error for Error {}

pub fn transpile(sql: &str) -> Result<String, Error> {
	let tokens = token::tokenize(sql)?;
	let ast = parser::Parser::new(tokens).parse()?;
	let rql = emit::emit(&ast)?;
	Ok(rql)
}
