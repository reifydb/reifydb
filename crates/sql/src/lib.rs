// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
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
