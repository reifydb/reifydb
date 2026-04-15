// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod ast;
pub mod bump;
pub mod compiler;
pub mod parse;
pub mod reshape;
pub mod token;

pub use compiler::compiler::{Compiler, CompilerError};
pub use parse::parser::{Parser, ParserError};
pub use reshape::reshaper::Reshaper;
use thiserror::Error;
pub use token::lexer::{Lexer, LexerError};

#[derive(Error, Debug)]
pub enum Error {
	#[error("Lexer error: {0}")]
	Lexer(#[from] LexerError),
	#[error("Parser error: {0}")]
	Parser(#[from] ParserError),
	#[error("Compiler error: {0}")]
	Compiler(#[from] CompilerError),
}

pub type Result<T> = std::result::Result<T, Error>;
