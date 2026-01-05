// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! RQL compilation error types.

use std::fmt::{Display, Formatter};

mod compile;
mod lex;
pub mod parse;
mod plan;

pub use parse::ParseErrors;
use reifydb_type::{
	Fragment,
	diagnostic::{Diagnostic, IntoDiagnostic},
};

use crate::{ast::parse::ParseError, bytecode::compile::CompileError, plan::compile::PlanError, token::LexError};

/// Combined error type for RQL compilation operations.
///
/// This error type aggregates all possible errors that can occur during
/// the RQL compilation pipeline: lexing, parsing, planning, and bytecode compilation.
#[derive(Debug)]
pub enum RqlError {
	/// Lexer error during tokenization.
	Lex(LexError),

	/// Parse errors (can be multiple).
	Parse(Vec<ParseError>),

	/// Plan compilation error.
	Plan(PlanError),

	/// Bytecode compilation error.
	Compile(CompileError),

	/// Empty program (no statements to execute).
	EmptyProgram,

	/// Compilation task panicked or was cancelled.
	/// This can occur when compiling on a ComputePool.
	CompilationPanicked(String),
}

impl Display for RqlError {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			RqlError::Lex(err) => write!(f, "lexer error: {}", err),
			RqlError::Parse(errors) => {
				write!(f, "parse errors: ")?;
				for (i, err) in errors.iter().enumerate() {
					if i > 0 {
						write!(f, ", ")?;
					}
					write!(f, "{}", err)?;
				}
				Ok(())
			}
			RqlError::Plan(err) => write!(f, "plan error: {}", err),
			RqlError::Compile(err) => write!(f, "compile error: {}", err),
			RqlError::EmptyProgram => write!(f, "empty program"),
			RqlError::CompilationPanicked(msg) => write!(f, "compilation panicked: {}", msg),
		}
	}
}

impl std::error::Error for RqlError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			RqlError::Lex(err) => Some(err),
			RqlError::Parse(errors) => errors.first().map(|e| e as &dyn std::error::Error),
			RqlError::Plan(err) => Some(err),
			RqlError::Compile(err) => Some(err),
			RqlError::EmptyProgram => None,
			RqlError::CompilationPanicked(_) => None,
		}
	}
}

// Convenience From implementations for ergonomic error propagation
impl From<LexError> for RqlError {
	fn from(err: LexError) -> Self {
		RqlError::Lex(err)
	}
}

impl From<PlanError> for RqlError {
	fn from(err: PlanError) -> Self {
		RqlError::Plan(err)
	}
}

impl From<CompileError> for RqlError {
	fn from(err: CompileError) -> Self {
		RqlError::Compile(err)
	}
}

// Conversion to reifydb_type::Error (via Diagnostic)
impl From<RqlError> for reifydb_type::Error {
	fn from(err: RqlError) -> Self {
		let diagnostic = match err {
			RqlError::Lex(lex_err) => lex_err.into_diagnostic(),
			RqlError::Parse(parse_errs) => ParseErrors(parse_errs).into_diagnostic(),
			RqlError::Plan(plan_err) => plan_err.into_diagnostic(),
			RqlError::Compile(compile_err) => compile_err.into_diagnostic(),
			RqlError::EmptyProgram => empty_program_diagnostic(),
			RqlError::CompilationPanicked(msg) => compilation_panicked_diagnostic(msg),
		};
		reifydb_type::Error(diagnostic)
	}
}

/// Create diagnostic for empty program error.
fn empty_program_diagnostic() -> Diagnostic {
	Diagnostic {
		code: "RQL_001".to_string(),
		statement: None,
		message: "empty program".to_string(),
		column: None,
		fragment: Fragment::None,
		label: Some("program contains no statements".to_string()),
		help: Some("add at least one statement to the program".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Create diagnostic for compilation panic error.
fn compilation_panicked_diagnostic(msg: String) -> Diagnostic {
	Diagnostic {
		code: "RQL_006".to_string(),
		statement: None,
		message: msg,
		column: None,
		fragment: Fragment::None,
		label: Some("compilation task failed".to_string()),
		help: Some("this may indicate a compiler bug or resource exhaustion".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
