// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! DSL parser and compiler for the VM.
//!
//! This module provides a text-based DSL for defining pipelines:
//!
//! ```text
//! scan users | filter age > 21 | select [name, email] | take 10
//! ```

pub mod ast;
pub mod compile;
pub mod lexer;
pub mod parser;
pub mod token;

use std::sync::Arc;

pub use ast::{DslAst, ExprAst, PipelineAst, StageAst};
pub use compile::{CompileError, DslCompiler, SourceRegistry};
pub use lexer::{LexError, Lexer};
pub use parser::{ParseError, Parser};
use reifydb_engine::StandardTransaction;
use thiserror::Error;
pub use token::{Span, Token, TokenKind};

use crate::{
	bytecode::Program,
	compile::BytecodeCompiler,
	error::VmError,
	pipeline::Pipeline,
	vmcore::{VmContext, VmState},
};

/// Combined error type for DSL operations.
#[derive(Debug, Error)]
pub enum DslError {
	#[error("lexer error: {0}")]
	Lex(#[from] LexError),

	#[error("parse error: {0}")]
	Parse(#[from] ParseError),

	#[error("compile error: {0}")]
	Compile(#[from] CompileError),

	#[error("vm error: {0}")]
	Vm(#[from] VmError),
}

/// Parse and compile a DSL string into a Pipeline.
///
/// # Example
///
/// ```ignore
/// use reifydb_vm::dsl::{parse_pipeline, SourceRegistry};
///
/// let result = parse_pipeline(
///     "scan users | filter age > 21 | select [name] | take 10",
///     &registry
/// )?;
/// ```
pub fn parse_pipeline(source: &str, sources: &dyn SourceRegistry) -> Result<Pipeline, DslError> {
	let mut lexer = Lexer::new(source);
	let tokens = lexer.tokenize()?;

	let mut parser = Parser::new(tokens);
	let ast = parser.parse()?;

	let compiler = DslCompiler::new(sources);
	let pipeline = compiler.compile(&ast)?;

	Ok(pipeline)
}

/// Compile a DSL script to bytecode Program.
///
/// This is the first step of bytecode execution. The returned Program
/// can be executed by creating a VmState and calling execute().
pub fn compile_script(source: &str) -> Result<Program, DslError> {
	let mut lexer = Lexer::new(source);
	let tokens = lexer.tokenize()?;

	let mut parser = Parser::new(tokens);
	let ast = parser.parse_program()?;

	let compiler = BytecodeCompiler::new();
	let program = compiler.compile(ast)?;

	Ok(program)
}

/// Execute a DSL script with full scripting support using bytecode execution.
///
/// This function:
/// 1. Parses the source into an AST (supporting let, def, if statements)
/// 2. Compiles the AST to bytecode
/// 3. Executes the bytecode in the VM
/// 4. Returns the resulting pipeline
///
/// # Example
///
/// ```ignore
/// use reifydb_vm::dsl::execute_script;
///
/// let pipeline = execute_script(
///     r#"
///         let $adults = scan users | filter age >= 18
///         $adults | select [name, email] | take 10
///     "#,
///     registry.clone(),
///     &mut transaction
/// ).await?;
/// ```
pub async fn execute_script<'a>(
	source: &str,
	sources: Arc<dyn crate::source::SourceRegistry + Send + Sync>,
	rx: &mut StandardTransaction<'a>,
) -> Result<Option<Pipeline>, DslError> {
	let program = compile_script(source)?;
	let program_arc = Arc::new(program);

	// Create subquery executor for expression evaluation
	let subquery_executor =
		Arc::new(crate::expr::RuntimeSubqueryExecutor::new(program_arc.clone(), sources.clone()));

	let context = Arc::new(VmContext::with_subquery_executor(sources, subquery_executor));
	let mut vm = VmState::new(program_arc, context);
	let result = vm.execute(rx).await?;
	Ok(result)
}

/// Execute a DSL script using only in-memory sources (for testing).
///
/// This variant doesn't require a transaction and only works when
/// all sources are registered in the InMemorySourceRegistry.
pub async fn execute_script_memory(
	source: &str,
	sources: Arc<dyn crate::source::SourceRegistry + Send + Sync>,
) -> Result<Option<Pipeline>, DslError> {
	let program = compile_script(source)?;
	let program_arc = Arc::new(program);

	// Create subquery executor for expression evaluation
	let subquery_executor =
		Arc::new(crate::expr::RuntimeSubqueryExecutor::new(program_arc.clone(), sources.clone()));

	let context = Arc::new(VmContext::with_subquery_executor(sources, subquery_executor));
	let mut vm = VmState::new(program_arc, context);
	let result = vm.execute_memory().await?;
	Ok(result)
}
