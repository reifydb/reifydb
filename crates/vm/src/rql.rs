// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! RQL v2 integration for the VM.
//!
//! This module provides wrapper functions around RQLv2's compilation pipeline,
//! making it easy to compile and execute RQL scripts with catalog integration.
//!
//! # Example
//!
//! ```ignore
//! use reifydb_vm::rql::{compile_script, execute_program};
//!
//! let program = compile_script(
//!     r#"
//!         let $users = scan users | filter age > 18
//!         $users | select [name, email]
//!     "#,
//!     &catalog,
//!     &mut tx
//! ).await?;
//!
//! let pipeline = execute_program(program, registry, catalog, &mut tx).await?;
//! ```

use std::sync::Arc;

use bumpalo::Bump;
use reifydb_catalog::Catalog;
use reifydb_rqlv2::{
	ast::parse::{ParseError, Parser},
	bytecode::{
		CompiledProgram,
		compile::{CompileError, PlanCompiler},
	},
	plan::compile::{PlanError, plan},
	token::{LexError, tokenize},
};
use reifydb_transaction::{StandardCommandTransaction, StandardTransaction};
use thiserror::Error;

use crate::{
	error::VmError,
	pipeline::Pipeline,
	vmcore::{VmContext, VmState},
};

/// Combined error type for RQL operations.
#[derive(Debug, Error)]
pub enum RqlError {
	/// Lexer error during tokenization.
	#[error("lexer error: {0}")]
	Lex(#[from] LexError),

	/// Parse errors (can be multiple).
	#[error("parse errors: {0:?}")]
	Parse(Vec<ParseError>),

	/// Plan compilation error.
	#[error("plan error: {0}")]
	Plan(#[from] PlanError),

	/// Bytecode compilation error.
	#[error("compile error: {0}")]
	Compile(#[from] CompileError),

	/// VM execution error.
	#[error("vm error: {0}")]
	Vm(#[from] VmError),

	/// Empty program (no statements to execute).
	#[error("empty program")]
	EmptyProgram,
}

/// Compile an RQL script to bytecode with catalog access.
///
/// This function performs the complete RQLv2 compilation pipeline:
/// 1. Tokenize the source
/// 2. Parse into AST
/// 3. Compile AST to Plan (requires catalog for table/view lookups)
/// 4. Compile Plan to bytecode
///
/// # Arguments
///
/// * `source` - The RQL source code
/// * `catalog` - Catalog for resolving tables, views, etc.
/// * `tx` - Transaction for catalog access
///
/// # Returns
///
/// A `CompiledProgram` ready for execution, or an `RqlError` on failure.
///
/// # Example
///
/// ```ignore
/// let program = compile_script(
///     "let $users = scan users | filter age > 18\n$users",
///     &catalog,
///     &mut tx
/// ).await?;
/// ```
pub async fn compile_script(
	source: &str,
	catalog: &Catalog,
	tx: &mut StandardCommandTransaction,
) -> Result<Arc<CompiledProgram>, RqlError> {
	// Create bump allocator for AST (transient - dropped after compilation)
	let bump = Bump::new();

	// Step 1: Tokenize
	let token_result = tokenize(source, &bump)?;

	// Step 2: Parse to AST
	let parse_result = Parser::new(&bump, &token_result.tokens, source).parse();

	// Check for parse errors
	if !parse_result.errors.is_empty() {
		return Err(RqlError::Parse(parse_result.errors.to_vec()));
	}

	let program_ast = parse_result.program;

	// Step 3: Compile AST to Plan (requires catalog for table resolution)
	let plans = plan(&bump, catalog, tx, program_ast).await?;

	if plans.is_empty() {
		return Err(RqlError::EmptyProgram);
	}

	// Step 4: Compile all plans to bytecode
	// For multi-statement programs (e.g., let bindings + query), we need to compile all plans
	// sequentially so that variable declarations in earlier statements are available in later ones
	let compiled_program = PlanCompiler::compile_all(plans)?;

	Ok(Arc::new(compiled_program))
}

/// Execute a compiled bytecode program with catalog access.
///
/// This function creates a VM context, initializes the VM state, and executes
/// the bytecode program with access to the catalog.
///
/// # Arguments
///
/// * `program` - The compiled bytecode program
/// * `catalog` - Catalog for table/view resolution
/// * `tx` - Transaction for catalog access and execution
///
/// # Returns
///
/// An optional `Pipeline` (Some if the program produces a result, None otherwise),
/// or an `RqlError` on failure.
///
/// # Example
///
/// ```ignore
/// let pipeline = execute_program(
///     program,
///     catalog,
///     &mut tx
/// ).await?;
/// ```
pub async fn execute_program(
	program: Arc<CompiledProgram>,
	catalog: Catalog,
	tx: &mut StandardCommandTransaction,
) -> Result<Option<Pipeline>, RqlError> {
	// Create VM context with catalog
	let context = Arc::new(VmContext::with_catalog(catalog));

	// Create VM state
	let mut vm = VmState::new(program, context);

	// Convert to StandardTransaction and execute (provides catalog access)
	let mut std_tx: StandardTransaction = tx.into();
	let result = vm.execute(&mut std_tx).await?;

	Ok(result)
}
