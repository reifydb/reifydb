// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! ReifyDB Query Language v2 (RQL) parser, AST, plan, and bytecode
//!
//! This crate provides:
//! - Bump-allocated tokenization via the [`token`] module
//! - Unified AST for queries and scripting via the [`ast`] module
//! - Unified execution plan via the [`plan`] module
//! - Bytecode compilation and encoding via the [`bytecode`] module
//! - Compiled expressions via the [`expression`] module
//! - RQL compilation pipeline via [`compile_script`]

pub mod ast;
pub mod bytecode;
pub mod compiler;
pub mod error;
pub mod expression;
pub mod plan;
pub mod token;

use ast::parse::parse;
use bumpalo::Bump;
use bytecode::compile::PlanCompiler;
use error::RqlError;
use reifydb_catalog::materialized::MaterializedCatalog;
use token::tokenize;

use crate::{bytecode::program::CompiledProgram, plan::compile::core::plan};

/// Compile an RQL script to bytecode.
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
/// * `catalog` - Materialized catalog for resolving tables, views, etc.
///
/// # Returns
///
/// A `CompiledProgram` ready for execution, or an `RqlError` on failure.
///
/// # Example
///
/// ```ignore
/// use reifydb_rqlv2::compile_script;
///
/// let program = compile_script(
///     "let $users = scan users | filter age > 18\n$users",
///     &catalog,
/// )?;
/// ```
pub fn compile_script(source: &str, catalog: &MaterializedCatalog) -> Result<CompiledProgram, RqlError> {
	let bump = Bump::new();

	let token_result = tokenize(source, &bump)?;
	let program = parse(&bump, &token_result.tokens, source)?;
	let plans = plan(&bump, catalog, program)?;

	if plans.is_empty() {
		return Err(RqlError::EmptyProgram);
	}

	PlanCompiler::compile(plans)
}
