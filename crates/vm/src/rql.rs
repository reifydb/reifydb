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
//!     &catalog.materialized,
//! )?;
//!
//! let pipeline = execute_program(program, registry, catalog, &mut tx).await?;
//! ```

use std::sync::Arc;

use reifydb_catalog::Catalog;
use reifydb_rqlv2::CompiledProgram;
use reifydb_transaction::IntoStandardTransaction;

use crate::{
	error::VmError,
	pipeline::Pipeline,
	vmcore::{VmContext, VmState},
};

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
pub async fn execute_program<T: IntoStandardTransaction>(
	program: CompiledProgram,
	catalog: Catalog,
	tx: &mut T,
) -> Result<Option<Pipeline>, VmError> {
	// Create VM context with catalog
	let context = Arc::new(VmContext::with_catalog(catalog));

	// Create VM state
	let mut vm = VmState::new(program, context);

	// Execute using the trait method
	let result = vm.execute(tx).await?;

	Ok(result)
}
