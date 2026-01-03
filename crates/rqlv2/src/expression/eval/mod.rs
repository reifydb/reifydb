// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Evaluation context for compiled expressions.

mod context;
mod value;

pub use context::EvalContext;
use reifydb_core::value::column::{ColumnData, Columns};
pub use value::EvalValue;

use super::types::EvalError;

/// Trait for calling script functions from expression evaluation.
///
/// This trait is defined in rqlv2 (lower-level crate) and implemented
/// by the VM (higher-level crate) to avoid circular dependencies.
/// The VM implementation executes bytecode for the script function.
pub trait ScriptFunctionCaller: Send + Sync {
	/// Call a script function by name with columnar arguments.
	///
	/// # Arguments
	/// * `name` - The name of the script function to call
	/// * `args` - Columnar arguments (one column per parameter)
	/// * `row_count` - Number of rows to produce in the result
	///
	/// # Returns
	/// Columnar result data, broadcast to `row_count` rows if the function
	/// returns a scalar value.
	fn call(&self, name: &str, args: &Columns, row_count: usize) -> Result<ColumnData, EvalError>;
}
