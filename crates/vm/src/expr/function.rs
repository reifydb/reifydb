// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Function executor for calling VM functions from expressions.

use std::{collections::HashMap, sync::Arc};

use reifydb_core::value::column::{ColumnData, Columns};

use crate::error::Result;

/// Context for calling a VM function with columnar arguments.
pub struct VmFunctionContext<'a> {
	/// Argument columns (one column per function parameter).
	pub columns: &'a Columns,
	/// Number of rows to process.
	pub row_count: usize,
}

/// Type alias for a VM scalar function that operates on columns.
/// Takes argument columns and returns result column data.
pub type VmScalarFn = Arc<dyn Fn(VmFunctionContext) -> Result<ColumnData> + Send + Sync>;

/// Executor for VM functions that can be called from expressions.
#[derive(Clone, Default)]
pub struct VmFunctionExecutor {
	/// Map from function name to callable function.
	functions: HashMap<String, VmScalarFn>,
}

impl VmFunctionExecutor {
	/// Create a new empty function executor.
	pub fn new() -> Self {
		Self {
			functions: HashMap::new(),
		}
	}

	/// Register a function with the executor.
	pub fn register(&mut self, name: String, func: VmScalarFn) {
		self.functions.insert(name, func);
	}

	/// Check if a function is registered.
	pub fn has_function(&self, name: &str) -> bool {
		self.functions.contains_key(name)
	}

	/// Call a function by name with the given arguments.
	pub fn call(&self, name: &str, ctx: VmFunctionContext) -> Result<ColumnData> {
		let func = self.functions.get(name).ok_or_else(|| crate::error::VmError::UndefinedFunction {
			name: name.to_string(),
		})?;
		func(ctx)
	}

	/// Get the number of registered functions.
	pub fn len(&self) -> usize {
		self.functions.len()
	}

	/// Check if the executor has no functions.
	pub fn is_empty(&self) -> bool {
		self.functions.is_empty()
	}
}

impl std::fmt::Debug for VmFunctionExecutor {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("VmFunctionExecutor")
			.field("functions", &self.functions.keys().collect::<Vec<_>>())
			.finish()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::Column;
	use reifydb_type::Fragment;

	use super::*;

	#[test]
	fn test_function_executor_new() {
		let executor = VmFunctionExecutor::new();
		assert!(executor.is_empty());
	}

	#[test]
	fn test_function_executor_register() {
		let mut executor = VmFunctionExecutor::new();
		executor.register("test".to_string(), Arc::new(|ctx| Ok(ColumnData::int8(vec![42; ctx.row_count]))));
		assert!(executor.has_function("test"));
		assert!(!executor.has_function("other"));
		assert_eq!(executor.len(), 1);
	}

	#[test]
	fn test_function_executor_call() {
		let mut executor = VmFunctionExecutor::new();
		executor.register(
			"double".to_string(),
			Arc::new(|ctx| {
				// Double the first argument column
				if ctx.columns.is_empty() {
					return Ok(ColumnData::int8(Vec::new()));
				}
				let col = &ctx.columns[0];
				match col.data() {
					ColumnData::Int8(container) => {
						let result: Vec<i64> = (0..ctx.row_count)
							.map(|i| container.get(i).copied().unwrap_or(0) * 2)
							.collect();
						Ok(ColumnData::int8(result))
					}
					_ => Ok(ColumnData::int8(Vec::new())),
				}
			}),
		);

		let arg_col = Column::new(Fragment::internal("x"), ColumnData::int8(vec![1, 2, 3]));
		let args = Columns::new(vec![arg_col]);
		let ctx = VmFunctionContext {
			columns: &args,
			row_count: 3,
		};

		let result = executor.call("double", ctx).unwrap();
		match result {
			ColumnData::Int8(container) => {
				assert_eq!(container.get(0), Some(&2));
				assert_eq!(container.get(1), Some(&4));
				assert_eq!(container.get(2), Some(&6));
			}
			_ => panic!("Expected Int8 column"),
		}
	}
}
