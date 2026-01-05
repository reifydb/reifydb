// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Script function caller - executes bytecode for script functions.

use std::sync::Arc;

use reifydb_core::value::column::{ColumnData, Columns};
use reifydb_rqlv2::{
	bytecode::CompiledProgram,
	expression::{EvalError, ScriptFunctionCaller},
};
use reifydb_type::Value;

use crate::vmcore::{VmContext, VmState};

/// Executes script functions by running their bytecode.
///
/// This struct implements `ScriptFunctionCaller` (defined in rqlv2) and is used
/// to bridge the expression evaluator with the bytecode VM.
pub struct BytecodeScriptCaller {
	program: CompiledProgram,
}

impl BytecodeScriptCaller {
	/// Create a new bytecode script caller.
	pub fn new(program: CompiledProgram) -> Self {
		Self {
			program,
		}
	}
}

impl ScriptFunctionCaller for BytecodeScriptCaller {
	fn call(&self, name: &str, _args: &Columns, row_count: usize) -> Result<ColumnData, EvalError> {
		// Find the script function by name
		let func = self.program.script_functions.iter().find(|f| f.name == name).ok_or_else(|| {
			EvalError::UnsupportedOperation {
				operation: format!("undefined script function '{}'", name),
			}
		})?;

		// Create a new VM state to execute the function
		let context = Arc::new(VmContext::new());
		let mut vm = VmState::new(self.program.clone(), context);

		// Set IP to the function's bytecode offset
		vm.ip = func.bytecode_offset;

		// Execute synchronously until return
		// For simple functions like `fn get_min_age() { 20 }`, this is a few instructions
		let result = execute_until_return(&mut vm)?;

		// Broadcast scalar result to column
		Ok(broadcast_value_to_column(result, row_count))
	}
}

/// Execute the VM until a return is encountered at the top level.
fn execute_until_return(vm: &mut VmState) -> Result<Value, EvalError> {
	use tokio::{runtime::Handle, task::block_in_place};

	use super::state::OperandValue;

	let handle = Handle::current();

	// Execute steps until we get a result
	loop {
		// We need a transaction for execution, but for pure functions we don't need one.
		// Use block_in_place to allow blocking from within async context.
		let result = block_in_place(|| handle.block_on(vm.step(None))).map_err(|e| {
			EvalError::UnsupportedOperation {
				operation: format!("bytecode execution error: {:?}", e),
			}
		})?;

		match result {
			super::DispatchResult::Continue => continue,
			super::DispatchResult::Halt => {
				// Function returned, get value from operand stack
				let operand = vm.pop_operand().map_err(|e| EvalError::UnsupportedOperation {
					operation: format!("script function returned no value: {:?}", e),
				})?;

				// Convert OperandValue to Value
				match operand {
					OperandValue::Scalar(v) => return Ok(v),
					_ => {
						return Err(EvalError::UnsupportedOperation {
							operation: "script function returned non-scalar value"
								.to_string(),
						});
					}
				}
			}
			super::DispatchResult::Yield(_) => {
				// Script functions shouldn't yield pipelines
				return Err(EvalError::UnsupportedOperation {
					operation: "script function yielded a pipeline (expected scalar)".to_string(),
				});
			}
		}
	}
}

/// Broadcast a scalar value to a column with the given row count.
fn broadcast_value_to_column(value: Value, row_count: usize) -> ColumnData {
	match value {
		Value::Undefined => ColumnData::undefined(row_count),
		Value::Boolean(v) => ColumnData::bool(vec![v; row_count]),
		Value::Int8(v) => ColumnData::int8(vec![v; row_count]),
		Value::Float8(v) => ColumnData::float8(vec![f64::from(v); row_count]),
		Value::Utf8(s) => ColumnData::utf8(std::iter::repeat(s.clone()).take(row_count).collect::<Vec<_>>()),
		// For other types, return undefined for now
		_ => ColumnData::undefined(row_count),
	}
}
