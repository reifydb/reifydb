// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! WASM scalar function implementation that executes WebAssembly modules as scalar functions

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine::routine::{
	Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_sdk::marshal::wasm::{marshal_columns_to_bytes, unmarshal_columns_from_bytes};
use reifydb_type::{fragment::Fragment, value::r#type::Type};

use crate::loader::wasm::invoke_wasm_module;

/// WASM scalar function that loads and executes a `.wasm` module.
///
/// Each WASM module must export:
/// - `alloc(size: i32) -> i32` - allocate `size` bytes, return pointer
/// - `dealloc(ptr: i32, size: i32)` - free memory
/// - `scalar(input_ptr: i32, input_len: i32) -> i32` - pointer to output (first 4 bytes at output pointer = output
///   length as LE u32)
///
/// Input: the context's `columns` marshalled as flat binary.
/// Output: flat binary representing a single-column `Columns`, from which
///   the first column's `ColumnBuffer` is extracted.
pub struct WasmScalarFunction {
	info: RoutineInfo,
	wasm_bytes: Vec<u8>,
}

impl WasmScalarFunction {
	pub fn new(name: impl Into<String>, wasm_bytes: Vec<u8>) -> Self {
		let name = name.into();
		Self {
			info: RoutineInfo::new(&name),
			wasm_bytes,
		}
	}

	pub fn name(&self) -> &str {
		&self.info.name
	}

	fn err(&self, reason: impl Into<String>) -> RoutineError {
		RoutineError::FunctionExecutionFailed {
			function: Fragment::internal(&self.info.name),
			reason: reason.into(),
		}
	}
}

// SAFETY: WasmScalarFunction only holds inert data (name + bytes).
// A fresh Engine is created per invocation, so no shared mutable state.
unsafe impl Send for WasmScalarFunction {}
unsafe impl Sync for WasmScalarFunction {}

impl<'a> Routine<FunctionContext<'a>> for WasmScalarFunction {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let input_bytes = marshal_columns_to_bytes(args);
		let label = format!("WASM scalar function '{}'", self.info.name);

		let output_bytes = invoke_wasm_module(&self.wasm_bytes, "scalar", &input_bytes, &label)
			.map_err(|e| self.err(e.to_string()))?;

		// Unmarshal as Columns and extract the first column's data
		let output_columns = unmarshal_columns_from_bytes(&output_bytes);

		match output_columns.first() {
			Some(col) => {
				let data = col.data().clone();
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), data)]))
			}
			None => {
				let data = ColumnBuffer::none_typed(Type::Any, args.row_count());
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), data)]))
			}
		}
	}
}

impl Function for WasmScalarFunction {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
