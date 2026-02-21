// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM scalar function implementation that executes WebAssembly modules as scalar functions

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{fragment::Fragment, value::r#type::Type};
use reifydb_wasm::{Engine, SpawnBinary, module::Value, source};

use super::{ScalarFunction, ScalarFunctionContext};
use crate::error::{ScalarFunctionError, ScalarFunctionResult};

/// WASM scalar function that loads and executes a `.wasm` module.
///
/// Each WASM module must export:
/// - `alloc(size: i32) -> i32` — allocate `size` bytes, return pointer
/// - `dealloc(ptr: i32, size: i32)` — free memory
/// - `scalar(input_ptr: i32, input_len: i32) -> i32` — pointer to output (first 4 bytes at output pointer = output
///   length as LE u32)
///
/// Input: the context's `columns` marshalled as flat binary.
/// Output: flat binary representing a single-column `Columns`, from which
///   the first column's `ColumnData` is extracted.
pub struct WasmScalarFunction {
	name: String,
	wasm_bytes: Vec<u8>,
}

impl WasmScalarFunction {
	pub fn new(name: impl Into<String>, wasm_bytes: Vec<u8>) -> Self {
		Self {
			name: name.into(),
			wasm_bytes,
		}
	}

	pub fn name(&self) -> &str {
		&self.name
	}

	fn err(&self, reason: impl Into<String>) -> ScalarFunctionError {
		ScalarFunctionError::ExecutionFailed {
			function: Fragment::internal(&self.name),
			reason: reason.into(),
		}
	}
}

// SAFETY: WasmScalarFunction only holds inert data (name + bytes).
// A fresh Engine is created per invocation, so no shared mutable state.
unsafe impl Send for WasmScalarFunction {}
unsafe impl Sync for WasmScalarFunction {}

impl ScalarFunction for WasmScalarFunction {
	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn scalar<'a>(&'a self, ctx: ScalarFunctionContext<'a>) -> ScalarFunctionResult<ColumnData> {
		let input_bytes = reifydb_sdk::marshal::wasm::marshal_columns_to_bytes(ctx.columns);

		let mut engine = Engine::default();
		engine.spawn(source::binary::bytes(&self.wasm_bytes))
			.map_err(|e| self.err(format!("failed to load: {:?}", e)))?;

		// Allocate space in WASM linear memory
		let alloc_result = engine
			.invoke("alloc", &[Value::I32(input_bytes.len() as i32)])
			.map_err(|e| self.err(format!("alloc failed: {:?}", e)))?;

		let input_ptr = match alloc_result.first() {
			Some(Value::I32(v)) => *v,
			_ => return Err(self.err("alloc returned unexpected result")),
		};

		// Write input data
		engine.write_memory(input_ptr as usize, &input_bytes)
			.map_err(|e| self.err(format!("write_memory failed: {:?}", e)))?;

		// Call scalar
		let result = engine
			.invoke("scalar", &[Value::I32(input_ptr), Value::I32(input_bytes.len() as i32)])
			.map_err(|e| self.err(format!("scalar call failed: {:?}", e)))?;

		let output_ptr = match result.first() {
			Some(Value::I32(v)) => *v as usize,
			_ => return Err(self.err("scalar returned unexpected result")),
		};

		// Read output length (first 4 bytes at output_ptr)
		let len_bytes = engine
			.read_memory(output_ptr, 4)
			.map_err(|e| self.err(format!("read output length failed: {:?}", e)))?;

		let output_len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;

		// Read full output data
		let output_bytes = engine
			.read_memory(output_ptr + 4, output_len)
			.map_err(|e| self.err(format!("read output data failed: {:?}", e)))?;

		// Unmarshal as Columns and extract the first column's data
		let output_columns = reifydb_sdk::marshal::wasm::unmarshal_columns_from_bytes(&output_bytes);

		match output_columns.first() {
			Some(col) => Ok(col.data().clone()),
			None => Ok(ColumnData::none_typed(Type::Any, ctx.row_count)),
		}
	}
}
