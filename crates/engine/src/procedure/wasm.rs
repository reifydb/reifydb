// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM procedure implementation that executes WebAssembly modules as stored procedures

use postcard::to_stdvec;
use reifydb_core::value::column::columns::Columns;
use reifydb_sdk::{error::FFIError, marshal::wasm::unmarshal_columns_from_bytes};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::Result;
use reifydb_wasm::{Engine, SpawnBinary, module::value::Value, source};

use super::{Procedure, context::ProcedureContext};

/// WASM procedure that loads and executes a `.wasm` module.
///
/// Each WASM module must export:
/// - `alloc(size: i32) -> i32` — allocate `size` bytes, return pointer
/// - `dealloc(ptr: i32, size: i32)` — free memory
/// - `procedure(params_ptr: i32, params_len: i32) -> i32` — pointer to output (first 4 bytes at output pointer = output
///   length as LE u32)
pub struct WasmProcedure {
	name: String,
	wasm_bytes: Vec<u8>,
}

impl WasmProcedure {
	pub fn new(name: impl Into<String>, wasm_bytes: Vec<u8>) -> Self {
		Self {
			name: name.into(),
			wasm_bytes,
		}
	}

	pub fn name(&self) -> &str {
		&self.name
	}
}

// SAFETY: WasmProcedure only holds inert data (name + bytes).
// A fresh Engine is created per invocation, so no shared mutable state.
unsafe impl Send for WasmProcedure {}
unsafe impl Sync for WasmProcedure {}

impl Procedure for WasmProcedure {
	fn call(&self, ctx: &ProcedureContext, _tx: &mut Transaction<'_>) -> Result<Columns> {
		let params_bytes = to_stdvec(ctx.params).map_err(|e| {
			FFIError::Other(format!("WASM procedure '{}' failed to serialize params: {}", self.name, e))
		})?;

		let mut engine = Engine::default();
		engine.spawn(source::binary::bytes(&self.wasm_bytes)).map_err(|e| {
			FFIError::Other(format!("WASM procedure '{}' failed to load: {:?}", self.name, e))
		})?;

		// Allocate space in WASM linear memory
		let alloc_result = engine.invoke("alloc", &[Value::I32(params_bytes.len() as i32)]).map_err(|e| {
			FFIError::Other(format!("WASM procedure '{}' alloc failed: {:?}", self.name, e))
		})?;

		let params_ptr = match alloc_result.first() {
			Some(Value::I32(v)) => *v,
			_ => {
				return Err(FFIError::Other(format!(
					"WASM procedure '{}': alloc returned unexpected result",
					self.name
				))
				.into());
			}
		};

		// Write params data into WASM memory
		engine.write_memory(params_ptr as usize, &params_bytes).map_err(|e| {
			FFIError::Other(format!("WASM procedure '{}' write_memory failed: {:?}", self.name, e))
		})?;

		// Call procedure
		let result = engine
			.invoke("procedure", &[Value::I32(params_ptr), Value::I32(params_bytes.len() as i32)])
			.map_err(|e| {
				FFIError::Other(format!(
					"WASM procedure '{}' procedure call failed: {:?}",
					self.name, e
				))
			})?;

		let output_ptr = match result.first() {
			Some(Value::I32(v)) => *v as usize,
			_ => {
				return Err(FFIError::Other(format!(
					"WASM procedure '{}': procedure returned unexpected result",
					self.name
				))
				.into());
			}
		};

		// Read output length (first 4 bytes at output_ptr)
		let len_bytes = engine.read_memory(output_ptr, 4).map_err(|e| {
			FFIError::Other(format!("WASM procedure '{}' read output length failed: {:?}", self.name, e))
		})?;

		let output_len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;

		// Read full output data
		let output_bytes = engine.read_memory(output_ptr + 4, output_len).map_err(|e| {
			FFIError::Other(format!("WASM procedure '{}' read output data failed: {:?}", self.name, e))
		})?;

		Ok(unmarshal_columns_from_bytes(&output_bytes))
	}
}
