// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM transform implementation that executes WebAssembly modules as columnar transforms

use reifydb_core::value::column::columns::Columns;
use reifydb_wasm::{Engine, SpawnBinary, module::value::Value, source};

use super::{Transform, context::TransformContext};

/// WASM transform that loads and executes a `.wasm` module.
///
/// Each WASM module must export:
/// - `alloc(size: i32) -> i32` — allocate `size` bytes, return pointer
/// - `dealloc(ptr: i32, size: i32)` — free memory
/// - `transform(input_ptr: i32, input_len: i32) -> i32` — pointer to output (first 4 bytes at output pointer = output
///   length as LE u32)
pub struct WasmTransform {
	name: String,
	wasm_bytes: Vec<u8>,
}

impl WasmTransform {
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

// SAFETY: WasmTransform only holds inert data (name + bytes).
// A fresh Engine is created per invocation, so no shared mutable state.
unsafe impl Send for WasmTransform {}
unsafe impl Sync for WasmTransform {}

impl Transform for WasmTransform {
	fn apply(&self, _ctx: &TransformContext, input: Columns) -> reifydb_type::Result<Columns> {
		let input_bytes = reifydb_sdk::marshal::wasm::marshal_columns_to_bytes(&input);

		let mut engine = Engine::default();
		engine.spawn(source::binary::bytes(&self.wasm_bytes)).map_err(|e| {
			reifydb_sdk::error::FFIError::Other(format!(
				"WASM transform '{}' failed to load: {:?}",
				self.name, e
			))
		})?;

		// Allocate space in WASM linear memory
		let alloc_result = engine.invoke("alloc", &[Value::I32(input_bytes.len() as i32)]).map_err(|e| {
			reifydb_sdk::error::FFIError::Other(format!(
				"WASM transform '{}' alloc failed: {:?}",
				self.name, e
			))
		})?;

		let input_ptr = match alloc_result.first() {
			Some(Value::I32(v)) => *v,
			_ => {
				return Err(reifydb_sdk::error::FFIError::Other(format!(
					"WASM transform '{}': alloc returned unexpected result",
					self.name
				))
				.into());
			}
		};

		// Write input data into WASM memory
		engine.write_memory(input_ptr as usize, &input_bytes).map_err(|e| {
			reifydb_sdk::error::FFIError::Other(format!(
				"WASM transform '{}' write_memory failed: {:?}",
				self.name, e
			))
		})?;

		// Call transform
		let result = engine
			.invoke("transform", &[Value::I32(input_ptr), Value::I32(input_bytes.len() as i32)])
			.map_err(|e| {
				reifydb_sdk::error::FFIError::Other(format!(
					"WASM transform '{}' transform call failed: {:?}",
					self.name, e
				))
			})?;

		let output_ptr = match result.first() {
			Some(Value::I32(v)) => *v as usize,
			_ => {
				return Err(reifydb_sdk::error::FFIError::Other(format!(
					"WASM transform '{}': transform returned unexpected result",
					self.name
				))
				.into());
			}
		};

		// Read output length (first 4 bytes at output_ptr)
		let len_bytes = engine.read_memory(output_ptr, 4).map_err(|e| {
			reifydb_sdk::error::FFIError::Other(format!(
				"WASM transform '{}' read output length failed: {:?}",
				self.name, e
			))
		})?;

		let output_len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;

		// Read full output data
		let output_bytes = engine.read_memory(output_ptr + 4, output_len).map_err(|e| {
			reifydb_sdk::error::FFIError::Other(format!(
				"WASM transform '{}' read output data failed: {:?}",
				self.name, e
			))
		})?;

		Ok(reifydb_sdk::marshal::wasm::unmarshal_columns_from_bytes(&output_bytes))
	}
}
