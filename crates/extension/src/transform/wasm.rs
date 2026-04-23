// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! WASM transform implementation that executes WebAssembly modules as columnar transforms

use reifydb_core::value::column::columns::Columns;
use reifydb_sdk::marshal::wasm::{marshal_columns_to_bytes, unmarshal_columns_from_bytes};
use reifydb_type::Result;

use super::{Transform, context::TransformContext};
use crate::loader::wasm::invoke_wasm_module;

/// WASM transform that loads and executes a `.wasm` module.
///
/// Each WASM module must export:
/// - `alloc(size: i32) -> i32` - allocate `size` bytes, return pointer
/// - `dealloc(ptr: i32, size: i32)` - free memory
/// - `transform(input_ptr: i32, input_len: i32) -> i32` - pointer to output (first 4 bytes at output pointer = output
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
	fn apply(&self, _ctx: &TransformContext, input: Columns) -> Result<Columns> {
		let input_bytes = marshal_columns_to_bytes(&input);
		let label = format!("WASM transform '{}'", self.name);

		let output_bytes = invoke_wasm_module(&self.wasm_bytes, "transform", &input_bytes, &label)?;

		Ok(unmarshal_columns_from_bytes(&output_bytes))
	}
}
