// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_sdk::marshal::wasm::{marshal_columns_to_bytes, unmarshal_columns_from_bytes};
use reifydb_type::Result;

use super::{Transform, context::TransformContext};
use crate::loader::wasm::invoke_wasm_module;

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
