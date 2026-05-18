// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_wasm::{Engine, SpawnBinary, module::value::Value, source};

use crate::error::ExtensionError;

pub fn invoke_wasm_module(
	wasm_bytes: &[u8],
	function_name: &str,
	input_bytes: &[u8],
	label: &str,
) -> Result<Vec<u8>, ExtensionError> {
	let mut engine = Engine::default();
	engine.spawn(source::binary::bytes(wasm_bytes))
		.map_err(|e| ExtensionError::WasmLoad(format!("{} failed to load: {:?}", label, e)))?;

	let alloc_result = engine
		.invoke("alloc", &[Value::I32(input_bytes.len() as i32)])
		.map_err(|e| ExtensionError::Invocation(format!("{} alloc failed: {:?}", label, e)))?;

	let input_ptr = match alloc_result.first() {
		Some(Value::I32(v)) => *v,
		_ => {
			return Err(ExtensionError::Invocation(format!("{}: alloc returned unexpected result", label)));
		}
	};

	engine.write_memory(input_ptr as usize, input_bytes)
		.map_err(|e| ExtensionError::Invocation(format!("{} write_memory failed: {:?}", label, e)))?;

	let result = engine
		.invoke(function_name, &[Value::I32(input_ptr), Value::I32(input_bytes.len() as i32)])
		.map_err(|e| ExtensionError::Invocation(format!("{} {} call failed: {:?}", label, function_name, e)))?;

	let output_ptr = match result.first() {
		Some(Value::I32(v)) => *v as usize,
		_ => {
			return Err(ExtensionError::Invocation(format!(
				"{}: {} returned unexpected result",
				label, function_name
			)));
		}
	};

	let len_bytes = engine
		.read_memory(output_ptr, 4)
		.map_err(|e| ExtensionError::Invocation(format!("{} read output length failed: {:?}", label, e)))?;

	let output_len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;

	engine.read_memory(output_ptr + 4, output_len)
		.map_err(|e| ExtensionError::Invocation(format!("{} read output data failed: {:?}", label, e)))
}
