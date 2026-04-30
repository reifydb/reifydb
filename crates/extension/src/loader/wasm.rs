// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_wasm::{Engine, SpawnBinary, module::value::Value, source};

use crate::error::ExtensionError;

/// Execute a WASM module using the standard alloc/call/read-output protocol.
///
/// 1. Spawn module bytes into a fresh `Engine`
/// 2. Call `alloc(input_len)` to get a pointer in WASM memory
/// 3. Write input bytes to that pointer
/// 4. Call the named function with `(ptr, len)`
/// 5. Read output length (4 bytes LE u32) then output bytes
///
/// Returns the output bytes (without the 4-byte length prefix).
pub fn invoke_wasm_module(
	wasm_bytes: &[u8],
	function_name: &str,
	input_bytes: &[u8],
	label: &str,
) -> Result<Vec<u8>, ExtensionError> {
	let mut engine = Engine::default();
	engine.spawn(source::binary::bytes(wasm_bytes))
		.map_err(|e| ExtensionError::WasmLoad(format!("{} failed to load: {:?}", label, e)))?;

	// alloc
	let alloc_result = engine
		.invoke("alloc", &[Value::I32(input_bytes.len() as i32)])
		.map_err(|e| ExtensionError::Invocation(format!("{} alloc failed: {:?}", label, e)))?;

	let input_ptr = match alloc_result.first() {
		Some(Value::I32(v)) => *v,
		_ => {
			return Err(ExtensionError::Invocation(format!("{}: alloc returned unexpected result", label)));
		}
	};

	// write input
	engine.write_memory(input_ptr as usize, input_bytes)
		.map_err(|e| ExtensionError::Invocation(format!("{} write_memory failed: {:?}", label, e)))?;

	// call function
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

	// read length + output
	let len_bytes = engine
		.read_memory(output_ptr, 4)
		.map_err(|e| ExtensionError::Invocation(format!("{} read output length failed: {:?}", label, e)))?;

	let output_len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;

	engine.read_memory(output_ptr + 4, output_len)
		.map_err(|e| ExtensionError::Invocation(format!("{} read output data failed: {:?}", label, e)))
}
