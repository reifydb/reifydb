// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! WASM function loader — scans a directory for `.wasm` files and registers scalar functions

use std::{fs, path::Path};

use reifydb_routine::function::registry::{Functions, FunctionsConfigurator};
use reifydb_sdk::error::FFIError;
use reifydb_type::Result;

use super::wasm::WasmScalarFunction;

/// Scan a directory for `.wasm` files and register each as a `WasmScalarFunction` into the given
/// `FunctionsConfigurator`, returning the updated builder.
pub fn register_wasm_scalar_functions_from_dir(
	dir: &Path,
	mut builder: FunctionsConfigurator,
) -> Result<FunctionsConfigurator> {
	let entries = fs::read_dir(dir).map_err(|e| {
		FFIError::Other(format!("Failed to read WASM scalar function directory {}: {}", dir.display(), e))
	})?;

	for entry in entries {
		let entry = entry.map_err(|e| FFIError::Other(format!("Failed to read directory entry: {}", e)))?;
		let path = entry.path();

		if path.extension().and_then(|s| s.to_str()) != Some("wasm") {
			continue;
		}

		let name = match path.file_stem().and_then(|s| s.to_str()) {
			Some(n) => n.to_string(),
			None => continue,
		};

		let wasm_bytes = fs::read(&path)
			.map_err(|e| FFIError::Other(format!("Failed to read WASM file {}: {}", path.display(), e)))?;

		let name_for_closure = name.clone();
		builder = builder.register_scalar(&name, move || {
			WasmScalarFunction::new(name_for_closure.clone(), wasm_bytes.clone())
		});
	}

	Ok(builder)
}

/// Scan a directory for `.wasm` files and return a `Functions` registry with scalar functions.
pub fn load_wasm_scalar_functions_from_dir(dir: &Path) -> Result<Functions> {
	Ok(register_wasm_scalar_functions_from_dir(dir, Functions::builder())?.configure())
}
