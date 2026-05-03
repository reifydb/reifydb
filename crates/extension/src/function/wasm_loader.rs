// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{fs, path::Path, sync::Arc};

use reifydb_routine::routine::registry::RoutinesConfigurator;
use reifydb_sdk::error::FFIError;
use reifydb_type::Result;

use super::wasm::WasmScalarFunction;

pub fn register_wasm_scalar_functions_from_dir(
	dir: &Path,
	mut builder: RoutinesConfigurator,
) -> Result<RoutinesConfigurator> {
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

		builder = builder.register_function(Arc::new(WasmScalarFunction::new(name, wasm_bytes)));
	}

	Ok(builder)
}
