// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! WASM procedure loader - scans a directory for `.wasm` files and registers procedures

use std::{fs, path::Path, sync::Arc};

use reifydb_routine::routine::RoutinesConfigurator;
use reifydb_sdk::error::FFIError;
use reifydb_type::Result;

use super::wasm::WasmProcedure;

/// Scan a directory for `.wasm` files and register each as a `WasmProcedure` into the given
/// `RoutinesConfigurator`, returning the updated builder.
///
/// The procedure name is derived from the file stem (e.g. `my_proc.wasm` → `"my_proc"`).
pub fn register_wasm_procedures_from_dir(
	dir: &Path,
	mut builder: RoutinesConfigurator,
) -> Result<RoutinesConfigurator> {
	let entries = fs::read_dir(dir).map_err(|e| {
		FFIError::Other(format!("Failed to read WASM procedure directory {}: {}", dir.display(), e))
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

		builder = builder.register_procedure(Arc::new(WasmProcedure::new(name, wasm_bytes)));
	}

	Ok(builder)
}
