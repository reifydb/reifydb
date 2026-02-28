// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM procedure loader — scans a directory for `.wasm` files and builds a procedure registry

use std::path::Path;

use reifydb_sdk::error::FFIError;
use reifydb_type::Result;

use super::{
	registry::{Procedures, ProceduresBuilder},
	wasm::WasmProcedure,
};

/// Scan a directory for `.wasm` files, read each one, and return a `Procedures`
/// registry with factory functions that create `WasmProcedure` instances.
///
/// The procedure name is derived from the file stem (e.g. `my_proc.wasm` → `"my_proc"`).
pub fn load_wasm_procedures_from_dir(dir: &Path) -> Result<Procedures> {
	Ok(register_wasm_procedures_from_dir(dir, Procedures::builder())?.build())
}

/// Scan a directory for `.wasm` files and register each as a `WasmProcedure` into the given
/// `ProceduresBuilder`, returning the updated builder.
pub fn register_wasm_procedures_from_dir(dir: &Path, mut builder: ProceduresBuilder) -> Result<ProceduresBuilder> {
	let entries = std::fs::read_dir(dir).map_err(|e| {
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

		let wasm_bytes = std::fs::read(&path)
			.map_err(|e| FFIError::Other(format!("Failed to read WASM file {}: {}", path.display(), e)))?;

		let name_for_closure = name.clone();
		builder = builder.with_procedure(&name, move || {
			WasmProcedure::new(name_for_closure.clone(), wasm_bytes.clone())
		});
	}

	Ok(builder)
}
