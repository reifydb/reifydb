// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM transform loader — scans a directory for `.wasm` files and builds a transform registry

use std::path::Path;

use super::{registry::Transforms, wasm::WasmTransform};

/// Scan a directory for `.wasm` files, read each one, and return a `Transforms`
/// registry with factory functions that create `WasmTransform` instances.
///
/// The transform name is derived from the file stem (e.g. `my_transform.wasm` → `"my_transform"`).
pub fn load_transforms_from_dir(dir: &Path) -> reifydb_type::Result<Transforms> {
	let entries = std::fs::read_dir(dir).map_err(|e| {
		reifydb_sdk::error::FFIError::Other(format!("Failed to read WASM transform directory {}: {}", dir.display(), e))
	})?;

	let mut builder = Transforms::builder();

	for entry in entries {
		let entry = entry.map_err(|e| {
			reifydb_sdk::error::FFIError::Other(format!("Failed to read directory entry: {}", e))
		})?;
		let path = entry.path();

		if path.extension().and_then(|s| s.to_str()) != Some("wasm") {
			continue;
		}

		let name = match path.file_stem().and_then(|s| s.to_str()) {
			Some(n) => n.to_string(),
			None => continue,
		};

		let wasm_bytes = std::fs::read(&path).map_err(|e| {
			reifydb_sdk::error::FFIError::Other(format!(
				"Failed to read WASM file {}: {}",
				path.display(),
				e
			))
		})?;

		let name_for_closure = name.clone();
		builder = builder.register(&name, move || {
			WasmTransform::new(name_for_closure.clone(), wasm_bytes.clone())
		});
	}

	Ok(builder.build())
}
