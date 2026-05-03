// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{fs, path::Path};

use reifydb_sdk::error::FFIError;
use reifydb_type::Result;

use super::{registry::Transforms, wasm::WasmTransform};

pub fn load_transforms_from_dir(dir: &Path) -> Result<Transforms> {
	let entries = fs::read_dir(dir).map_err(|e| {
		FFIError::Other(format!("Failed to read WASM transform directory {}: {}", dir.display(), e))
	})?;

	let mut builder = Transforms::builder();

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
		builder = builder
			.register(&name, move || WasmTransform::new(name_for_closure.clone(), wasm_bytes.clone()));
	}

	Ok(builder.configure())
}
