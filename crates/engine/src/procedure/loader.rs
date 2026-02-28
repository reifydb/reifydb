#![cfg(reifydb_target = "native")]
// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FFI procedure dynamic library loader

use std::{
	collections::HashMap,
	path::{Path, PathBuf},
	sync::{OnceLock, RwLock},
};

use libloading::{Library, Symbol};
use reifydb_abi::{
	constants::CURRENT_API,
	data::buffer::BufferFFI,
	procedure::{
		descriptor::ProcedureDescriptorFFI,
		types::{PROCEDURE_MAGIC, ProcedureCreateFnFFI, ProcedureMagicFnFFI},
	},
};
use reifydb_sdk::error::{FFIError, Result as FFIResult};

use super::{
	ffi::NativeProcedureFFI,
	registry::{Procedures, ProceduresBuilder},
};

/// Extract a UTF-8 string from a BufferFFI
///
/// # Safety
/// The buffer must contain valid UTF-8 data and the pointer must be valid for the given length
unsafe fn buffer_to_string(buffer: &BufferFFI) -> String {
	if buffer.ptr.is_null() || buffer.len == 0 {
		return String::new();
	}
	let slice = unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) };
	std::str::from_utf8(slice).unwrap_or("<invalid UTF-8>").to_string()
}

/// Global singleton FFI procedure loader
static GLOBAL_FFI_PROCEDURE_LOADER: OnceLock<RwLock<ProcedureLoader>> = OnceLock::new();

/// Get the global FFI procedure loader
pub fn ffi_procedure_loader() -> &'static RwLock<ProcedureLoader> {
	GLOBAL_FFI_PROCEDURE_LOADER.get_or_init(|| RwLock::new(ProcedureLoader::new()))
}

/// FFI procedure loader for dynamic libraries
pub struct ProcedureLoader {
	/// Loaded libraries mapped by path
	loaded_libraries: HashMap<PathBuf, Library>,
	/// Map of procedure names to library paths for quick lookup
	procedure_paths: HashMap<String, PathBuf>,
}

impl ProcedureLoader {
	fn new() -> Self {
		Self {
			loaded_libraries: HashMap::new(),
			procedure_paths: HashMap::new(),
		}
	}

	pub fn load_procedure_library(&mut self, path: &Path) -> FFIResult<bool> {
		if !self.loaded_libraries.contains_key(path) {
			let lib = unsafe {
				Library::new(path).map_err(|e| {
					FFIError::Other(format!("Failed to load library {}: {}", path.display(), e))
				})?
			};
			self.loaded_libraries.insert(path.to_path_buf(), lib);
		}

		let library = self.loaded_libraries.get(path).unwrap();

		let magic_result: Result<Symbol<ProcedureMagicFnFFI>, _> =
			unsafe { library.get(b"ffi_procedure_magic\0") };

		match magic_result {
			Ok(magic_fn) => {
				let magic = magic_fn();
				Ok(magic == PROCEDURE_MAGIC)
			}
			Err(_) => {
				self.loaded_libraries.remove(path);
				Ok(false)
			}
		}
	}

	fn get_descriptor(&self, library: &Library) -> FFIResult<ProcedureDescriptorFFI> {
		unsafe {
			let get_descriptor: Symbol<extern "C" fn() -> *const ProcedureDescriptorFFI> =
				library.get(b"ffi_procedure_get_descriptor\0").map_err(|e| {
					FFIError::Other(format!("Failed to find ffi_procedure_get_descriptor: {}", e))
				})?;

			let descriptor_ptr = get_descriptor();
			if descriptor_ptr.is_null() {
				return Err(FFIError::Other("Descriptor is null".to_string()));
			}

			Ok(ProcedureDescriptorFFI {
				api: (*descriptor_ptr).api,
				name: (*descriptor_ptr).name,
				version: (*descriptor_ptr).version,
				description: (*descriptor_ptr).description,
				vtable: (*descriptor_ptr).vtable,
			})
		}
	}

	fn validate_and_register(
		&mut self,
		descriptor: &ProcedureDescriptorFFI,
		path: &Path,
	) -> FFIResult<(String, u32)> {
		if descriptor.api != CURRENT_API {
			return Err(FFIError::Other(format!(
				"API version mismatch: expected {}, got {}",
				CURRENT_API, descriptor.api
			)));
		}

		let name = unsafe { buffer_to_string(&descriptor.name) };
		self.procedure_paths.insert(name.clone(), path.to_path_buf());

		Ok((name, descriptor.api))
	}

	/// Register a procedure library without instantiating it
	pub fn register_procedure(&mut self, path: &Path) -> FFIResult<Option<LoadedProcedureInfo>> {
		if !self.load_procedure_library(path)? {
			return Ok(None);
		}

		let library = self.loaded_libraries.get(path).unwrap();
		let descriptor = self.get_descriptor(library)?;
		let (name, api) = self.validate_and_register(&descriptor, path)?;

		let info = unsafe {
			LoadedProcedureInfo {
				name,
				library_path: path.to_path_buf(),
				api,
				version: buffer_to_string(&descriptor.version),
				description: buffer_to_string(&descriptor.description),
			}
		};

		Ok(Some(info))
	}

	/// Load a procedure from a dynamic library
	pub fn load_procedure(&mut self, path: &Path, config: &[u8]) -> FFIResult<Option<NativeProcedureFFI>> {
		if !self.load_procedure_library(path)? {
			return Ok(None);
		}

		let descriptor = {
			let library = self.loaded_libraries.get(path).unwrap();
			self.get_descriptor(library)?
		};

		self.validate_and_register(&descriptor, path)?;

		let library = self.loaded_libraries.get(path).unwrap();
		let create_fn: ProcedureCreateFnFFI = unsafe {
			let create_symbol: Symbol<ProcedureCreateFnFFI> = library
				.get(b"ffi_procedure_create\0")
				.map_err(|e| FFIError::Other(format!("Failed to find ffi_procedure_create: {}", e)))?;

			*create_symbol
		};

		let instance = create_fn(config.as_ptr(), config.len());
		if instance.is_null() {
			return Err(FFIError::Other("Failed to create procedure instance".to_string()));
		}

		Ok(Some(NativeProcedureFFI::new(descriptor, instance)))
	}

	/// Create a procedure instance from an already loaded library by name
	pub fn create_procedure_by_name(&mut self, name: &str, config: &[u8]) -> FFIResult<NativeProcedureFFI> {
		let path = self
			.procedure_paths
			.get(name)
			.ok_or_else(|| FFIError::Other(format!("Procedure not found: {}", name)))?
			.clone();

		self.load_procedure(&path, config)?
			.ok_or_else(|| FFIError::Other(format!("Procedure library no longer valid: {}", name)))
	}

	/// Check if a procedure name is registered
	pub fn has_procedure(&self, name: &str) -> bool {
		self.procedure_paths.contains_key(name)
	}
}

/// Information about a loaded FFI procedure
#[derive(Debug, Clone)]
pub struct LoadedProcedureInfo {
	pub name: String,
	pub library_path: PathBuf,
	pub api: u32,
	pub version: String,
	pub description: String,
}

impl Default for ProcedureLoader {
	fn default() -> Self {
		Self::new()
	}
}

impl Drop for ProcedureLoader {
	fn drop(&mut self) {
		self.loaded_libraries.clear();
	}
}

/// Scan a directory for FFI procedure shared libraries and register them
/// onto an existing `ProceduresBuilder`.
pub fn register_procedures_from_dir(dir: &Path, mut builder: ProceduresBuilder) -> FFIResult<ProceduresBuilder> {
	let loader = ffi_procedure_loader();
	let mut loader_guard = loader.write().unwrap();

	let mut names = Vec::new();

	let entries = std::fs::read_dir(dir)
		.map_err(|e| FFIError::Other(format!("Failed to read directory {}: {}", dir.display(), e)))?;

	for entry in entries {
		let entry = entry.map_err(|e| FFIError::Other(format!("Failed to read directory entry: {}", e)))?;
		let path = entry.path();
		let ext = path.extension().and_then(|s| s.to_str());

		if ext == Some("so") || ext == Some("dylib") {
			match loader_guard.register_procedure(&path) {
				Ok(Some(info)) => {
					names.push(info.name);
				}
				Ok(None) => {
					// Not a valid procedure library, skip
				}
				Err(e) => {
					eprintln!(
						"Warning: Failed to register procedure from {}: {}",
						path.display(),
						e
					);
				}
			}
		}
	}

	drop(loader_guard);

	for name in names {
		let name_clone = name.clone();
		builder = builder.with_procedure(&name, move || {
			let loader = ffi_procedure_loader();
			let mut loader_guard = loader.write().unwrap();
			loader_guard.create_procedure_by_name(&name_clone, &[]).unwrap()
		});
	}

	Ok(builder)
}

/// Scan a directory for FFI procedure shared libraries, register them,
/// and return a `Procedures` registry with factory functions for each.
pub fn load_procedures_from_dir(dir: &Path) -> FFIResult<Procedures> {
	Ok(register_procedures_from_dir(dir, Procedures::builder())?.build())
}
