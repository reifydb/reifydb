// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! FFI procedure dynamic library loader

use std::{
	collections::HashMap,
	fs,
	path::{Path, PathBuf},
	sync::{Arc, OnceLock, RwLock},
};

use libloading::Symbol;
use reifydb_abi::procedure::{
	descriptor::ProcedureDescriptorFFI,
	types::{PROCEDURE_MAGIC, ProcedureCreateFnFFI},
};
use reifydb_routine::routine::registry::RoutinesConfigurator;
use reifydb_sdk::error::{FFIError, Result as FFIResult};

use super::ffi::NativeProcedureFFI;
use crate::loader::ffi::{LibraryCache, buffer_to_string, validate_api_version};

/// Global singleton FFI procedure loader
static GLOBAL_FFI_PROCEDURE_LOADER: OnceLock<RwLock<ProcedureLoader>> = OnceLock::new();

/// Get the global FFI procedure loader
pub fn ffi_procedure_loader() -> &'static RwLock<ProcedureLoader> {
	GLOBAL_FFI_PROCEDURE_LOADER.get_or_init(|| RwLock::new(ProcedureLoader::new()))
}

/// FFI procedure loader for dynamic libraries
pub struct ProcedureLoader {
	cache: LibraryCache,
	procedure_paths: HashMap<String, PathBuf>,
}

impl ProcedureLoader {
	fn new() -> Self {
		Self {
			cache: LibraryCache::new(),
			procedure_paths: HashMap::new(),
		}
	}

	pub fn load_procedure_library(&mut self, path: &Path) -> FFIResult<bool> {
		self.cache
			.check_magic(path, b"ffi_procedure_magic\0", PROCEDURE_MAGIC)
			.map_err(|e| FFIError::Other(e.to_string()))
	}

	fn get_descriptor(&self, path: &Path) -> FFIResult<ProcedureDescriptorFFI> {
		let library = self
			.cache
			.get(path)
			.ok_or_else(|| FFIError::Other(format!("Library not loaded: {}", path.display())))?;
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
		validate_api_version(descriptor.api).map_err(|e| FFIError::Other(e.to_string()))?;

		let name = unsafe { buffer_to_string(&descriptor.name) };
		self.procedure_paths.insert(name.clone(), path.to_path_buf());

		Ok((name, descriptor.api))
	}

	/// Register a procedure library without instantiating it
	pub fn register_procedure(&mut self, path: &Path) -> FFIResult<Option<LoadedProcedureInfo>> {
		if !self.load_procedure_library(path)? {
			return Ok(None);
		}

		let descriptor = self.get_descriptor(path)?;
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

		let descriptor = self.get_descriptor(path)?;
		self.validate_and_register(&descriptor, path)?;

		let library = self.cache.get(path).unwrap();
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

		let name = unsafe { buffer_to_string(&descriptor.name) };
		Ok(Some(NativeProcedureFFI::new(name, descriptor, instance)))
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

/// Scan a directory for FFI procedure shared libraries and register them
/// onto an existing `RoutinesConfigurator`.
pub fn register_procedures_from_dir(dir: &Path, mut builder: RoutinesConfigurator) -> FFIResult<RoutinesConfigurator> {
	let loader = ffi_procedure_loader();
	let mut loader_guard = loader.write().unwrap();

	let mut names = Vec::new();

	let entries = fs::read_dir(dir)
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

	for name in names {
		let proc = loader_guard
			.create_procedure_by_name(&name, &[])
			.map_err(|e| FFIError::Other(format!("Failed to instantiate procedure '{}': {}", name, e)))?;
		builder = builder.register_procedure(Arc::new(proc));
	}

	Ok(builder)
}
