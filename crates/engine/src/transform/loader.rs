#![cfg(reifydb_target = "native")]
// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FFI transform dynamic library loader

use std::{
	collections::HashMap,
	path::{Path, PathBuf},
	sync::{OnceLock, RwLock},
};

use libloading::{Library, Symbol};
use reifydb_abi::{
	constants::CURRENT_API,
	data::buffer::BufferFFI,
	transform::{
		descriptor::TransformDescriptorFFI,
		types::{TRANSFORM_MAGIC, TransformCreateFnFFI, TransformMagicFnFFI},
	},
};
use reifydb_sdk::error::{FFIError, Result as FFIResult};

use super::{ffi::NativeTransformFFI, registry::Transforms};

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

/// Global singleton FFI transform loader
static GLOBAL_FFI_TRANSFORM_LOADER: OnceLock<RwLock<TransformLoader>> = OnceLock::new();

/// Get the global FFI transform loader
pub fn ffi_transform_loader() -> &'static RwLock<TransformLoader> {
	GLOBAL_FFI_TRANSFORM_LOADER.get_or_init(|| RwLock::new(TransformLoader::new()))
}

/// FFI transform loader for dynamic libraries
pub struct TransformLoader {
	/// Loaded libraries mapped by path
	loaded_libraries: HashMap<PathBuf, Library>,
	/// Map of transform names to library paths for quick lookup
	transform_paths: HashMap<String, PathBuf>,
}

impl TransformLoader {
	fn new() -> Self {
		Self {
			loaded_libraries: HashMap::new(),
			transform_paths: HashMap::new(),
		}
	}

	pub fn load_transform_library(&mut self, path: &Path) -> FFIResult<bool> {
		if !self.loaded_libraries.contains_key(path) {
			let lib = unsafe {
				Library::new(path).map_err(|e| {
					FFIError::Other(format!("Failed to load library {}: {}", path.display(), e))
				})?
			};
			self.loaded_libraries.insert(path.to_path_buf(), lib);
		}

		let library = self.loaded_libraries.get(path).unwrap();

		let magic_result: Result<Symbol<TransformMagicFnFFI>, _> =
			unsafe { library.get(b"ffi_transform_magic\0") };

		match magic_result {
			Ok(magic_fn) => {
				let magic = magic_fn();
				Ok(magic == TRANSFORM_MAGIC)
			}
			Err(_) => {
				self.loaded_libraries.remove(path);
				Ok(false)
			}
		}
	}

	fn get_descriptor(&self, library: &Library) -> FFIResult<TransformDescriptorFFI> {
		unsafe {
			let get_descriptor: Symbol<extern "C" fn() -> *const TransformDescriptorFFI> =
				library.get(b"ffi_transform_get_descriptor\0").map_err(|e| {
					FFIError::Other(format!("Failed to find ffi_transform_get_descriptor: {}", e))
				})?;

			let descriptor_ptr = get_descriptor();
			if descriptor_ptr.is_null() {
				return Err(FFIError::Other("Descriptor is null".to_string()));
			}

			Ok(TransformDescriptorFFI {
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
		descriptor: &TransformDescriptorFFI,
		path: &Path,
	) -> FFIResult<(String, u32)> {
		if descriptor.api != CURRENT_API {
			return Err(FFIError::Other(format!(
				"API version mismatch: expected {}, got {}",
				CURRENT_API, descriptor.api
			)));
		}

		let name = unsafe { buffer_to_string(&descriptor.name) };
		self.transform_paths.insert(name.clone(), path.to_path_buf());

		Ok((name, descriptor.api))
	}

	/// Register a transform library without instantiating it
	pub fn register_transform(&mut self, path: &Path) -> FFIResult<Option<LoadedTransformInfo>> {
		if !self.load_transform_library(path)? {
			return Ok(None);
		}

		let library = self.loaded_libraries.get(path).unwrap();
		let descriptor = self.get_descriptor(library)?;
		let (name, api) = self.validate_and_register(&descriptor, path)?;

		let info = unsafe {
			LoadedTransformInfo {
				name,
				library_path: path.to_path_buf(),
				api,
				version: buffer_to_string(&descriptor.version),
				description: buffer_to_string(&descriptor.description),
			}
		};

		Ok(Some(info))
	}

	/// Load a transform from a dynamic library
	pub fn load_transform(&mut self, path: &Path, config: &[u8]) -> FFIResult<Option<NativeTransformFFI>> {
		if !self.load_transform_library(path)? {
			return Ok(None);
		}

		let descriptor = {
			let library = self.loaded_libraries.get(path).unwrap();
			self.get_descriptor(library)?
		};

		self.validate_and_register(&descriptor, path)?;

		let library = self.loaded_libraries.get(path).unwrap();
		let create_fn: TransformCreateFnFFI = unsafe {
			let create_symbol: Symbol<TransformCreateFnFFI> = library
				.get(b"ffi_transform_create\0")
				.map_err(|e| FFIError::Other(format!("Failed to find ffi_transform_create: {}", e)))?;

			*create_symbol
		};

		let instance = create_fn(config.as_ptr(), config.len());
		if instance.is_null() {
			return Err(FFIError::Other("Failed to create transform instance".to_string()));
		}

		Ok(Some(NativeTransformFFI::new(descriptor, instance)))
	}

	/// Create a transform instance from an already loaded library by name
	pub fn create_transform_by_name(&mut self, name: &str, config: &[u8]) -> FFIResult<NativeTransformFFI> {
		let path = self
			.transform_paths
			.get(name)
			.ok_or_else(|| FFIError::Other(format!("Transform not found: {}", name)))?
			.clone();

		self.load_transform(&path, config)?
			.ok_or_else(|| FFIError::Other(format!("Transform library no longer valid: {}", name)))
	}

	/// Check if a transform name is registered
	pub fn has_transform(&self, name: &str) -> bool {
		self.transform_paths.contains_key(name)
	}
}

/// Information about a loaded FFI transform
#[derive(Debug, Clone)]
pub struct LoadedTransformInfo {
	pub name: String,
	pub library_path: PathBuf,
	pub api: u32,
	pub version: String,
	pub description: String,
}

impl Default for TransformLoader {
	fn default() -> Self {
		Self::new()
	}
}

impl Drop for TransformLoader {
	fn drop(&mut self) {
		self.loaded_libraries.clear();
	}
}

/// Scan a directory for FFI transform shared libraries, register them,
/// and return a `Transforms` registry with factory functions for each.
pub fn load_transforms_from_dir(dir: &Path) -> reifydb_type::Result<Transforms> {
	let loader = ffi_transform_loader();
	let mut loader_guard = loader.write().unwrap();

	let mut names = Vec::new();

	let entries = std::fs::read_dir(dir).map_err(|e| {
		reifydb_sdk::error::FFIError::Other(format!("Failed to read directory {}: {}", dir.display(), e))
	})?;

	for entry in entries {
		let entry = entry.map_err(|e| {
			reifydb_sdk::error::FFIError::Other(format!("Failed to read directory entry: {}", e))
		})?;
		let path = entry.path();
		let ext = path.extension().and_then(|s| s.to_str());

		if ext == Some("so") || ext == Some("dylib") {
			match loader_guard.register_transform(&path) {
				Ok(Some(info)) => {
					names.push(info.name);
				}
				Ok(None) => {
					// Not a valid transform library, skip
				}
				Err(e) => {
					eprintln!(
						"Warning: Failed to register transform from {}: {}",
						path.display(),
						e
					);
				}
			}
		}
	}

	drop(loader_guard);

	let mut builder = Transforms::builder();
	for name in names {
		let name_clone = name.clone();
		builder = builder.register(&name, move || {
			let loader = ffi_transform_loader();
			let mut loader_guard = loader.write().unwrap();
			loader_guard.create_transform_by_name(&name_clone, &[]).unwrap()
		});
	}

	Ok(builder.build())
}
