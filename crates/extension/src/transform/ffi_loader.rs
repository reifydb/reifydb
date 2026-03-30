// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! FFI transform dynamic library loader

use std::{
	collections::HashMap,
	fs,
	path::{Path, PathBuf},
	sync::{OnceLock, RwLock},
};

use libloading::Symbol;
use reifydb_abi::transform::{
	descriptor::TransformDescriptorFFI,
	types::{TRANSFORM_MAGIC, TransformCreateFnFFI},
};
use reifydb_sdk::error::{FFIError, Result as FFIResult};

use super::{
	ffi::NativeTransformFFI,
	registry::{Transforms, TransformsConfigurator},
};
use crate::loader::ffi::{LibraryCache, buffer_to_string, validate_api_version};

/// Global singleton FFI transform loader
static GLOBAL_FFI_TRANSFORM_LOADER: OnceLock<RwLock<TransformLoader>> = OnceLock::new();

/// Get the global FFI transform loader
pub fn ffi_transform_loader() -> &'static RwLock<TransformLoader> {
	GLOBAL_FFI_TRANSFORM_LOADER.get_or_init(|| RwLock::new(TransformLoader::new()))
}

/// FFI transform loader for dynamic libraries
pub struct TransformLoader {
	cache: LibraryCache,
	transform_paths: HashMap<String, PathBuf>,
}

impl TransformLoader {
	fn new() -> Self {
		Self {
			cache: LibraryCache::new(),
			transform_paths: HashMap::new(),
		}
	}

	pub fn load_transform_library(&mut self, path: &Path) -> FFIResult<bool> {
		self.cache
			.check_magic(path, b"ffi_transform_magic\0", TRANSFORM_MAGIC)
			.map_err(|e| FFIError::Other(e.to_string()))
	}

	fn get_descriptor(&self, path: &Path) -> FFIResult<TransformDescriptorFFI> {
		let library = self
			.cache
			.get(path)
			.ok_or_else(|| FFIError::Other(format!("Library not loaded: {}", path.display())))?;
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
		validate_api_version(descriptor.api).map_err(|e| FFIError::Other(e.to_string()))?;

		let name = unsafe { buffer_to_string(&descriptor.name) };
		self.transform_paths.insert(name.clone(), path.to_path_buf());

		Ok((name, descriptor.api))
	}

	/// Register a transform library without instantiating it
	pub fn register_transform(&mut self, path: &Path) -> FFIResult<Option<LoadedTransformInfo>> {
		if !self.load_transform_library(path)? {
			return Ok(None);
		}

		let descriptor = self.get_descriptor(path)?;
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

		let descriptor = self.get_descriptor(path)?;
		self.validate_and_register(&descriptor, path)?;

		let library = self.cache.get(path).unwrap();
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

/// Scan a directory for FFI transform shared libraries and register them
/// onto an existing `TransformsConfigurator`.
pub fn register_transforms_from_dir(
	dir: &Path,
	mut builder: TransformsConfigurator,
) -> FFIResult<TransformsConfigurator> {
	let loader = ffi_transform_loader();
	let mut loader_guard = loader.write().unwrap();

	let mut names = Vec::new();

	let entries = fs::read_dir(dir)
		.map_err(|e| FFIError::Other(format!("Failed to read directory {}: {}", dir.display(), e)))?;

	for entry in entries {
		let entry = entry.map_err(|e| FFIError::Other(format!("Failed to read directory entry: {}", e)))?;
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

	for name in names {
		let name_clone = name.clone();
		builder = builder.register(&name, move || {
			let loader = ffi_transform_loader();
			let mut loader_guard = loader.write().unwrap();
			loader_guard.create_transform_by_name(&name_clone, &[]).unwrap()
		});
	}

	Ok(builder)
}

/// Scan a directory for FFI transform shared libraries, register them,
/// and return a `Transforms` registry with factory functions for each.
pub fn load_transforms_from_dir(dir: &Path) -> FFIResult<Transforms> {
	Ok(register_transforms_from_dir(dir, Transforms::builder())?.configure())
}
