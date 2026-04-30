// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	path::{Path, PathBuf},
	slice, str,
};

use libloading::{Library, Symbol};
use reifydb_abi::{constants::CURRENT_API, data::buffer::BufferFFI};

use crate::error::ExtensionError;

/// Extract a UTF-8 string from a `BufferFFI`.
///
/// # Safety
/// The buffer must contain valid UTF-8 data and the pointer must be valid for the given length.
pub unsafe fn buffer_to_string(buffer: &BufferFFI) -> String {
	if buffer.ptr.is_null() || buffer.len == 0 {
		return String::new();
	}
	let slice = unsafe { slice::from_raw_parts(buffer.ptr, buffer.len) };
	str::from_utf8(slice).unwrap_or("<invalid UTF-8>").to_string()
}

/// Validate that the given API version matches `CURRENT_API`.
pub fn validate_api_version(api: u32) -> Result<(), ExtensionError> {
	if api != CURRENT_API {
		return Err(ExtensionError::ApiVersionMismatch {
			expected: CURRENT_API,
			actual: api,
		});
	}
	Ok(())
}

/// Shared library cache that keeps loaded libraries alive.
pub struct LibraryCache {
	libraries: HashMap<PathBuf, Library>,
}

impl LibraryCache {
	pub fn new() -> Self {
		Self {
			libraries: HashMap::new(),
		}
	}

	/// Load a library if not already cached. Returns `true` if library was loaded or already present.
	pub fn load(&mut self, path: &Path) -> Result<(), ExtensionError> {
		if !self.libraries.contains_key(path) {
			let lib = unsafe {
				Library::new(path).map_err(|e| {
					ExtensionError::FfiLoad(format!(
						"Failed to load library {}: {}",
						path.display(),
						e
					))
				})?
			};
			self.libraries.insert(path.to_path_buf(), lib);
		}
		Ok(())
	}

	/// Get a reference to a loaded library.
	pub fn get(&self, path: &Path) -> Option<&Library> {
		self.libraries.get(path)
	}

	/// Remove a library from the cache.
	pub fn remove(&mut self, path: &Path) {
		self.libraries.remove(path);
	}

	/// Check the magic number exported by a library.
	/// Returns `true` if the magic matches, `false` if the symbol is missing or doesn't match.
	/// Removes the library from cache if the symbol is not found.
	pub fn check_magic(&mut self, path: &Path, symbol_name: &[u8], expected: u32) -> Result<bool, ExtensionError> {
		self.load(path)?;
		let library = self.libraries.get(path).unwrap();

		let magic_result: Result<Symbol<extern "C" fn() -> u32>, _> = unsafe { library.get(symbol_name) };

		match magic_result {
			Ok(magic_fn) => {
				let magic = magic_fn();
				Ok(magic == expected)
			}
			Err(_) => {
				self.remove(path);
				Ok(false)
			}
		}
	}
}

impl Default for LibraryCache {
	fn default() -> Self {
		Self::new()
	}
}

impl Drop for LibraryCache {
	fn drop(&mut self) {
		self.libraries.clear();
	}
}
