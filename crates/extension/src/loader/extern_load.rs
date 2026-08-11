// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	path::{Path, PathBuf},
};

use libloading::{Library, Symbol};

use crate::error::ExtensionError;

pub struct ExternLoad {
	libraries: HashMap<PathBuf, Library>,
}

impl ExternLoad {
	pub fn new() -> Self {
		Self {
			libraries: HashMap::new(),
		}
	}

	pub fn load(&mut self, path: &Path) -> Result<(), ExtensionError> {
		if !self.libraries.contains_key(path) {
			// SAFETY: loading runs the object's initializers; only trusted paths reach here.
			let lib = unsafe {
				Library::new(path).map_err(|e| {
					ExtensionError::ExternError(format!(
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

	pub fn library(&mut self, path: &Path) -> Result<&Library, ExtensionError> {
		self.load(path)?;
		self.libraries
			.get(path)
			.ok_or_else(|| ExtensionError::ExternError(format!("library not loaded: {}", path.display())))
	}

	pub fn get(&self, path: &Path) -> Option<&Library> {
		self.libraries.get(path)
	}

	pub fn remove(&mut self, path: &Path) {
		self.libraries.remove(path);
	}

	pub fn check_magic(&mut self, path: &Path, symbol_name: &[u8], expected: u32) -> Result<bool, ExtensionError> {
		let library = self.library(path)?;

		// SAFETY: the ABI declares the magic symbol with this signature; Symbol borrows the library.
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

impl Default for ExternLoad {
	fn default() -> Self {
		Self::new()
	}
}

impl Drop for ExternLoad {
	fn drop(&mut self) {
		self.libraries.clear();
	}
}
