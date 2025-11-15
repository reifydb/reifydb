//! FFI operator dynamic library loader

use std::{
	collections::HashMap,
	path::{Path, PathBuf},
};

use libloading::{Library, Symbol};
use reifydb_core::interface::FlowNodeId;
use reifydb_flow_operator_abi::{FFIOperatorCreateFn, FFIOperatorDescriptor};

use crate::{
	ffi::{FFIError, FFIResult},
	operator::FFIOperator,
};

/// FFI operator loader for dynamic libraries
pub struct FFIOperatorLoader {
	/// Loaded libraries mapped by path
	loaded_libraries: HashMap<PathBuf, Library>,
}

impl FFIOperatorLoader {
	/// Create a new FFI operator loader
	pub fn new() -> Self {
		Self {
			loaded_libraries: HashMap::new(),
		}
	}

	/// Load an operator from a dynamic library
	///
	/// # Arguments
	/// * `path` - Path to the shared library file
	/// * `config` - Operator configuration data
	/// * `node_id` - Node ID for this operator instance
	///
	/// # Returns
	/// * `Ok(FFIOperator)` - Successfully loaded operator
	/// * `Err(FFIError)` - Loading or initialization failed
	pub fn load_operator(&mut self, path: &Path, config: &[u8], node_id: FlowNodeId) -> FFIResult<FFIOperator> {
		// Load the library if not already loaded
		let library = if let Some(lib) = self.loaded_libraries.get(path) {
			lib
		} else {
			// Load the library
			let lib = unsafe {
				Library::new(path).map_err(|e| {
					FFIError::Other(format!("Failed to load library {}: {}", path.display(), e))
				})?
			};

			self.loaded_libraries.insert(path.to_path_buf(), lib);
			self.loaded_libraries.get(path).unwrap()
		};

		// Get the operator descriptor
		let descriptor = unsafe {
			let get_descriptor: Symbol<extern "C" fn() -> *const FFIOperatorDescriptor> =
				library.get(b"ffi_operator_get_descriptor\0").map_err(|e| {
					FFIError::Other(format!("Failed to find ffi_operator_get_descriptor: {}", e))
				})?;

			let descriptor_ptr = get_descriptor();
			if descriptor_ptr.is_null() {
				return Err(FFIError::Other("Descriptor is null".to_string()));
			}

			// Copy the descriptor fields
			FFIOperatorDescriptor {
				api_version: (*descriptor_ptr).api_version,
				operator_name: (*descriptor_ptr).operator_name,
				capabilities: (*descriptor_ptr).capabilities,
				vtable: (*descriptor_ptr).vtable,
			}
		};

		// Verify API version
		if descriptor.api_version != reifydb_flow_operator_abi::CURRENT_API_VERSION {
			return Err(FFIError::Other(format!(
				"API version mismatch: expected {}, got {}",
				reifydb_flow_operator_abi::CURRENT_API_VERSION,
				descriptor.api_version
			)));
		}

		// Get the create function
		let create_fn: FFIOperatorCreateFn = unsafe {
			let create_symbol: Symbol<FFIOperatorCreateFn> = library
				.get(b"ffi_operator_create\0")
				.map_err(|e| FFIError::Other(format!("Failed to find ffi_operator_create: {}", e)))?;

			*create_symbol
		};

		// Create the operator instance
		let instance = create_fn(config.as_ptr(), config.len());
		if instance.is_null() {
			return Err(FFIError::Other("Failed to create operator instance".to_string()));
		}

		// Create the FFI operator wrapper
		Ok(FFIOperator::new(descriptor, instance, node_id))
	}

	/// Create an operator instance from an already loaded library
	///
	/// # Arguments
	/// * `operator_type` - Name of the operator type
	/// * `operator_id` - Node ID for this operator instance
	/// * `config` - Configuration data for the operator
	///
	/// # Returns
	/// * `Ok(Box<dyn Operator>)` - Successfully created operator
	/// * `Err(FFIError)` - Creation failed
	pub fn create_operator(
		&self,
		_operator_type: &str,
		_operator_id: FlowNodeId,
		_config: &[u8],
	) -> Result<Box<dyn crate::operator::Operator>, FFIError> {
		// This would look up the operator type in a registry
		// For now, return not supported
		Err(FFIError::NotSupported)
	}

	/// Unload a library
	///
	/// # Arguments
	/// * `path` - Path to the library to unload
	///
	/// # Safety
	/// This will invalidate any operators created from this library.
	/// Ensure all operators from this library are destroyed first.
	pub fn unload_library(&mut self, path: &Path) -> FFIResult<()> {
		if self.loaded_libraries.remove(path).is_some() {
			Ok(())
		} else {
			Err(FFIError::Other(format!("Library not loaded: {}", path.display())))
		}
	}

	/// Get the number of loaded libraries
	pub fn loaded_count(&self) -> usize {
		self.loaded_libraries.len()
	}

	/// Check if a library is loaded
	pub fn is_loaded(&self, path: &Path) -> bool {
		self.loaded_libraries.contains_key(path)
	}
}

impl Default for FFIOperatorLoader {
	fn default() -> Self {
		Self::new()
	}
}

impl Drop for FFIOperatorLoader {
	fn drop(&mut self) {
		// Libraries will be automatically unloaded when dropped
		self.loaded_libraries.clear();
	}
}
