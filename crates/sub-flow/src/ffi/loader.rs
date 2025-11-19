//! FFI operator dynamic library loader

use std::{
	collections::HashMap,
	ffi::CStr,
	path::{Path, PathBuf},
	sync::OnceLock,
};

use libloading::{Library, Symbol};
use parking_lot::RwLock;
use reifydb_core::interface::FlowNodeId;
use reifydb_flow_operator_abi::{
	CURRENT_API_VERSION, FFIOperatorCreateFn, FFIOperatorDescriptor, FFIOperatorMagicFn, OPERATOR_MAGIC,
};
use reifydb_flow_operator_sdk::{FFIError, Result as FFIResult};

use crate::operator::FFIOperator;

/// Global singleton FFI operator loader
/// Ensures libraries stay loaded for the entire process lifetime
static GLOBAL_FFI_OPERATOR_LOADER: OnceLock<RwLock<FFIOperatorLoader>> = OnceLock::new();

/// Get the global FFI operator loader
pub fn ffi_operator_loader() -> &'static RwLock<FFIOperatorLoader> {
	GLOBAL_FFI_OPERATOR_LOADER.get_or_init(|| RwLock::new(FFIOperatorLoader::new()))
}

/// FFI operator loader for dynamic libraries
/// This is meant to be used as a singleton via get_global_loader()
pub struct FFIOperatorLoader {
	/// Loaded libraries mapped by path
	loaded_libraries: HashMap<PathBuf, Library>,

	/// Map of operator names to library paths for quick lookup
	operator_paths: HashMap<String, PathBuf>,
}

impl FFIOperatorLoader {
	/// Create a new FFI operator loader
	fn new() -> Self {
		Self {
			loaded_libraries: HashMap::new(),
			operator_paths: HashMap::new(),
		}
	}

	/// Check if a library is a valid FFI operator
	///
	/// This checks for the presence of the `ffi_operator_magic` symbol and verifies
	/// the magic number matches.
	///
	/// # Arguments
	/// * `path` - Path to the shared library file
	///
	/// # Returns
	/// * `Ok(true)` - Library is a valid FFI operator
	/// * `Ok(false)` - Library is not an FFI operator (missing or wrong magic)
	/// * `Err(FFIError)` - Failed to load the library
	pub fn is_operator_library(&mut self, path: &Path) -> FFIResult<bool> {
		// Load the library if not already loaded
		if !self.loaded_libraries.contains_key(path) {
			let lib = unsafe {
				Library::new(path).map_err(|e| {
					FFIError::Other(format!("Failed to load library {}: {}", path.display(), e))
				})?
			};
			self.loaded_libraries.insert(path.to_path_buf(), lib);
		}

		let library = self.loaded_libraries.get(path).unwrap();

		// Check for magic symbol
		let magic_result: Result<Symbol<FFIOperatorMagicFn>, _> =
			unsafe { library.get(b"ffi_operator_magic\0") };

		match magic_result {
			Ok(magic_fn) => {
				let magic = magic_fn();
				Ok(magic == OPERATOR_MAGIC)
			}
			Err(_) => {
				// Symbol not found - not an operator, remove from cache
				self.loaded_libraries.remove(path);
				Ok(false)
			}
		}
	}

	/// Register an operator library without instantiating it
	///
	/// This loads the library, validates it as an operator, and extracts metadata
	/// without creating an operator instance. Use this for discovery/registration.
	///
	/// # Arguments
	/// * `path` - Path to the shared library file
	///
	/// # Returns
	/// * `Ok(Some((name, api_version)))` - Successfully registered operator
	/// * `Ok(None)` - Library is not a valid FFI operator (silently skipped)
	/// * `Err(FFIError)` - Loading or validation failed
	pub fn register_operator(&mut self, path: &Path) -> FFIResult<Option<(String, u32)>> {
		// First check if this is a valid operator library
		if !self.is_operator_library(path)? {
			return Ok(None);
		}

		let library = self.loaded_libraries.get(path).unwrap();

		// Get the operator descriptor
		let (operator_name, api_version) = unsafe {
			let get_descriptor: Symbol<extern "C" fn() -> *const FFIOperatorDescriptor> =
				library.get(b"ffi_operator_get_descriptor\0").map_err(|e| {
					FFIError::Other(format!("Failed to find ffi_operator_get_descriptor: {}", e))
				})?;

			let descriptor_ptr = get_descriptor();
			if descriptor_ptr.is_null() {
				return Err(FFIError::Other("Descriptor is null".to_string()));
			}

			let name = CStr::from_ptr((*descriptor_ptr).operator_name).to_str().unwrap().to_string();
			let version = (*descriptor_ptr).api_version;

			(name, version)
		};

		// Verify API version
		if api_version != CURRENT_API_VERSION {
			return Err(FFIError::Other(format!(
				"API version mismatch: expected {}, got {}",
				CURRENT_API_VERSION, api_version
			)));
		}

		// Store operator name -> path mapping
		self.operator_paths.insert(operator_name.clone(), path.to_path_buf());

		Ok(Some((operator_name, api_version)))
	}

	/// Load an operator from a dynamic library
	///
	/// # Arguments
	/// * `path` - Path to the shared library file
	/// * `config` - Operator configuration data
	/// * `operator_id` - ID for this operator instance
	///
	/// # Returns
	/// * `Ok(Some(FFIOperator))` - Successfully loaded operator
	/// * `Ok(None)` - Library is not a valid FFI operator (silently skipped)
	/// * `Err(FFIError)` - Loading or initialization failed
	pub fn load_operator(
		&mut self,
		path: &Path,
		config: &[u8],
		operator_id: FlowNodeId,
	) -> FFIResult<Option<FFIOperator>> {
		// First check if this is a valid operator library
		if !self.is_operator_library(path)? {
			return Ok(None);
		}

		let library = self.loaded_libraries.get(path).unwrap();

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
				vtable: (*descriptor_ptr).vtable,
			}
		};

		// Store operator name -> path mapping
		let operator_name = unsafe { CStr::from_ptr(descriptor.operator_name).to_str().unwrap().to_string() };
		self.operator_paths.insert(operator_name.clone(), path.to_path_buf());

		// Verify API version
		if descriptor.api_version != CURRENT_API_VERSION {
			return Err(FFIError::Other(format!(
				"API version mismatch: expected {}, got {}",
				CURRENT_API_VERSION, descriptor.api_version
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
		let instance = create_fn(config.as_ptr(), config.len(), operator_id.0);
		if instance.is_null() {
			return Err(FFIError::Other("Failed to create operator instance".to_string()));
		}

		// Create the FFI operator wrapper
		// Library stays loaded via global cache and loader reference
		Ok(Some(FFIOperator::new(descriptor, instance, operator_id)))
	}

	/// Create an operator instance from an already loaded library by name
	///
	/// # Arguments
	/// * `operator_name` - Name of the operator type
	/// * `operator_id` - Node ID for this operator instance
	/// * `config` - Configuration data for the operator
	///
	/// # Returns
	/// * `Ok(FFIOperator)` - Successfully created operator
	/// * `Err(FFIError)` - Creation failed
	pub fn create_operator_by_name(
		&mut self,
		operator_name: &str,
		operator_id: FlowNodeId,
		config: &[u8],
	) -> FFIResult<FFIOperator> {
		// Look up the path for this operator
		let path = self
			.operator_paths
			.get(operator_name)
			.ok_or_else(|| FFIError::Other(format!("Operator not found: {}", operator_name)))?
			.clone();

		// Load operator from the known path
		// Since this operator was previously registered, it should always be valid
		self.load_operator(&path, config, operator_id)?
			.ok_or_else(|| FFIError::Other(format!("Operator library no longer valid: {}", operator_name)))
	}

	/// Check if an operator name is registered
	pub fn has_operator(&self, operator_name: &str) -> bool {
		self.operator_paths.contains_key(operator_name)
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

	/// List all loaded operators with their metadata
	///
	/// Returns a vector of tuples containing:
	/// - Operator name
	/// - Library path
	/// - API version
	pub fn list_loaded_operators(&self) -> Vec<(String, PathBuf, u32)> {
		let mut operators = Vec::new();

		for (path, library) in &self.loaded_libraries {
			// Get the operator descriptor from the library
			unsafe {
				let get_descriptor: Result<Symbol<extern "C" fn() -> *const FFIOperatorDescriptor>, _> =
					library.get(b"ffi_operator_get_descriptor\0");

				if let Ok(get_descriptor) = get_descriptor {
					let descriptor_ptr = get_descriptor();
					if !descriptor_ptr.is_null() {
						let descriptor = &*descriptor_ptr;

						// Extract operator name
						let operator_name = CStr::from_ptr(descriptor.operator_name)
							.to_str()
							.unwrap_or("<invalid UTF-8>")
							.to_string();

						operators.push((operator_name, path.clone(), descriptor.api_version));
					}
				}
			}
		}

		operators
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
