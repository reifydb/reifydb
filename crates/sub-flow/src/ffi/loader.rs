//! FFI operator dynamic library loader

use std::{
	collections::HashMap,
	path::{Path, PathBuf},
	sync::OnceLock,
};

use libloading::{Library, Symbol};
use parking_lot::RwLock;
use reifydb_core::interface::FlowNodeId;
use reifydb_flow_operator_abi::{
	BufferFFI, CURRENT_API_VERSION, FFIOperatorColumnDefs, FFIOperatorCreateFn, FFIOperatorDescriptor,
	FFIOperatorMagicFn, OPERATOR_MAGIC,
};
use reifydb_flow_operator_sdk::{FFIError, Result as FFIResult};

use crate::operator::FFIOperator;

/// Extract a UTF-8 string from a BufferFFI
///
/// # Safety
/// The buffer must contain valid UTF-8 data and the pointer must be valid for the given length
unsafe fn buffer_to_string(buffer: &BufferFFI) -> String {
	if buffer.ptr.is_null() || buffer.len == 0 {
		return String::new();
	}
	// SAFETY: caller guarantees buffer.ptr is valid for buffer.len bytes
	let slice = unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) };
	std::str::from_utf8(slice).unwrap_or("<invalid UTF-8>").to_string()
}

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

	pub fn load_operator_library(&mut self, path: &Path) -> FFIResult<bool> {
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

	/// Get the operator descriptor from a loaded library
	fn get_descriptor(&self, library: &Library) -> FFIResult<FFIOperatorDescriptor> {
		unsafe {
			let get_descriptor: Symbol<extern "C" fn() -> *const FFIOperatorDescriptor> =
				library.get(b"ffi_operator_get_descriptor\0").map_err(|e| {
					FFIError::Other(format!("Failed to find ffi_operator_get_descriptor: {}", e))
				})?;

			let descriptor_ptr = get_descriptor();
			if descriptor_ptr.is_null() {
				return Err(FFIError::Other("Descriptor is null".to_string()));
			}

			// Copy the descriptor fields
			Ok(FFIOperatorDescriptor {
				api_version: (*descriptor_ptr).api_version,
				operator_name: (*descriptor_ptr).operator_name,
				operator_version: (*descriptor_ptr).operator_version,
				operator_description: (*descriptor_ptr).operator_description,
				input_columns: (*descriptor_ptr).input_columns,
				output_columns: (*descriptor_ptr).output_columns,
				vtable: (*descriptor_ptr).vtable,
			})
		}
	}

	/// Validate descriptor and register operator name mapping
	/// Returns the operator name and API version
	fn validate_and_register(
		&mut self,
		descriptor: &FFIOperatorDescriptor,
		path: &Path,
	) -> FFIResult<(String, u32)> {
		// Verify API version
		if descriptor.api_version != CURRENT_API_VERSION {
			return Err(FFIError::Other(format!(
				"API version mismatch: expected {}, got {}",
				CURRENT_API_VERSION, descriptor.api_version
			)));
		}

		// Extract operator name
		let operator_name = unsafe { buffer_to_string(&descriptor.operator_name) };

		// Store operator name -> path mapping
		self.operator_paths.insert(operator_name.clone(), path.to_path_buf());

		Ok((operator_name, descriptor.api_version))
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
	/// * `Ok(Some(LoadedOperatorInfo))` - Successfully registered operator with full metadata
	/// * `Ok(None)` - Library is not a valid FFI operator (silently skipped)
	/// * `Err(FFIError)` - Loading or validation failed
	pub fn register_operator(&mut self, path: &Path) -> FFIResult<Option<LoadedOperatorInfo>> {
		if !self.load_operator_library(path)? {
			return Ok(None);
		}

		let library = self.loaded_libraries.get(path).unwrap();
		let descriptor = self.get_descriptor(library)?;
		let (operator_name, api_version) = self.validate_and_register(&descriptor, path)?;

		// Extract full operator info including column definitions
		let info = unsafe {
			LoadedOperatorInfo {
				operator_name,
				library_path: path.to_path_buf(),
				api_version,
				operator_version: buffer_to_string(&descriptor.operator_version),
				description: buffer_to_string(&descriptor.operator_description),
				input_columns: extract_column_defs(&descriptor.input_columns),
				output_columns: extract_column_defs(&descriptor.output_columns),
			}
		};

		Ok(Some(info))
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
		if !self.load_operator_library(path)? {
			return Ok(None);
		}

		// Get descriptor and validate - done in separate scope to avoid borrow conflicts
		let descriptor = {
			let library = self.loaded_libraries.get(path).unwrap();
			self.get_descriptor(library)?
		};

		self.validate_and_register(&descriptor, path)?;

		// Get the create function and instantiate operator
		let library = self.loaded_libraries.get(path).unwrap();
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
	pub fn list_loaded_operators(&self) -> Vec<LoadedOperatorInfo> {
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

						operators.push(LoadedOperatorInfo {
							operator_name: buffer_to_string(&descriptor.operator_name),
							library_path: path.clone(),
							api_version: descriptor.api_version,
							operator_version: buffer_to_string(
								&descriptor.operator_version,
							),
							description: buffer_to_string(&descriptor.operator_description),
							input_columns: extract_column_defs(&descriptor.input_columns),
							output_columns: extract_column_defs(&descriptor.output_columns),
						});
					}
				}
			}
		}

		operators
	}
}

/// Information about a loaded FFI operator
#[derive(Debug, Clone)]
pub struct LoadedOperatorInfo {
	pub operator_name: String,
	pub library_path: PathBuf,
	pub api_version: u32,
	pub operator_version: String,
	pub description: String,
	pub input_columns: Vec<ColumnDefInfo>,
	pub output_columns: Vec<ColumnDefInfo>,
}

/// Information about a single column definition in an operator
#[derive(Debug, Clone)]
pub struct ColumnDefInfo {
	pub name: String,
	pub field_type: reifydb_type::Type,
	pub description: String,
}

/// Extract column definitions from an FFIOperatorColumnDefs
///
/// # Safety
/// The column_defs must have valid columns pointer for column_count elements
unsafe fn extract_column_defs(column_defs: &FFIOperatorColumnDefs) -> Vec<ColumnDefInfo> {
	if column_defs.columns.is_null() || column_defs.column_count == 0 {
		return Vec::new();
	}

	let mut columns = Vec::with_capacity(column_defs.column_count);
	for i in 0..column_defs.column_count {
		// SAFETY: caller guarantees column_defs.columns is valid for column_count elements
		let col = unsafe { &*column_defs.columns.add(i) };
		columns.push(ColumnDefInfo {
			// SAFETY: column buffers are valid UTF-8 strings from the operator
			name: unsafe { buffer_to_string(&col.name) },
			field_type: reifydb_type::Type::from_u8(col.field_type),
			description: unsafe { buffer_to_string(&col.description) },
		});
	}

	columns
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
