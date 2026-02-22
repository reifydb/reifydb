#![cfg(reifydb_target = "native")]
// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FFI operator dynamic library loader

use std::{
	collections::HashMap,
	path::{Path, PathBuf},
	sync::{OnceLock, RwLock},
};

use libloading::{Library, Symbol};
use reifydb_abi::{
	constants::{CURRENT_API, OPERATOR_MAGIC},
	data::buffer::BufferFFI,
	operator::{
		column::OperatorColumnDefsFFI,
		descriptor::OperatorDescriptorFFI,
		types::{OperatorCreateFnFFI, OperatorMagicFnFFI},
	},
};
use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_engine::vm::executor::Executor;
use reifydb_sdk::error::{FFIError, Result as FFIResult};
use reifydb_type::value::constraint::{FFITypeConstraint, TypeConstraint};

use crate::operator::ffi::FFIOperator;

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
		let magic_result: Result<Symbol<OperatorMagicFnFFI>, _> =
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
	fn get_descriptor(&self, library: &Library) -> FFIResult<OperatorDescriptorFFI> {
		unsafe {
			let get_descriptor: Symbol<extern "C" fn() -> *const OperatorDescriptorFFI> =
				library.get(b"ffi_operator_get_descriptor\0").map_err(|e| {
					FFIError::Other(format!("Failed to find ffi_operator_get_descriptor: {}", e))
				})?;

			let descriptor_ptr = get_descriptor();
			if descriptor_ptr.is_null() {
				return Err(FFIError::Other("Descriptor is null".to_string()));
			}

			// Copy the descriptor fields
			Ok(OperatorDescriptorFFI {
				api: (*descriptor_ptr).api,
				operator: (*descriptor_ptr).operator,
				version: (*descriptor_ptr).version,
				description: (*descriptor_ptr).description,
				input_columns: (*descriptor_ptr).input_columns,
				output_columns: (*descriptor_ptr).output_columns,
				capabilities: (*descriptor_ptr).capabilities,
				vtable: (*descriptor_ptr).vtable,
			})
		}
	}

	/// Validate descriptor and register operator name mapping
	/// Returns the operator name and API version
	fn validate_and_register(
		&mut self,
		descriptor: &OperatorDescriptorFFI,
		path: &Path,
	) -> FFIResult<(String, u32)> {
		// Verify API version
		if descriptor.api != CURRENT_API {
			return Err(FFIError::Other(format!(
				"API version mismatch: expected {}, got {}",
				CURRENT_API, descriptor.api
			)));
		}

		// Extract operator name
		let operator = unsafe { buffer_to_string(&descriptor.operator) };

		// Store operator name -> path mapping
		self.operator_paths.insert(operator.clone(), path.to_path_buf());

		Ok((operator, descriptor.api))
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
		let (operator, api) = self.validate_and_register(&descriptor, path)?;

		// Extract full operator info including column definitions
		let info = unsafe {
			LoadedOperatorInfo {
				operator,
				library_path: path.to_path_buf(),
				api,
				version: buffer_to_string(&descriptor.version),
				description: buffer_to_string(&descriptor.description),
				input_columns: extract_column_defs(&descriptor.input_columns),
				output_columns: extract_column_defs(&descriptor.output_columns),
				capabilities: descriptor.capabilities,
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
		executor: Executor,
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
		let create_fn: OperatorCreateFnFFI = unsafe {
			let create_symbol: Symbol<OperatorCreateFnFFI> = library
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
		Ok(Some(FFIOperator::new(descriptor, instance, operator_id, executor)))
	}

	/// Create an operator instance from an already loaded library by name
	///
	/// # Arguments
	/// * `operator` - Name of the operator type
	/// * `operator_id` - Node ID for this operator instance
	/// * `config` - Configuration data for the operator
	///
	/// # Returns
	/// * `Ok(FFIOperator)` - Successfully created operator
	/// * `Err(FFIError)` - Creation failed
	pub fn create_operator_by_name(
		&mut self,
		operator: &str,
		operator_id: FlowNodeId,
		config: &[u8],
		executor: Executor,
	) -> FFIResult<FFIOperator> {
		let path = self
			.operator_paths
			.get(operator)
			.ok_or_else(|| FFIError::Other(format!("Operator not found: {}", operator)))?
			.clone();

		// Load operator from the known path
		// Since this operator was previously registered, it should always be valid
		self.load_operator(&path, config, operator_id, executor)?
			.ok_or_else(|| FFIError::Other(format!("Operator library no longer valid: {}", operator)))
	}

	/// Check if an operator name is registered
	pub fn has_operator(&self, operator: &str) -> bool {
		self.operator_paths.contains_key(operator)
	}

	/// List all loaded operators with their metadata
	pub fn list_loaded_operators(&self) -> Vec<LoadedOperatorInfo> {
		let mut operators = Vec::new();

		for (path, library) in &self.loaded_libraries {
			// Get the operator descriptor from the library
			unsafe {
				let get_descriptor: Result<Symbol<extern "C" fn() -> *const OperatorDescriptorFFI>, _> =
					library.get(b"ffi_operator_get_descriptor\0");

				if let Ok(get_descriptor) = get_descriptor {
					let descriptor_ptr = get_descriptor();
					if !descriptor_ptr.is_null() {
						let descriptor = &*descriptor_ptr;

						operators.push(LoadedOperatorInfo {
							operator: buffer_to_string(&descriptor.operator),
							library_path: path.clone(),
							api: descriptor.api,
							version: buffer_to_string(&descriptor.version),
							description: buffer_to_string(&descriptor.description),
							input_columns: extract_column_defs(&descriptor.input_columns),
							output_columns: extract_column_defs(&descriptor.output_columns),
							capabilities: descriptor.capabilities,
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
	pub operator: String,
	pub library_path: PathBuf,
	pub api: u32,
	pub version: String,
	pub description: String,
	pub input_columns: Vec<ColumnDefInfo>,
	pub output_columns: Vec<ColumnDefInfo>,
	pub capabilities: u32,
}

/// Information about a single column definition in an operator
#[derive(Debug, Clone)]
pub struct ColumnDefInfo {
	pub name: String,
	pub field_type: TypeConstraint,
	pub description: String,
}

/// Extract column definitions from an OperatorColumnDefsFFI
///
/// # Safety
/// The column_defs must have valid columns pointer for column_count elements
unsafe fn extract_column_defs(column_defs: &OperatorColumnDefsFFI) -> Vec<ColumnDefInfo> {
	if column_defs.columns.is_null() || column_defs.column_count == 0 {
		return Vec::new();
	}

	let mut columns = Vec::with_capacity(column_defs.column_count);
	for i in 0..column_defs.column_count {
		// SAFETY: caller guarantees column_defs.columns is valid for column_count elements
		let col = unsafe { &*column_defs.columns.add(i) };

		// Reconstruct TypeConstraint from FFI fields
		let field_type = TypeConstraint::from_ffi(FFITypeConstraint {
			base_type: col.base_type,
			constraint_type: col.constraint_type,
			constraint_param1: col.constraint_param1,
			constraint_param2: col.constraint_param2,
		});

		columns.push(ColumnDefInfo {
			// SAFETY: column buffers are valid UTF-8 strings from the operator
			name: unsafe { buffer_to_string(&col.name) },
			field_type,
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
