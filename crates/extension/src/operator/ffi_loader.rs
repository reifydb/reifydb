// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#![cfg(reifydb_target = "native")]

//! FFI operator dynamic library loader

use std::{
	collections::HashMap,
	ffi::c_void,
	path::{Path, PathBuf},
	sync::{OnceLock, RwLock},
};

use libloading::Symbol;
use reifydb_abi::operator::{
	column::OperatorColumnsFFI,
	descriptor::OperatorDescriptorFFI,
	types::{OPERATOR_MAGIC, OperatorCreateFnFFI},
};
use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_sdk::error::{FFIError, Result as FFIResult};
use reifydb_type::value::constraint::{FFITypeConstraint, TypeConstraint};

use crate::loader::ffi::{LibraryCache, buffer_to_string, validate_api_version};

/// Global singleton FFI operator loader
/// Ensures libraries stay loaded for the entire process lifetime
static GLOBAL_FFI_OPERATOR_LOADER: OnceLock<RwLock<FFIOperatorLoader>> = OnceLock::new();

/// Get the global FFI operator loader
pub fn ffi_operator_loader() -> &'static RwLock<FFIOperatorLoader> {
	GLOBAL_FFI_OPERATOR_LOADER.get_or_init(|| RwLock::new(FFIOperatorLoader::new()))
}

/// FFI operator loader for dynamic libraries
/// This is meant to be used as a singleton via ffi_operator_loader()
pub struct FFIOperatorLoader {
	cache: LibraryCache,
	/// Map of operator names to library paths for quick lookup
	operator_paths: HashMap<String, PathBuf>,
}

impl FFIOperatorLoader {
	fn new() -> Self {
		Self {
			cache: LibraryCache::new(),
			operator_paths: HashMap::new(),
		}
	}

	pub fn load_operator_library(&mut self, path: &Path) -> FFIResult<bool> {
		self.cache
			.check_magic(path, b"ffi_operator_magic\0", OPERATOR_MAGIC)
			.map_err(|e| FFIError::Other(e.to_string()))
	}

	/// Get the operator descriptor from a loaded library
	fn get_descriptor(&self, path: &Path) -> FFIResult<OperatorDescriptorFFI> {
		let library = self
			.cache
			.get(path)
			.ok_or_else(|| FFIError::Other(format!("Library not loaded: {}", path.display())))?;
		unsafe {
			let get_descriptor: Symbol<extern "C" fn() -> *const OperatorDescriptorFFI> =
				library.get(b"ffi_operator_get_descriptor\0").map_err(|e| {
					FFIError::Other(format!("Failed to find ffi_operator_get_descriptor: {}", e))
				})?;

			let descriptor_ptr = get_descriptor();
			if descriptor_ptr.is_null() {
				return Err(FFIError::Other("Descriptor is null".to_string()));
			}

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
	fn validate_and_register(
		&mut self,
		descriptor: &OperatorDescriptorFFI,
		path: &Path,
	) -> FFIResult<(String, u32)> {
		validate_api_version(descriptor.api).map_err(|e| FFIError::Other(e.to_string()))?;

		let operator = unsafe { buffer_to_string(&descriptor.operator) };
		self.operator_paths.insert(operator.clone(), path.to_path_buf());

		Ok((operator, descriptor.api))
	}

	/// Register an operator library without instantiating it
	pub fn register_operator(&mut self, path: &Path) -> FFIResult<Option<LoadedOperatorInfo>> {
		if !self.load_operator_library(path)? {
			return Ok(None);
		}

		let descriptor = self.get_descriptor(path)?;
		let (operator, api) = self.validate_and_register(&descriptor, path)?;

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

	/// Load an operator from a dynamic library, returning raw descriptor + instance pointer.
	///
	/// The caller is responsible for wrapping these into the appropriate operator type.
	pub fn load_operator(
		&mut self,
		path: &Path,
		config: &[u8],
		operator_id: FlowNodeId,
	) -> FFIResult<Option<(OperatorDescriptorFFI, *mut c_void)>> {
		if !self.load_operator_library(path)? {
			return Ok(None);
		}

		let descriptor = self.get_descriptor(path)?;
		self.validate_and_register(&descriptor, path)?;

		let library = self.cache.get(path).unwrap();
		let create_fn: OperatorCreateFnFFI = unsafe {
			let create_symbol: Symbol<OperatorCreateFnFFI> = library
				.get(b"ffi_operator_create\0")
				.map_err(|e| FFIError::Other(format!("Failed to find ffi_operator_create: {}", e)))?;

			*create_symbol
		};

		let instance = create_fn(config.as_ptr(), config.len(), operator_id.0);
		if instance.is_null() {
			return Err(FFIError::Other("Failed to create operator instance".to_string()));
		}

		Ok(Some((descriptor, instance)))
	}

	/// Create an operator instance from an already loaded library by name,
	/// returning raw descriptor + instance pointer.
	pub fn create_operator_by_name(
		&mut self,
		operator: &str,
		operator_id: FlowNodeId,
		config: &[u8],
	) -> FFIResult<(OperatorDescriptorFFI, *mut c_void)> {
		let path = self
			.operator_paths
			.get(operator)
			.ok_or_else(|| FFIError::Other(format!("Operator not found: {}", operator)))?
			.clone();

		self.load_operator(&path, config, operator_id)?
			.ok_or_else(|| FFIError::Other(format!("Operator library no longer valid: {}", operator)))
	}

	/// Check if an operator name is registered
	pub fn has_operator(&self, operator: &str) -> bool {
		self.operator_paths.contains_key(operator)
	}

	/// List all loaded operators with their metadata
	pub fn list_loaded_operators(&self) -> Vec<LoadedOperatorInfo> {
		let mut operators = Vec::new();

		for path in self.operator_paths.values() {
			if let Ok(descriptor) = self.get_descriptor(path) {
				unsafe {
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
	pub input_columns: Vec<ColumnInfo>,
	pub output_columns: Vec<ColumnInfo>,
	pub capabilities: u32,
}

/// Information about a single column definition in an operator
#[derive(Debug, Clone)]
pub struct ColumnInfo {
	pub name: String,
	pub field_type: TypeConstraint,
	pub description: String,
}

/// Extract column definitions from an OperatorColumnsFFI
///
/// # Safety
/// The column_defs must have valid columns pointer for column_count elements
unsafe fn extract_column_defs(column_defs: &OperatorColumnsFFI) -> Vec<ColumnInfo> {
	if column_defs.columns.is_null() || column_defs.column_count == 0 {
		return Vec::new();
	}

	let mut columns = Vec::with_capacity(column_defs.column_count);
	for i in 0..column_defs.column_count {
		let col = unsafe { &*column_defs.columns.add(i) };

		let field_type = TypeConstraint::from_ffi(FFITypeConstraint {
			base_type: col.base_type,
			constraint_type: col.constraint_type,
			constraint_param1: col.constraint_param1,
			constraint_param2: col.constraint_param2,
		});

		columns.push(ColumnInfo {
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
