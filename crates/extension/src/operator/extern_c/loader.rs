// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	ffi::c_void,
	path::{Path, PathBuf},
	sync::OnceLock,
};

use libloading::Symbol;
use reifydb_abi::{
	constants::OPERATOR_ABI_TAG,
	data::constraint::ExternCTypeConstraint,
	operator::{
		column::ExternCOperatorColumns,
		descriptor::ExternCOperatorDescriptor,
		types::{ExternCOperatorCreateFn, OPERATOR_MAGIC},
	},
};
use reifydb_codec::constraint::type_constraint_from_extern_c;
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_sdk::error::{Result as ExternCResult, SdkError};
use reifydb_value::value::constraint::TypeConstraint;

use crate::loader::extern_c::{LibraryCache, buffer_to_string, validate_api_version};

static GLOBAL_EXTERN_C_OPERATOR_LOADER: OnceLock<RwLock<ExternCOperatorLoader>> = OnceLock::new();

pub fn extern_c_operator_loader() -> &'static RwLock<ExternCOperatorLoader> {
	GLOBAL_EXTERN_C_OPERATOR_LOADER.get_or_init(|| RwLock::new(ExternCOperatorLoader::new()))
}

pub fn check_operator_abi_tag(abi_tag: u32) -> ExternCResult<()> {
	if abi_tag != OPERATOR_ABI_TAG {
		return Err(SdkError::Other(format!(
			"extern-C operator ABI tag mismatch: plugin reports {:#06x}, host expects {:#06x}",
			abi_tag, OPERATOR_ABI_TAG
		)));
	}
	Ok(())
}

pub struct ExternCOperatorLoader {
	cache: LibraryCache,

	operator_paths: HashMap<String, PathBuf>,
}

impl ExternCOperatorLoader {
	fn new() -> Self {
		Self {
			cache: LibraryCache::new(),
			operator_paths: HashMap::new(),
		}
	}

	pub fn load_operator_library(&mut self, path: &Path) -> ExternCResult<bool> {
		self.cache
			.check_magic(path, b"extern_c_operator_magic\0", OPERATOR_MAGIC)
			.map_err(|e| SdkError::Other(e.to_string()))
	}

	fn get_descriptor(&self, path: &Path) -> ExternCResult<ExternCOperatorDescriptor> {
		let library = self
			.cache
			.get(path)
			.ok_or_else(|| SdkError::Other(format!("Library not loaded: {}", path.display())))?;
		// SAFETY: the operator ABI declares this symbol; the descriptor is module-static data.
		unsafe {
			let get_descriptor: Symbol<extern "C" fn() -> *const ExternCOperatorDescriptor> =
				library.get(b"extern_c_operator_get_descriptor\0").map_err(|e| {
					SdkError::Other(format!("Failed to find extern_c_operator_get_descriptor: {}", e))
				})?;

			let descriptor_ptr = get_descriptor();
			if descriptor_ptr.is_null() {
				return Err(SdkError::Other("Descriptor is null".to_string()));
			}

			Ok(ExternCOperatorDescriptor {
				api: (*descriptor_ptr).api,
				abi_tag: (*descriptor_ptr).abi_tag,
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

	fn validate_and_register(
		&mut self,
		descriptor: &ExternCOperatorDescriptor,
		path: &Path,
	) -> ExternCResult<(String, u32)> {
		validate_api_version(descriptor.api).map_err(|e| SdkError::Other(e.to_string()))?;

		check_operator_abi_tag(descriptor.abi_tag)?;

		// SAFETY: the buffer points into the loaded image's static data, which outlives this read.
		let operator = unsafe { buffer_to_string(&descriptor.operator) };
		self.operator_paths.insert(operator.clone(), path.to_path_buf());

		Ok((operator, descriptor.api))
	}

	pub fn register_operator(&mut self, path: &Path) -> ExternCResult<Option<LoadedOperatorInfo>> {
		if !self.load_operator_library(path)? {
			return Ok(None);
		}

		let descriptor = self.get_descriptor(path)?;
		let (operator, api) = self.validate_and_register(&descriptor, path)?;

		// SAFETY: the descriptor's buffers and column arrays are module-static data.
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

	pub fn load_operator(
		&mut self,
		path: &Path,
		config: &[u8],
		operator_id: OperatorId,
	) -> ExternCResult<Option<(ExternCOperatorDescriptor, *mut c_void)>> {
		if !self.load_operator_library(path)? {
			return Ok(None);
		}

		let descriptor = self.get_descriptor(path)?;
		self.validate_and_register(&descriptor, path)?;

		let library = self.cache.get(path).unwrap();
		// SAFETY: the ABI declares this symbol as ExternCOperatorCreateFn and the cache keeps it loaded.
		let create_fn: ExternCOperatorCreateFn = unsafe {
			let create_symbol: Symbol<ExternCOperatorCreateFn> = library
				.get(b"extern_c_operator_create\0")
				.map_err(|e| SdkError::Other(format!("Failed to find extern_c_operator_create: {}", e)))?;

			*create_symbol
		};

		let instance = create_fn(config.as_ptr(), config.len(), operator_id.0);
		if instance.is_null() {
			return Err(SdkError::Other("Failed to create operator instance".to_string()));
		}

		Ok(Some((descriptor, instance)))
	}

	pub fn create_operator_by_name(
		&mut self,
		operator: &str,
		operator_id: OperatorId,
		config: &[u8],
	) -> ExternCResult<(ExternCOperatorDescriptor, *mut c_void)> {
		let path = self
			.operator_paths
			.get(operator)
			.ok_or_else(|| SdkError::Other(format!("Operator not found: {}", operator)))?
			.clone();

		self.load_operator(&path, config, operator_id)?
			.ok_or_else(|| SdkError::Other(format!("Operator library no longer valid: {}", operator)))
	}

	pub fn has_operator(&self, operator: &str) -> bool {
		self.operator_paths.contains_key(operator)
	}

	pub fn list_loaded_operators(&self) -> Vec<LoadedOperatorInfo> {
		let mut operators = Vec::new();

		for path in self.operator_paths.values() {
			if let Ok(descriptor) = self.get_descriptor(path) {
				// SAFETY: the descriptor's buffers and arrays are module-static data.
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

#[derive(Debug, Clone)]
pub struct ColumnInfo {
	pub name: String,
	pub field_type: TypeConstraint,
	pub description: String,
}

/// # Safety
/// `columns` must address `column_count` initialized `ExternCOperatorColumn`, each with buffers valid for the
/// duration of the call.
unsafe fn extract_column_defs(column_defs: &ExternCOperatorColumns) -> Vec<ColumnInfo> {
	if column_defs.columns.is_null() || column_defs.column_count == 0 {
		return Vec::new();
	}

	let mut columns = Vec::with_capacity(column_defs.column_count);
	for i in 0..column_defs.column_count {
		// SAFETY: i < column_count and the pointer is non-null, so this stays inside the array.
		let col = unsafe { &*column_defs.columns.add(i) };

		let field_type = type_constraint_from_extern_c(&ExternCTypeConstraint {
			base_type: col.base_type,
			constraint_type: col.constraint_type,
			constraint_param1: col.constraint_param1,
			constraint_param2: col.constraint_param2,
		})
		.expect("invalid persisted type constraint tag");

		// SAFETY: both buffers belong to `col`, whose validity the caller guarantees.
		columns.push(ColumnInfo {
			name: unsafe { buffer_to_string(&col.name) },
			field_type,
			description: unsafe { buffer_to_string(&col.description) },
		});
	}

	columns
}

impl Default for ExternCOperatorLoader {
	fn default() -> Self {
		Self::new()
	}
}
