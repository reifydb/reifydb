// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	ffi::c_void,
	path::{Path, PathBuf},
	sync::OnceLock,
};

use libloading::Symbol;
use reifydb_codec::constraint::{EncodedTypeConstraint, decode_type_constraint};
use reifydb_core::{
	common::{OperatorClass, WindowRequirements, WindowSizeDomain},
	interface::catalog::flow::OperatorId,
};
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_sdk::{
	common::extern_c::wire::buffer::ExternCBuffer,
	error::{Result as ExternCResult, SdkError},
	flow::{
		extern_c::wire::schema::ExternCOperatorColumns,
		operator::extern_c::wire::{
			descriptor::{ExternCOperatorDescriptor, ExternCWindowRequirements},
			types::{ExternCOperatorCreateFn, OPERATOR_ABI_TAG, OPERATOR_MAGIC},
		},
	},
};
use reifydb_value::value::constraint::TypeConstraint;

use crate::loader::{extern_c::buffer_to_string, extern_load::ExternLoad};

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
	cache: ExternLoad,

	operator_paths: HashMap<String, PathBuf>,
}

impl ExternCOperatorLoader {
	fn new() -> Self {
		Self {
			cache: ExternLoad::new(),
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
					SdkError::Other(format!(
						"Failed to find extern_c_operator_get_descriptor: {}",
						e
					))
				})?;

			let descriptor_ptr = get_descriptor();
			if descriptor_ptr.is_null() {
				return Err(SdkError::Other("Descriptor is null".to_string()));
			}

			Ok(ExternCOperatorDescriptor {
				abi_tag: (*descriptor_ptr).abi_tag,
				operator: (*descriptor_ptr).operator,
				version: (*descriptor_ptr).version,
				description: (*descriptor_ptr).description,
				input_columns: (*descriptor_ptr).input_columns,
				output_columns: (*descriptor_ptr).output_columns,
				capabilities: (*descriptor_ptr).capabilities,
				class: (*descriptor_ptr).class,
				unmanaged_because: (*descriptor_ptr).unmanaged_because,
				window: (*descriptor_ptr).window,
				vtable: (*descriptor_ptr).vtable,
			})
		}
	}

	fn validate_and_register(
		&mut self,
		descriptor: &ExternCOperatorDescriptor,
		path: &Path,
	) -> ExternCResult<(String, u32)> {
		check_operator_abi_tag(descriptor.abi_tag)?;

		// SAFETY: the buffer points into the loaded image's static data, which outlives this read.
		let operator = unsafe { buffer_to_string(&descriptor.operator) };
		self.operator_paths.insert(operator.clone(), path.to_path_buf());

		Ok((operator, descriptor.abi_tag))
	}

	pub fn register_operator(&mut self, path: &Path) -> ExternCResult<Option<LoadedOperatorInfo>> {
		if !self.load_operator_library(path)? {
			return Ok(None);
		}

		let descriptor = self.get_descriptor(path)?;
		let (operator, abi) = self.validate_and_register(&descriptor, path)?;
		let class = decode_class(descriptor.class)?;
		let window = decode_window(&descriptor.window)?;

		// SAFETY: the descriptor's buffers and column arrays are module-static data.
		let info = unsafe {
			LoadedOperatorInfo {
				operator,
				library_path: path.to_path_buf(),
				abi,
				version: buffer_to_string(&descriptor.version),
				description: buffer_to_string(&descriptor.description),
				input_columns: extract_column_defs(&descriptor.input_columns),
				output_columns: extract_column_defs(&descriptor.output_columns),
				capabilities: descriptor.capabilities,
				class,
				window,
				unmanaged_because: decode_unmanaged_because(&descriptor.unmanaged_because),
			}
		};

		Ok(Some(info))
	}

	pub fn load_operator(
		&mut self,
		path: &Path,
		params: &[u8],
		with: &[u8],
		operator_id: OperatorId,
	) -> ExternCResult<Option<(ExternCOperatorDescriptor, OperatorClass, *mut c_void)>> {
		if !self.load_operator_library(path)? {
			return Ok(None);
		}

		let descriptor = self.get_descriptor(path)?;
		self.validate_and_register(&descriptor, path)?;
		let class = decode_class(descriptor.class)?;

		let library = self.cache.library(path).map_err(|e| SdkError::Other(e.to_string()))?;
		// SAFETY: the ABI declares this symbol as ExternCOperatorCreateFn and the cache keeps it loaded.
		let create_fn: ExternCOperatorCreateFn = unsafe {
			let create_symbol: Symbol<ExternCOperatorCreateFn> =
				library.get(b"extern_c_operator_create\0").map_err(|e| {
					SdkError::Other(format!("Failed to find extern_c_operator_create: {}", e))
				})?;

			*create_symbol
		};

		let instance = create_fn(params.as_ptr(), params.len(), with.as_ptr(), with.len(), operator_id.0);
		if instance.is_null() {
			return Err(SdkError::Other("Failed to create operator instance".to_string()));
		}

		Ok(Some((descriptor, class, instance)))
	}

	pub fn create_operator_by_name(
		&mut self,
		operator: &str,
		operator_id: OperatorId,
		params: &[u8],
		with: &[u8],
	) -> ExternCResult<(ExternCOperatorDescriptor, OperatorClass, *mut c_void)> {
		let path = self
			.operator_paths
			.get(operator)
			.ok_or_else(|| SdkError::Other(format!("Operator not found: {}", operator)))?
			.clone();

		self.load_operator(&path, params, with, operator_id)?
			.ok_or_else(|| SdkError::Other(format!("Operator library no longer valid: {}", operator)))
	}

	pub fn has_operator(&self, operator: &str) -> bool {
		self.operator_paths.contains_key(operator)
	}
}

#[derive(Debug, Clone)]
pub struct LoadedOperatorInfo {
	pub operator: String,
	pub library_path: PathBuf,
	pub abi: u32,
	pub version: String,
	pub description: String,
	pub input_columns: Vec<ColumnInfo>,
	pub output_columns: Vec<ColumnInfo>,
	pub capabilities: u32,
	pub class: OperatorClass,
	pub window: WindowRequirements,
	pub unmanaged_because: Option<String>,
}

pub fn decode_class(class: u8) -> ExternCResult<OperatorClass> {
	OperatorClass::from_u8(class)
		.ok_or_else(|| SdkError::Other(format!("extern-C operator declares an unknown class {}", class)))
}

pub fn decode_window(window: &ExternCWindowRequirements) -> ExternCResult<WindowRequirements> {
	let kinds = WindowRequirements::kinds_from_bitmask(window.kinds).ok_or_else(|| {
		SdkError::Other(format!("extern-C operator declares unknown window kinds {:#x}", window.kinds))
	})?;
	let domain = WindowSizeDomain::from_u8(window.domain).ok_or_else(|| {
		SdkError::Other(format!("extern-C operator declares an unknown window size domain {}", window.domain))
	})?;
	Ok(WindowRequirements {
		takes_window: window.takes_window != 0,
		kinds,
		domain,
		needs_pane: window.needs_pane != 0,
		throttles: window.throttles != 0,
	})
}

/// # Safety
/// `reason.ptr` must be null, or valid for reads of `reason.len` bytes for the duration of the call.
pub unsafe fn decode_unmanaged_because(reason: &ExternCBuffer) -> Option<String> {
	// SAFETY: a non-null pointer is valid for `reason.len` bytes by this function's contract.
	(!reason.ptr.is_null()).then(|| unsafe { buffer_to_string(reason) })
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

		let field_type = decode_type_constraint(&EncodedTypeConstraint {
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

#[cfg(test)]
mod tests {
	use core::ptr;

	use reifydb_core::common::{OperatorClass, WindowSizeDomain};
	use reifydb_sdk::{
		common::extern_c::wire::buffer::ExternCBuffer,
		flow::operator::extern_c::wire::descriptor::ExternCWindowRequirements,
	};

	use super::{decode_class, decode_unmanaged_because, decode_window};

	fn window(kinds: u32, domain: u8) -> ExternCWindowRequirements {
		ExternCWindowRequirements {
			takes_window: 1,
			kinds,
			domain,
			needs_pane: 1,
			throttles: 1,
		}
	}

	#[test]
	fn an_unknown_class_byte_fails_the_load() {
		// Reading an unknown byte as some class runs the guest under checks it never agreed to.
		assert_eq!(decode_class(2).unwrap(), OperatorClass::Unmanaged);
		assert!(decode_class(0).is_err());
		assert!(decode_class(5).is_err());
	}

	#[test]
	fn a_window_decodes_every_field_and_refuses_unknown_kinds_or_domain() {
		// An unknown bit read as a known kind lets CREATE accept a window the guest cannot run.
		let decoded = decode_window(&window(0b1011, 2)).unwrap();
		assert!(decoded.takes_window);
		assert_eq!(decoded.kinds, &["tumbling", "sliding", "rolling"]);
		assert_eq!(decoded.domain, WindowSizeDomain::Slots);
		assert!(decoded.needs_pane);
		assert!(decode_window(&window(0b1_0000, 1)).is_err());
		assert!(decode_window(&window(0b1, 0)).is_err());
	}

	#[test]
	fn a_window_decodes_its_throttle_byte() {
		// A throttle byte read as false makes the host refuse every throttled view of an extern-C driver.
		assert!(decode_window(&window(0b1, 1)).unwrap().throttles);
		let silent = ExternCWindowRequirements {
			throttles: 0,
			..window(0b1, 1)
		};
		assert!(!decode_window(&silent).unwrap().throttles);
	}

	#[test]
	fn only_a_non_null_reason_decodes_to_a_reason() {
		// A null read as an empty reason names an owner for an operator that is not unmanaged.
		let reason = "frees its own rows";
		let present = ExternCBuffer {
			ptr: reason.as_ptr(),
			len: reason.len(),
			cap: reason.len(),
		};
		let absent = ExternCBuffer {
			ptr: ptr::null(),
			len: 0,
			cap: 0,
		};
		// SAFETY: `present` points at a live `&'static str` of `len` bytes and `absent` is null.
		unsafe {
			assert_eq!(decode_unmanaged_because(&present).as_deref(), Some(reason));
			assert_eq!(decode_unmanaged_because(&absent), None);
		}
	}
}
