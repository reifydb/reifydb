// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// Redesigned FFI exports that work with static metadata

use std::{collections::HashMap, ffi::c_void, ptr, slice};

use reifydb_abi::{
	constants::CURRENT_API,
	data::buffer::BufferFFI,
	operator::{
		column::{OperatorColumnDefFFI, OperatorColumnDefsFFI},
		descriptor::OperatorDescriptorFFI,
		types::OPERATOR_MAGIC,
	},
};
use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_type::value::Value;

use crate::{
	ffi::wrapper::{OperatorWrapper, create_vtable},
	operator::{FFIOperatorWithMetadata, column::OperatorColumnDef},
};

/// Convert a static string to a BufferFFI
fn str_to_buffer(s: &'static str) -> BufferFFI {
	BufferFFI {
		ptr: s.as_ptr(),
		len: s.len(),
		cap: s.len(),
	}
}

/// Convert operator column definitions to FFI representation
fn columns_to_ffi(columns: &'static [OperatorColumnDef]) -> OperatorColumnDefsFFI {
	if columns.is_empty() {
		return OperatorColumnDefsFFI::empty();
	}

	let ffi_columns: Vec<OperatorColumnDefFFI> = columns
		.iter()
		.map(|c| {
			let ffi_type = c.field_type.to_ffi();
			OperatorColumnDefFFI {
				name: str_to_buffer(c.name),
				base_type: ffi_type.base_type,
				constraint_type: ffi_type.constraint_type,
				constraint_param1: ffi_type.constraint_param1,
				constraint_param2: ffi_type.constraint_param2,
				description: str_to_buffer(c.description),
			}
		})
		.collect();

	let column_count = ffi_columns.len();
	let columns_ptr = Box::leak(ffi_columns.into_boxed_slice()).as_ptr();

	OperatorColumnDefsFFI {
		columns: columns_ptr,
		column_count,
	}
}

pub fn create_descriptor<O: FFIOperatorWithMetadata>() -> OperatorDescriptorFFI {
	OperatorDescriptorFFI {
		api: CURRENT_API,
		operator: str_to_buffer(O::NAME),
		version: str_to_buffer(O::VERSION),
		description: str_to_buffer(O::DESCRIPTION),
		input_columns: columns_to_ffi(O::INPUT_COLUMNS),
		output_columns: columns_to_ffi(O::OUTPUT_COLUMNS),
		capabilities: O::CAPABILITIES,
		vtable: create_vtable::<O>(),
	}
}

/// Create an operator instance from FFI parameters
///
/// # Safety
/// - config_ptr must be valid for config_len bytes or null
/// - The returned pointer must be freed by calling the destroy function
pub unsafe extern "C" fn create_operator_instance<O: FFIOperatorWithMetadata>(
	config_ptr: *const u8,
	config_len: usize,
	operator_id: u64,
) -> *mut c_void {
	// Deserialize configuration from postcard if provided
	let config = if config_ptr.is_null() || config_len == 0 {
		// No configuration provided, use empty HashMap
		HashMap::new()
	} else {
		// SAFETY: caller guarantees config_ptr is valid for config_len bytes
		let config_bytes = unsafe { slice::from_raw_parts(config_ptr, config_len) };

		match postcard::from_bytes::<HashMap<String, Value>>(config_bytes) {
			Ok(decoded_config) => decoded_config,
			Err(e) => {
				panic!(
					"Failed to deserialize operator config for operator {}: {}. Using empty config.",
					operator_id, e
				);
			}
		}
	};

	// Create operator with ID and config
	let operator = match O::new(FlowNodeId(operator_id), &config) {
		Ok(op) => op,
		Err(e) => {
			eprintln!("Failed to create operator: {}", e);
			return ptr::null_mut();
		}
	};

	// Wrap in FFI wrapper
	let wrapper = Box::new(OperatorWrapper::new(operator));
	Box::into_raw(wrapper) as *mut c_void
}

/// Returns the operator magic number
///
/// FFI operator libraries must export this function as `ffi_operator_magic`
/// to be recognized as valid operators by the loader.
///
/// # Example
/// ```ignore
/// #[unsafe(no_mangle)]
/// pub extern "C" fn ffi_operator_magic() -> u32 {
///     reifydb_flow_operator_sdk::ffi::exports::operator_magic()
/// }
/// ```
pub extern "C" fn operator_magic() -> u32 {
	OPERATOR_MAGIC
}
