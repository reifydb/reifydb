// Redesigned FFI exports that work with static metadata

use std::{collections::HashMap, ffi::c_void, ptr, slice};

use reifydb_core::interface::FlowNodeId;
use reifydb_flow_operator_abi::{
	BufferFFI, CURRENT_API_VERSION, FFIOperatorColumnDef, FFIOperatorColumnDefs, FFIOperatorDescriptor,
	OPERATOR_MAGIC,
};
use reifydb_type::Value;

use crate::{
	FFIOperatorWithMetadata, OperatorColumnDef,
	ffi::wrapper::{OperatorWrapper, create_vtable},
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
fn columns_to_ffi(columns: &'static [OperatorColumnDef]) -> FFIOperatorColumnDefs {
	if columns.is_empty() {
		return FFIOperatorColumnDefs::empty();
	}

	let ffi_columns: Vec<FFIOperatorColumnDef> = columns
		.iter()
		.map(|c| {
			let ffi_type = c.field_type.to_ffi();
			FFIOperatorColumnDef {
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

	FFIOperatorColumnDefs {
		columns: columns_ptr,
		column_count,
	}
}

pub fn create_descriptor<O: FFIOperatorWithMetadata>() -> FFIOperatorDescriptor {
	FFIOperatorDescriptor {
		api_version: CURRENT_API_VERSION,
		operator_name: str_to_buffer(O::NAME),
		operator_version: str_to_buffer(O::VERSION),
		operator_description: str_to_buffer(O::DESCRIPTION),
		input_columns: columns_to_ffi(O::INPUT_COLUMNS),
		output_columns: columns_to_ffi(O::OUTPUT_COLUMNS),
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
	// Deserialize configuration from bincode if provided
	let config = if config_ptr.is_null() || config_len == 0 {
		// No configuration provided, use empty HashMap
		HashMap::new()
	} else {
		// SAFETY: caller guarantees config_ptr is valid for config_len bytes
		let config_bytes = unsafe { slice::from_raw_parts(config_ptr, config_len) };

		match bincode::serde::decode_from_slice::<HashMap<String, Value>, _>(
			config_bytes,
			bincode::config::standard(),
		) {
			Ok((decoded_config, _bytes_read)) => decoded_config,
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
