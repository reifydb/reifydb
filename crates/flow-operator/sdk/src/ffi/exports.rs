// Redesigned FFI exports that work with static metadata

use std::{
	collections::HashMap,
	ffi::{CString, c_void},
	ptr, slice,
};

use reifydb_core::interface::FlowNodeId;
use reifydb_flow_operator_abi::{CURRENT_API_VERSION, FFIOperatorDescriptor, OPERATOR_MAGIC};
use reifydb_type::Value;

use crate::{
	FFIOperatorWithMetadata,
	ffi::wrapper::{OperatorWrapper, create_vtable},
};

pub fn create_descriptor<O: FFIOperatorWithMetadata>() -> FFIOperatorDescriptor {
	let name_cstring = CString::new(O::NAME).unwrap_or_else(|_| CString::new("unknown").unwrap());
	// Leak the CString and get a pointer to its internal C string data
	let name_ptr = Box::leak(Box::new(name_cstring)).as_ptr();

	FFIOperatorDescriptor {
		api_version: CURRENT_API_VERSION,
		operator_name: name_ptr,
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
