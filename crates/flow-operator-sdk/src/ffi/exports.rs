// Redesigned FFI exports that work with static metadata

use std::{
	collections::HashMap,
	ffi::{CString, c_void},
	ptr,
};

use reifydb_core::interface::FlowNodeId;
use reifydb_flow_operator_abi::{CURRENT_API_VERSION, FFIOperatorDescriptor};

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
	_config_ptr: *const u8,
	_config_len: usize,
	operator_id: u64,
) -> *mut c_void {
	// Parse configuration if provided
	let config = HashMap::new();

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
