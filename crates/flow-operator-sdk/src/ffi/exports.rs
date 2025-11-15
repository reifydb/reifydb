// Redesigned FFI exports that work with static metadata

use std::{
	collections::HashMap,
	ffi::{CString, c_char, c_void},
	ptr,
};

use reifydb_core::interface::FlowNodeId;
use reifydb_flow_operator_abi::{CURRENT_API_VERSION, FFIOperatorDescriptor};

use crate::{
	ffi::wrapper::{OperatorWrapper, create_vtable},
	operator::FFIOperatorWithMetadata,
};

pub fn create_descriptor<O: FFIOperatorWithMetadata>() -> FFIOperatorDescriptor {
	let name_cstring = CString::new(O::NAME).unwrap_or_else(|_| CString::new("unknown").unwrap());
	let name_ptr = Box::into_raw(Box::new(name_cstring)) as *const c_char;

	FFIOperatorDescriptor {
		api_version: CURRENT_API_VERSION,
		operator_name: name_ptr,
		capabilities: O::CAPABILITIES.to_ffi_flags(),
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

	// Create operator using new() - no Default required!
	let mut operator = O::new();

	// Initialize with configuration
	if let Err(e) = operator.initialize(&config) {
		eprintln!("Failed to initialize operator: {}", e);
		return ptr::null_mut();
	}

	// Wrap in FFI wrapper with proper node_id
	let wrapper = Box::new(OperatorWrapper::new(operator, FlowNodeId(operator_id)));
	Box::into_raw(wrapper) as *mut c_void
}
