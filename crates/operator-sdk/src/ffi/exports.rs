//! FFI export helpers

use crate::ffi::wrapper::{create_vtable, OperatorWrapper};
use crate::operator::Operator;
use reifydb_core::interface::FlowNodeId;
use reifydb_operator_abi::{FFIOperatorDescriptor, CURRENT_API_VERSION};
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CString};

/// Create an FFI descriptor for an operator type
pub fn create_descriptor<O: Operator>() -> FFIOperatorDescriptor {
	// Get metadata from a temporary instance
	let temp_op = create_temp_instance::<O>();
	let metadata = temp_op.metadata();

	// Create operator name as C string
	let name_cstring = CString::new(metadata.name).unwrap_or_else(|_| CString::new("unknown").unwrap());
	let name_ptr = Box::into_raw(Box::new(name_cstring)) as *const c_char;

	FFIOperatorDescriptor {
		api_version: CURRENT_API_VERSION,
		operator_name: name_ptr,
		capabilities: metadata.capabilities.to_ffi_flags(),
		vtable: create_vtable::<O>(),
	}
}

/// Create an operator instance from FFI parameters
pub fn create_operator_instance<O: Operator>() -> *mut c_void {
	// Create operator
	let operator = match create_and_initialize::<O>() {
		Ok(op) => op,
		Err(_) => return std::ptr::null_mut(),
	};

	// Wrap in FFI wrapper
	let mut wrapper = Box::new(OperatorWrapper::new(operator, FlowNodeId(0)));
	let ptr = wrapper.as_ptr();

	// Leak the box so it stays alive
	Box::leak(wrapper);

	ptr
}

/// Create and initialize an operator
fn create_and_initialize<O: Operator>() -> Result<O, crate::error::Error> {
	let mut operator = create_temp_instance::<O>();
	operator.initialize(&HashMap::new())?;
	Ok(operator)
}

/// Create a temporary instance of an operator for metadata extraction
fn create_temp_instance<O: Operator>() -> O {
	// This is a bit of a hack - we need a way to create an instance
	// without knowing the constructor. For now, we'll use Default if available,
	// or panic with a helpful message.

	// Try to use Default if implemented
	if let Some(op) = try_default::<O>() {
		return op;
	}

	// If not, we need a better solution
	panic!("Operator must implement Default trait or provide a no-arg constructor. \
         Consider adding #[derive(Default)] to your operator struct.");
}

/// Try to create an instance using Default trait
fn try_default<O: Operator>() -> Option<O> {
	// This would need to be implemented with a trait bound
	// For now, return None
	None
}

/// Helper trait for operators that can be created with default values
pub trait DefaultOperator: Operator + Default {
	fn create() -> Self {
		Self::default()
	}
}

impl<T: Operator + Default> DefaultOperator for T {}
