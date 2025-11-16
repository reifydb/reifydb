//! Virtual table definitions for FFI operators

use core::ffi::{c_char, c_void};

use crate::types::*;

/// Virtual function table for FFI operators
///
/// This unified interface provides all methods an operator might need.
/// Operators that don't use certain features (e.g., state) simply won't
/// call those methods. All function pointers must be valid (non-null).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FFIOperatorVTable {
	/// Apply the operator to a flow change
	///
	/// # Parameters
	/// - `instance`: The operator instance pointer
	/// - `ctx`: FFI context for this operation
	/// - `input`: Input flow change
	/// - `output`: Output flow change (to be filled by operator)
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub apply: extern "C" fn(
		instance: *mut c_void,
		ctx: *mut FFIContext,
		input: *const FlowChangeFFI,
		output: *mut FlowChangeFFI,
	) -> i32,

	/// Get specific rows by their row numbers
	///
	/// # Parameters
	/// - `instance`: The operator instance pointer
	/// - `ctx`: FFI context for this operation
	/// - `row_numbers`: Array of row numbers to fetch
	/// - `count`: Number of row numbers
	/// - `output`: Output rows structure (to be filled)
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub get_rows: extern "C" fn(
		instance: *mut c_void,
		ctx: *mut FFIContext,
		row_numbers: *const u64,
		count: usize,
		output: *mut RowsFFI,
	) -> i32,
}

/// Descriptor for an FFI operator
///
/// This structure describes an operator's capabilities and provides
/// its virtual function table.
#[repr(C)]
pub struct FFIOperatorDescriptor {
	/// API version (must match CURRENT_API_VERSION)
	pub api_version: u32,

	/// Operator name (null-terminated C string)
	pub operator_name: *const c_char,

	/// Virtual function table with all operator methods
	pub vtable: FFIOperatorVTable,
}

// SAFETY: FFIOperatorDescriptor contains pointers to static strings and functions
// which are safe to share across threads
unsafe impl Send for FFIOperatorDescriptor {}
unsafe impl Sync for FFIOperatorDescriptor {}

/// Factory function type for creating operator instances
pub type FFIOperatorCreateFn = extern "C" fn(config: *const u8, config_len: usize, operator_id: u64) -> *mut c_void;
