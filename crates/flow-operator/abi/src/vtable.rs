//! Virtual table definitions for FFI operators

use core::ffi::c_void;

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

	/// Destroy an operator instance and free its resources
	///
	/// # Parameters
	/// - `instance`: The operator instance pointer to destroy
	///
	/// # Safety
	/// - The instance pointer must have been created by this operator's create function
	/// - The instance must not be used after calling destroy
	/// - This function must be called exactly once per instance
	pub destroy: extern "C" fn(instance: *mut c_void),
}

/// Descriptor for an FFI operator
///
/// This structure describes an operator's capabilities and provides
/// its virtual function table.
#[repr(C)]
pub struct FFIOperatorDescriptor {
	/// API version (must match CURRENT_API_VERSION)
	pub api_version: u32,

	/// Operator name (UTF-8 encoded)
	pub operator_name: BufferFFI,

	/// Operator semantic version (UTF-8 encoded, e.g., "1.0.0")
	pub operator_version: BufferFFI,

	/// Operator description (UTF-8 encoded)
	pub operator_description: BufferFFI,

	/// Input columns describing expected input row format (for documentation)
	pub input_columns: FFIOperatorColumnDefs,

	/// Output columns describing output row format (for documentation)
	pub output_columns: FFIOperatorColumnDefs,

	/// Virtual function table with all operator methods
	pub vtable: FFIOperatorVTable,
}

// SAFETY: FFIOperatorDescriptor contains pointers to static strings and functions
// which are safe to share across threads
unsafe impl Send for FFIOperatorDescriptor {}
unsafe impl Sync for FFIOperatorDescriptor {}

/// Factory function type for creating operator instances
pub type FFIOperatorCreateFn = extern "C" fn(config: *const u8, config_len: usize, operator_id: u64) -> *mut c_void;
