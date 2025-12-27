use core::ffi::c_void;

use crate::{context::ContextFFI, data::ColumnsFFI, flow::FlowChangeFFI};

/// Virtual function table for FFI operators
///
/// This unified interface provides all methods an operator might need.
/// Operators that don't use certain features (e.g., state) simply won't
/// call those methods. All function pointers must be valid (non-null).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct OperatorVTableFFI {
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
		ctx: *mut ContextFFI,
		input: *const FlowChangeFFI,
		output: *mut FlowChangeFFI,
	) -> i32,

	/// Pull specific rows by their row numbers
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
	pub pull: extern "C" fn(
		instance: *mut c_void,
		ctx: *mut ContextFFI,
		row_numbers: *const u64,
		count: usize,
		output: *mut ColumnsFFI,
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
