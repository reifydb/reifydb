// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::{context::context::ContextFFI, data::column::ColumnsFFI};

/// Virtual function table for FFI procedures
///
/// Procedures receive params as postcard-serialized bytes and return Columns.
/// They have access to a ContextFFI for executing RQL within the current transaction.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ProcedureVTableFFI {
	/// Call the procedure
	///
	/// # Parameters
	/// - `instance`: The procedure instance pointer
	/// - `ctx`: FFI context (carries txn_ptr, executor_ptr, callbacks)
	/// - `params_ptr`: Postcard-serialized Params bytes
	/// - `params_len`: Length of params bytes
	/// - `output`: Output columns (to be filled by procedure)
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub call: extern "C" fn(
		instance: *mut c_void,
		ctx: *mut ContextFFI,
		params_ptr: *const u8,
		params_len: usize,
		output: *mut ColumnsFFI,
	) -> i32,

	/// Destroy a procedure instance and free its resources
	///
	/// # Parameters
	/// - `instance`: The procedure instance pointer to destroy
	pub destroy: extern "C" fn(instance: *mut c_void),
}
