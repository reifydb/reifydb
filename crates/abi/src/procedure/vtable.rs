// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::context::context::ContextFFI;

/// Virtual function table for FFI procedures
///
/// Procedures receive params as postcard-serialized bytes. They have access to
/// a ContextFFI for executing RQL within the current transaction. Output columns
/// are emitted via the `builder` callbacks on `ctx` (zero-copy: the guest writes
/// into host-pool-owned buffers and the host drains the registry after the call
/// returns).
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
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub call: unsafe extern "C" fn(
		instance: *mut c_void,
		ctx: *mut ContextFFI,
		params_ptr: *const u8,
		params_len: usize,
	) -> i32,

	/// Destroy a procedure instance and free its resources
	///
	/// # Parameters
	/// - `instance`: The procedure instance pointer to destroy
	pub destroy: unsafe extern "C" fn(instance: *mut c_void),
}
