// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::{context::context::ContextFFI, data::column::ColumnsFFI};

/// Virtual function table for FFI transforms
///
/// Transforms are stateless Columns -> Columns operations. The host supplies
/// a `ContextFFI` so the guest can emit output columns through the
/// `BuilderCallbacks` (zero-copy: guest writes into host-pool-owned buffers).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TransformVTableFFI {
	/// Apply the transform to input columns
	///
	/// # Parameters
	/// - `instance`: The transform instance pointer
	/// - `ctx`: FFI context (host callbacks, clock, txn ptr)
	/// - `input`: Input columns (zero-copy borrow)
	///
	/// The guest emits its output via the `builder` callbacks on `ctx`; the
	/// host drains the registry after this call returns.
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub transform:
		unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI, input: *const ColumnsFFI) -> i32,

	/// Destroy a transform instance and free its resources
	///
	/// # Parameters
	/// - `instance`: The transform instance pointer to destroy
	///
	/// # Safety
	/// - The instance pointer must have been created by this transform's create function
	/// - The instance must not be used after calling destroy
	/// - This function must be called exactly once per instance
	pub destroy: unsafe extern "C" fn(instance: *mut c_void),
}
