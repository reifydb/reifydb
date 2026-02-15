// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::data::column::ColumnsFFI;

/// Virtual function table for FFI transforms
///
/// Transforms are stateless Columns → Columns operations. Unlike operators,
/// they do not receive Change/Diff or a ContextFFI — they are pure data transformations.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TransformVTableFFI {
	/// Apply the transform to input columns
	///
	/// # Parameters
	/// - `instance`: The transform instance pointer
	/// - `input`: Input columns
	/// - `output`: Output columns (to be filled by transform)
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub transform: extern "C" fn(instance: *mut c_void, input: *const ColumnsFFI, output: *mut ColumnsFFI) -> i32,

	/// Destroy a transform instance and free its resources
	///
	/// # Parameters
	/// - `instance`: The transform instance pointer to destroy
	///
	/// # Safety
	/// - The instance pointer must have been created by this transform's create function
	/// - The instance must not be used after calling destroy
	/// - This function must be called exactly once per instance
	pub destroy: extern "C" fn(instance: *mut c_void),
}
