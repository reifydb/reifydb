// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{context::context::ContextFFI, data::buffer::BufferFFI};

/// RQL execution callbacks
///
/// Allows FFI operators to execute RQL statements within the current transaction context.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RqlCallbacks {
	/// Execute an RQL statement within the current transaction context.
	///
	/// # Parameters
	/// - `ctx`: FFI context (carries opaque txn_ptr + executor_ptr)
	/// - `rql_ptr`: UTF-8 RQL string
	/// - `rql_len`: Length of RQL string
	/// - `params_ptr`: JSON-serialized params (or null for no params)
	/// - `params_len`: Length of params
	/// - `result_out`: Buffer to receive JSON-serialized result frames
	///
	/// # Returns
	/// - FFI_OK on success, FFI_ERROR_* on failure
	pub rql: extern "C" fn(
		ctx: *mut ContextFFI,
		rql_ptr: *const u8,
		rql_len: usize,
		params_ptr: *const u8,
		params_len: usize,
		result_out: *mut BufferFFI,
	) -> i32,
}
