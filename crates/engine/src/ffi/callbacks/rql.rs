// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Real host_rql callback implementation for FFI procedures
//!
//! Allows FFI procedures to execute RQL within the current transaction.

use reifydb_abi::{
	constants::{FFI_ERROR_INTERNAL, FFI_ERROR_INVALID_UTF8, FFI_OK},
	context::context::ContextFFI,
	data::buffer::BufferFFI,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{params::Params, value::identity::IdentityId};
use tracing::error;

use super::memory::host_alloc;
use crate::vm::executor::Executor;

/// Host RQL callback for FFI procedures.
///
/// Reconstructs the Transaction and Executor from the ContextFFI pointers,
/// executes the RQL statement, and serializes the result frames into the output buffer.
pub extern "C" fn host_rql(
	ctx: *mut ContextFFI,
	rql_ptr: *const u8,
	rql_len: usize,
	params_ptr: *const u8,
	params_len: usize,
	result_out: *mut BufferFFI,
) -> i32 {
	let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		if ctx.is_null() || rql_ptr.is_null() || result_out.is_null() {
			return FFI_ERROR_INTERNAL;
		}

		unsafe {
			// Reconstruct RQL string
			let rql_bytes = std::slice::from_raw_parts(rql_ptr, rql_len);
			let rql_str = match std::str::from_utf8(rql_bytes) {
				Ok(s) => s,
				Err(_) => return FFI_ERROR_INVALID_UTF8,
			};

			// Deserialize params
			let params: Params = if params_ptr.is_null() || params_len == 0 {
				Params::None
			} else {
				let params_bytes = std::slice::from_raw_parts(params_ptr, params_len);
				match postcard::from_bytes(params_bytes) {
					Ok(p) => p,
					Err(e) => {
						error!("host_rql: failed to deserialize params: {}", e);
						return FFI_ERROR_INTERNAL;
					}
				}
			};

			// Reconstruct Transaction and Executor from context pointers
			let ctx_ref = &mut *ctx;
			let tx = &mut *(ctx_ref.txn_ptr as *mut Transaction<'_>);
			let executor = &*(ctx_ref.executor_ptr as *const Executor);

			// Execute RQL
			let frames = match executor.rql(tx, IdentityId::root(), rql_str, params) {
				Ok(f) => f,
				Err(e) => {
					error!("host_rql: rql execution failed: {}", e);
					return FFI_ERROR_INTERNAL;
				}
			};

			// Serialize result frames with postcard
			let result_bytes = match postcard::to_stdvec(&frames) {
				Ok(b) => b,
				Err(e) => {
					error!("host_rql: failed to serialize result: {}", e);
					return FFI_ERROR_INTERNAL;
				}
			};

			// Copy result into output buffer using host_alloc
			let out_ptr = host_alloc(result_bytes.len());
			if out_ptr.is_null() && !result_bytes.is_empty() {
				return FFI_ERROR_INTERNAL;
			}
			if !result_bytes.is_empty() {
				std::ptr::copy_nonoverlapping(result_bytes.as_ptr(), out_ptr, result_bytes.len());
			}

			*result_out = BufferFFI {
				ptr: out_ptr,
				len: result_bytes.len(),
				cap: result_bytes.len(),
			};

			FFI_OK
		}
	}));
	match result {
		Ok(code) => code,
		Err(_) => {
			error!("host_rql: panic caught in FFI callback");
			FFI_ERROR_INTERNAL
		}
	}
}
