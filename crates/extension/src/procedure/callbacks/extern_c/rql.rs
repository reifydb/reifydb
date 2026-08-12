// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ffi::c_void, panic, ptr, slice, str};

use reifydb_codec::{
	frame::{encode::encode_frames, options::EncodeOptions},
	value::decode_params,
};
use reifydb_sdk::{
	common::extern_c::wire::{
		buffer::ExternCBuffer,
		status::{EXTERN_C_ERROR_INTERNAL, EXTERN_C_ERROR_INVALID_UTF8, EXTERN_C_OK},
	},
	procedure::extern_c::wire::context::ExternCContextRaw,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::params::Params;
use tracing::error;

use super::memory::host_alloc;

/// Runs an RQL statement on the caller's transaction and writes host-allocated encoded frames, or the error
/// message on failure, into `result_out`.
///
/// # Safety
///
/// - `ctx` must be a valid pointer to a procedure `ExternCContextRaw` whose `txn_ptr` points to a live `Transaction`.
/// - `rql_ptr` must be valid for reading `rql_len` bytes of valid UTF-8.
/// - `params_ptr` must be valid for reading `params_len` bytes, or null if `params_len` is 0.
/// - `result_out` must be a valid pointer to a `ExternCBuffer` for writing.
pub unsafe extern "C" fn host_rql(
	ctx: *mut c_void,
	rql_ptr: *const u8,
	rql_len: usize,
	params_ptr: *const u8,
	params_len: usize,
	result_out: *mut ExternCBuffer,
) -> i32 {
	let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
		if ctx.is_null() || rql_ptr.is_null() || result_out.is_null() {
			return EXTERN_C_ERROR_INTERNAL;
		}

		// SAFETY: the three pointers are non-null here and txn_ptr addresses a live Transaction.
		unsafe {
			let rql_bytes = slice::from_raw_parts(rql_ptr, rql_len);
			let rql_str = match str::from_utf8(rql_bytes) {
				Ok(s) => s,
				Err(_) => return EXTERN_C_ERROR_INVALID_UTF8,
			};

			let params: Params = if params_ptr.is_null() || params_len == 0 {
				Params::None
			} else {
				let params_bytes = slice::from_raw_parts(params_ptr, params_len);
				match decode_params(params_bytes) {
					Ok(p) => p,
					Err(e) => {
						error!("host_rql: failed to deserialize params: {}", e);
						return EXTERN_C_ERROR_INTERNAL;
					}
				}
			};

			let ctx_ref = &mut *(ctx as *mut ExternCContextRaw);
			let tx = &mut *(ctx_ref.txn_ptr as *mut Transaction<'_>);

			let result = tx.rql(rql_str, params);
			if let Some(ref e) = result.error {
				error!("host_rql: rql execution failed: {}", e);
				let msg = e.to_string();
				let msg_bytes = msg.as_bytes();
				let out_ptr = host_alloc(msg_bytes.len());
				if !out_ptr.is_null() {
					ptr::copy_nonoverlapping(msg_bytes.as_ptr(), out_ptr, msg_bytes.len());
					*result_out = ExternCBuffer {
						ptr: out_ptr,
						len: msg_bytes.len(),
						cap: msg_bytes.len(),
					};
				}
				return EXTERN_C_ERROR_INTERNAL;
			}

			let result_bytes = match encode_frames(&result.frames, &EncodeOptions::fast()) {
				Ok(b) => b,
				Err(e) => {
					error!("host_rql: failed to serialize result: {}", e);
					return EXTERN_C_ERROR_INTERNAL;
				}
			};

			let out_ptr = host_alloc(result_bytes.len());
			if out_ptr.is_null() && !result_bytes.is_empty() {
				return EXTERN_C_ERROR_INTERNAL;
			}
			if !result_bytes.is_empty() {
				ptr::copy_nonoverlapping(result_bytes.as_ptr(), out_ptr, result_bytes.len());
			}

			*result_out = ExternCBuffer {
				ptr: out_ptr,
				len: result_bytes.len(),
				cap: result_bytes.len(),
			};

			EXTERN_C_OK
		}
	}));
	match result {
		Ok(code) => code,
		Err(_) => {
			error!("host_rql: panic caught in extern-C callback");
			EXTERN_C_ERROR_INTERNAL
		}
	}
}
