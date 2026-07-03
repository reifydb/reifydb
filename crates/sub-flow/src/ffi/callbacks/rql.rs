// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{panic, ptr, slice, str};

use reifydb_abi::{
	constants::{FFI_ERROR_INTERNAL, FFI_ERROR_INVALID_UTF8, FFI_OK},
	context::context::ContextFFI,
	data::buffer::BufferFFI,
};
use reifydb_codec::{
	frame::{encode::encode_frames, options::EncodeOptions},
	value::decode_params,
};
use reifydb_engine::vm::executor::Executor;
use reifydb_extension::procedure::ffi_callbacks::memory::host_alloc;
use reifydb_transaction::transaction::{Transaction, query::QueryTransaction};
use reifydb_value::{params::Params, value::identity::IdentityId};
use tracing::error;

use crate::ffi::context::get_transaction_mut;

/// # Safety
///
/// `ctx`, `rql_ptr`, and `result_out` must be valid non-null pointers for the duration of the
/// call. `rql_ptr` must point to `rql_len` valid UTF-8 bytes. If `params_ptr` is non-null it
/// must point to `params_len` valid bytes holding codec-encoded params.
pub unsafe extern "C" fn host_rql(
	ctx: *mut ContextFFI,
	rql_ptr: *const u8,
	rql_len: usize,
	params_ptr: *const u8,
	params_len: usize,
	result_out: *mut BufferFFI,
) -> i32 {
	let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
		if ctx.is_null() || rql_ptr.is_null() || result_out.is_null() {
			return FFI_ERROR_INTERNAL;
		}

		unsafe {
			let rql_bytes = slice::from_raw_parts(rql_ptr, rql_len);
			let rql_str = match str::from_utf8(rql_bytes) {
				Ok(s) => s,
				Err(_) => return FFI_ERROR_INVALID_UTF8,
			};

			let params: Params = if params_ptr.is_null() || params_len == 0 {
				Params::None
			} else {
				let params_bytes = slice::from_raw_parts(params_ptr, params_len);
				match decode_params(params_bytes) {
					Ok(p) => p,
					Err(e) => {
						error!("host_rql: failed to deserialize params: {}", e);
						return FFI_ERROR_INTERNAL;
					}
				}
			};

			let ctx_ref = &mut *ctx;

			if ctx_ref.executor_ptr.is_null() {
				error!("host_rql: executor_ptr is null");
				return FFI_ERROR_INTERNAL;
			}

			let executor = &*(ctx_ref.executor_ptr as *const Executor);
			let flow_txn = get_transaction_mut(ctx_ref);

			let cloned_multi = flow_txn.inner_mut().query.clone();
			let single = flow_txn.inner_mut().single.clone();
			let mut qt = QueryTransaction::new(cloned_multi, single, IdentityId::system());

			let exec_result = executor.rql(&mut Transaction::Query(&mut qt), rql_str, params);

			if let Some(ref e) = exec_result.error {
				error!("host_rql: rql execution failed: {}", e);
				let msg = e.to_string();
				let msg_bytes = msg.as_bytes();
				let out_ptr = host_alloc(msg_bytes.len());
				if !out_ptr.is_null() {
					ptr::copy_nonoverlapping(msg_bytes.as_ptr(), out_ptr, msg_bytes.len());
					*result_out = BufferFFI {
						ptr: out_ptr,
						len: msg_bytes.len(),
						cap: msg_bytes.len(),
					};
				}
				return FFI_ERROR_INTERNAL;
			}

			let result_bytes = match encode_frames(&exec_result.frames, &EncodeOptions::fast()) {
				Ok(b) => b,
				Err(e) => {
					error!("host_rql: failed to serialize result: {}", e);
					return FFI_ERROR_INTERNAL;
				}
			};

			let out_ptr = host_alloc(result_bytes.len());
			if out_ptr.is_null() && !result_bytes.is_empty() {
				return FFI_ERROR_INTERNAL;
			}
			if !result_bytes.is_empty() {
				ptr::copy_nonoverlapping(result_bytes.as_ptr(), out_ptr, result_bytes.len());
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
