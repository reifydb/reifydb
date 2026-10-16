// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::from_raw_parts;

use reifydb_sdk::{
	common::extern_c::wire::{
		callbacks::builder::ColumnBufferHandle,
		columns::ExternCColumnData,
		status::{EXTERN_C_ERROR_NULL_PTR, EXTERN_C_OK},
	},
	flow::operator::extern_c::wire::context::ExternCContextRaw,
};

use crate::callbacks::extern_c::builder::commit_passed;

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_pass_through(
	ctx: *mut ExternCContextRaw,
	input: *const ExternCColumnData,
	mask: *const u8,
	mask_len: usize,
	handle_out: *mut *mut ColumnBufferHandle,
) -> i32 {
	if ctx.is_null() || input.is_null() || handle_out.is_null() || (mask_len > 0 && mask.is_null()) {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	// SAFETY: every pointer is null-checked above; the guest passes back an input column the host marshalled
	// for this call (its buffers stay live until the call returns), `mask` valid for `mask_len` reads and
	// `handle_out` valid for one write.
	unsafe {
		let mask = if mask_len == 0 {
			&[]
		} else {
			from_raw_parts(mask, mask_len)
		};
		match commit_passed(&*input, mask) {
			Ok(handle) => {
				*handle_out = handle;
				EXTERN_C_OK
			}
			Err(code) => code,
		}
	}
}
