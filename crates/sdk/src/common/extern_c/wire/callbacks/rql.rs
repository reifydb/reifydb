// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::common::extern_c::wire::buffer::ExternCBuffer;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RqlCallbacks {
	pub rql: unsafe extern "C" fn(
		ctx: *mut c_void,
		rql_ptr: *const u8,
		rql_len: usize,
		params_ptr: *const u8,
		params_len: usize,
		result_out: *mut ExternCBuffer,
	) -> i32,
}
