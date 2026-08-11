// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{context::context::ExternCContext, data::buffer::ExternCBuffer};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RqlCallbacks {
	pub rql: unsafe extern "C" fn(
		ctx: *mut ExternCContext,
		rql_ptr: *const u8,
		rql_len: usize,
		params_ptr: *const u8,
		params_len: usize,
		result_out: *mut ExternCBuffer,
	) -> i32,
}
