// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{context::context::ContextFFI, data::buffer::BufferFFI};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RqlCallbacks {
	pub rql: unsafe extern "C" fn(
		ctx: *mut ContextFFI,
		rql_ptr: *const u8,
		rql_len: usize,
		params_ptr: *const u8,
		params_len: usize,
		result_out: *mut BufferFFI,
	) -> i32,
}
