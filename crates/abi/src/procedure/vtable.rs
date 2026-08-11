// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::context::context::ExternCContext;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExternCProcedureVTable {
	pub call: unsafe extern "C" fn(
		instance: *mut c_void,
		ctx: *mut ExternCContext,
		params_ptr: *const u8,
		params_len: usize,
	) -> i32,

	pub destroy: unsafe extern "C" fn(instance: *mut c_void),
}
