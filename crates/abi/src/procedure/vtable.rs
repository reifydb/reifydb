// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::context::context::ContextFFI;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ProcedureVTableFFI {
	pub call: unsafe extern "C" fn(
		instance: *mut c_void,
		ctx: *mut ContextFFI,
		params_ptr: *const u8,
		params_len: usize,
	) -> i32,

	pub destroy: unsafe extern "C" fn(instance: *mut c_void),
}
