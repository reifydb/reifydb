// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::{context::context::ContextFFI, flow::change::ChangeFFI};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct OperatorVTableFFI {
	pub apply: unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI, input: *const ChangeFFI) -> i32,

	pub pull: unsafe extern "C" fn(
		instance: *mut c_void,
		ctx: *mut ContextFFI,
		row_numbers: *const u64,
		count: usize,
	) -> i32,

	pub tick: unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI, timestamp_nanos: u64) -> i32,

	pub destroy: unsafe extern "C" fn(instance: *mut c_void),

	pub flush_state: unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI) -> i32,
}
