// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::{context::context::ContextFFI, data::state::StateUsageFFI, flow::change::ChangeFFI};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct OperatorVTableFFI {
	pub apply: unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI, input: *const ChangeFFI) -> i32,

	pub on_timer: unsafe extern "C" fn(
		instance: *mut c_void,
		ctx: *mut ContextFFI,
		at_millis: u64,
		kind: u8,
		key: *const u8,
		key_len: usize,
	) -> i32,

	pub destroy: unsafe extern "C" fn(instance: *mut c_void),

	pub flush_state:
		unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI, usage: *mut StateUsageFFI) -> i32,

	pub sample: unsafe extern "C" fn(instance: *mut c_void, out: *mut StateUsageFFI) -> i32,

	pub invalidate_groups: unsafe extern "C" fn(instance: *mut c_void, groups: *const u64, len: usize) -> i32,

	pub seal_after_ms: unsafe extern "C" fn(instance: *mut c_void) -> u64,
}
