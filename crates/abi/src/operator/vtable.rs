// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::{context::context::ExternCContext, data::state::ExternCStateUsage, flow::change::ExternCChange};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExternCOperatorVTable {
	pub apply: unsafe extern "C" fn(
		instance: *mut c_void,
		ctx: *mut ExternCContext,
		input: *const ExternCChange,
	) -> i32,

	pub on_timer: unsafe extern "C" fn(
		instance: *mut c_void,
		ctx: *mut ExternCContext,
		at_millis: u64,
		kind: u8,
		key: *const u8,
		key_len: usize,
	) -> i32,

	pub destroy: unsafe extern "C" fn(instance: *mut c_void),

	pub flush_state: unsafe extern "C" fn(
		instance: *mut c_void,
		ctx: *mut ExternCContext,
		usage: *mut ExternCStateUsage,
	) -> i32,

	pub sample: unsafe extern "C" fn(instance: *mut c_void, out: *mut ExternCStateUsage) -> i32,

	pub seal_after_ms: unsafe extern "C" fn(instance: *mut c_void) -> u64,
}
