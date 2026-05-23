// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::{context::context::ContextFFI, flow::change::ChangeFFI};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct OperatorVTableFFI {
	pub apply: unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI, input: *const ChangeFFI) -> i32,

	pub tick: unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI, timestamp_nanos: u64) -> i32,

	pub tick_interval: unsafe extern "C" fn(instance: *mut c_void) -> u64,

	pub destroy: unsafe extern "C" fn(instance: *mut c_void),

	pub flush_state: unsafe extern "C" fn(instance: *mut c_void, ctx: *mut ContextFFI) -> i32,
}
