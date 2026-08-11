// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::common::extern_c::wire::{buffer::ExternCBuffer, columns::ExternCColumns};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExternCSourceVTable {
	pub poll: extern "C" fn(
		instance: *mut c_void,
		checkpoint: *const u8,
		checkpoint_len: usize,
		output: *mut ExternCColumns,
		out_checkpoint: *mut ExternCBuffer,
	) -> i32,

	pub run: extern "C" fn(
		instance: *mut c_void,
		checkpoint: *const u8,
		checkpoint_len: usize,
		emit_ctx: *mut c_void,
		emit_fn: extern "C" fn(
			ctx: *mut c_void,
			columns: *const ExternCColumns,
			checkpoint: *const ExternCBuffer,
		) -> i32,
	) -> i32,

	pub destroy: extern "C" fn(instance: *mut c_void),
}
