// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::{
	data::{buffer::ExternCBuffer, column::ExternCColumns},
	operator::column::ExternCOperatorColumns,
};

pub type ExternCSourceMagicFn = extern "C" fn() -> u32;

pub type ExternCSourceCreateFn = extern "C" fn(config: *const u8, config_len: usize) -> *mut c_void;

#[repr(C)]
pub struct ExternCSourceDescriptor {
	pub api: u32,

	pub name: ExternCBuffer,

	pub version: ExternCBuffer,

	pub description: ExternCBuffer,

	pub mode: u8,

	pub output_columns: ExternCOperatorColumns,

	pub vtable: ExternCSourceVTable,
}

// SAFETY: every pointer in the descriptor addresses immutable module-static data (strings, symbols).
unsafe impl Send for ExternCSourceDescriptor {}
unsafe impl Sync for ExternCSourceDescriptor {}

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
