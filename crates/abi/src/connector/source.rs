// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::{
	data::{buffer::BufferFFI, column::ColumnsFFI},
	operator::column::OperatorColumnsFFI,
};

pub type SourceMagicFnFFI = extern "C" fn() -> u32;

pub type SourceCreateFnFFI = extern "C" fn(config: *const u8, config_len: usize) -> *mut c_void;

#[repr(C)]
pub struct SourceDescriptorFFI {
	pub api: u32,

	pub name: BufferFFI,

	pub version: BufferFFI,

	pub description: BufferFFI,

	pub mode: u8,

	pub output_columns: OperatorColumnsFFI,

	pub vtable: SourceVTableFFI,
}

unsafe impl Send for SourceDescriptorFFI {}
unsafe impl Sync for SourceDescriptorFFI {}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SourceVTableFFI {
	pub poll: extern "C" fn(
		instance: *mut c_void,
		checkpoint: *const u8,
		checkpoint_len: usize,
		output: *mut ColumnsFFI,
		out_checkpoint: *mut BufferFFI,
	) -> i32,

	pub run: extern "C" fn(
		instance: *mut c_void,
		checkpoint: *const u8,
		checkpoint_len: usize,
		emit_ctx: *mut c_void,
		emit_fn: extern "C" fn(
			ctx: *mut c_void,
			columns: *const ColumnsFFI,
			checkpoint: *const BufferFFI,
		) -> i32,
	) -> i32,

	pub destroy: extern "C" fn(instance: *mut c_void),
}
