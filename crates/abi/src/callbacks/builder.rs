// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::{context::context::ContextFFI, data::column::ColumnTypeCode};

pub type ColumnBufferHandle = c_void;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmitDiffKind {
	Insert = 0,
	Update = 1,
	Remove = 2,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct BuilderCallbacks {
	pub acquire: unsafe extern "C" fn(
		ctx: *mut ContextFFI,
		type_code: ColumnTypeCode,
		capacity: usize,
	) -> *mut ColumnBufferHandle,

	pub data_ptr: unsafe extern "C" fn(handle: *mut ColumnBufferHandle) -> *mut u8,

	pub offsets_ptr: unsafe extern "C" fn(handle: *mut ColumnBufferHandle) -> *mut u64,

	pub bitvec_ptr: unsafe extern "C" fn(handle: *mut ColumnBufferHandle) -> *mut u8,

	pub grow: unsafe extern "C" fn(handle: *mut ColumnBufferHandle, additional: usize) -> i32,

	pub commit: unsafe extern "C" fn(handle: *mut ColumnBufferHandle, written_count: usize) -> i32,

	pub release: unsafe extern "C" fn(handle: *mut ColumnBufferHandle),

	pub emit_diff: unsafe extern "C" fn(
		ctx: *mut ContextFFI,
		kind: EmitDiffKind,
		pre_handles_ptr: *const *mut ColumnBufferHandle,
		pre_name_ptrs: *const *const u8,
		pre_name_lens: *const usize,
		pre_count: usize,
		pre_row_count: usize,
		pre_row_numbers_ptr: *const u64,
		pre_row_numbers_len: usize,
		post_handles_ptr: *const *mut ColumnBufferHandle,
		post_name_ptrs: *const *const u8,
		post_name_lens: *const usize,
		post_count: usize,
		post_row_count: usize,
		post_row_numbers_ptr: *const u64,
		post_row_numbers_len: usize,
	) -> i32,
}
