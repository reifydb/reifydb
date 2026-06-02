// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::{
	context::{context::ContextFFI, iterators::StateIteratorFFI},
	data::{buffer::BufferFFI, key_ref::KeyRefFFI},
};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct StateCallbacks {
	pub get: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		key: *const u8,
		key_len: usize,
		output: *mut BufferFFI,
	) -> i32,

	pub set: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		key: *const u8,
		key_len: usize,
		value: *const u8,
		value_len: usize,
	) -> i32,

	pub remove: extern "C" fn(operator_id: u64, ctx: *mut ContextFFI, key: *const u8, key_len: usize) -> i32,

	pub clear: extern "C" fn(operator_id: u64, ctx: *mut ContextFFI) -> i32,

	pub prefix: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		prefix: *const u8,
		prefix_len: usize,
		iterator_out: *mut *mut StateIteratorFFI,
	) -> i32,

	pub range: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		start: *const u8,
		start_len: usize,
		start_bound_type: u8,
		end: *const u8,
		end_len: usize,
		end_bound_type: u8,
		iterator_out: *mut *mut StateIteratorFFI,
	) -> i32,

	pub iterator_next: extern "C" fn(
		iterator: *mut StateIteratorFFI,
		key_out: *mut BufferFFI,
		value_out: *mut BufferFFI,
	) -> i32,

	pub iterator_free: extern "C" fn(iterator: *mut StateIteratorFFI),

	pub internal_get: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		key: *const u8,
		key_len: usize,
		output: *mut BufferFFI,
	) -> i32,

	pub internal_set: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		key: *const u8,
		key_len: usize,
		value: *const u8,
		value_len: usize,
	) -> i32,

	pub internal_remove:
		extern "C" fn(operator_id: u64, ctx: *mut ContextFFI, key: *const u8, key_len: usize) -> i32,

	pub get_many: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		keys: *const KeyRefFFI,
		keys_len: usize,
		iterator_out: *mut *mut StateIteratorFFI,
	) -> i32,

	pub internal_get_many: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		keys: *const KeyRefFFI,
		keys_len: usize,
		iterator_out: *mut *mut StateIteratorFFI,
	) -> i32,

	pub allocate_row_numbers:
		extern "C" fn(operator_id: u64, ctx: *mut ContextFFI, count: u64, out_start: *mut u64) -> i32,
}
