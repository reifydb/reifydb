// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	context::{context::ContextFFI, iterators::StateIteratorFFI},
	data::{buffer::BufferFFI, key_ref::KeyRefFFI, state::StateEntryFFI},
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
		out: *mut StateEntryFFI,
		cap: usize,
		out_len: *mut usize,
	) -> i32,

	pub iterator_free: extern "C" fn(iterator: *mut StateIteratorFFI),

	pub get_many: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		keys: *const KeyRefFFI,
		keys_len: usize,
		iterator_out: *mut *mut StateIteratorFFI,
	) -> i32,

	pub get_or_create_row_numbers: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		group: u64,
		keys: *const KeyRefFFI,
		keys_len: usize,
		row_numbers_out: *mut u64,
		is_new_out: *mut u8,
	) -> i32,

	pub remove_row_number: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		group: u64,
		key: *const u8,
		key_len: usize,
	) -> i32,

	pub remove_row_numbers_below: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		group: u64,
		upper: *const u8,
		upper_len: usize,
		output: *mut BufferFFI,
	) -> i32,

	pub intern_groups: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		groups: *const KeyRefFFI,
		groups_len: usize,
		ids_out: *mut u64,
	) -> i32,

	pub lookup_groups: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		groups: *const KeyRefFFI,
		groups_len: usize,
		ids_out: *mut u64,
	) -> i32,

	pub arm_timer: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		at_millis: u64,
		kind: u8,
		key: *const u8,
		key_len: usize,
	) -> i32,

	pub disarm_timer: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		at_millis: u64,
		kind: u8,
		key: *const u8,
		key_len: usize,
	) -> i32,

	pub flow_watermark: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		millis_out: *mut u64,
		present_out: *mut u8,
	) -> i32,
}
