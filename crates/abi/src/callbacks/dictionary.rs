// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{context::context::ContextFFI, data::buffer::BufferFFI};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct DictionaryCallbacks {
	pub id_by_name: extern "C" fn(
		ctx: *mut ContextFFI,
		name_ptr: *const u8,
		name_len: usize,
		out_id: *mut u64,
		found: *mut u8,
	) -> i32,

	pub find: extern "C" fn(
		ctx: *mut ContextFFI,
		dictionary_id: u64,
		value_ptr: *const u8,
		value_len: usize,
		out_id: *mut u128,
		out_id_type: *mut u8,
		found: *mut u8,
	) -> i32,

	pub get: extern "C" fn(ctx: *mut ContextFFI, dictionary_id: u64, id: u128, output: *mut BufferFFI) -> i32,
}
