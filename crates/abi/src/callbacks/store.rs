// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	context::{context::ContextFFI, iterators::StoreIteratorFFI},
	data::buffer::BufferFFI,
};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct StoreCallbacks {
	pub get: extern "C" fn(ctx: *mut ContextFFI, key: *const u8, key_len: usize, output: *mut BufferFFI) -> i32,

	pub contains_key: extern "C" fn(ctx: *mut ContextFFI, key: *const u8, key_len: usize, result: *mut u8) -> i32,

	pub prefix: extern "C" fn(
		ctx: *mut ContextFFI,
		prefix: *const u8,
		prefix_len: usize,
		iterator_out: *mut *mut StoreIteratorFFI,
	) -> i32,

	pub range: extern "C" fn(
		ctx: *mut ContextFFI,
		start: *const u8,
		start_len: usize,
		start_bound_type: u8,
		end: *const u8,
		end_len: usize,
		end_bound_type: u8,
		iterator_out: *mut *mut StoreIteratorFFI,
	) -> i32,

	pub iterator_next: extern "C" fn(
		iterator: *mut StoreIteratorFFI,
		key_out: *mut BufferFFI,
		value_out: *mut BufferFFI,
	) -> i32,

	pub iterator_free: extern "C" fn(iterator: *mut StoreIteratorFFI),
}
