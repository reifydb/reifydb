// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	context::{context::ExternCContext, iterators::ExternCStoreIterator},
	data::buffer::ExternCBuffer,
};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct StoreCallbacks {
	pub get: extern "C" fn(ctx: *mut ExternCContext, key: *const u8, key_len: usize, output: *mut ExternCBuffer) -> i32,

	pub contains_key: extern "C" fn(ctx: *mut ExternCContext, key: *const u8, key_len: usize, result: *mut u8) -> i32,

	pub prefix: extern "C" fn(
		ctx: *mut ExternCContext,
		prefix: *const u8,
		prefix_len: usize,
		iterator_out: *mut *mut ExternCStoreIterator,
	) -> i32,

	pub range: extern "C" fn(
		ctx: *mut ExternCContext,
		start: *const u8,
		start_len: usize,
		start_bound_type: u8,
		end: *const u8,
		end_len: usize,
		end_bound_type: u8,
		iterator_out: *mut *mut ExternCStoreIterator,
	) -> i32,

	pub iterator_next: extern "C" fn(
		iterator: *mut ExternCStoreIterator,
		key_out: *mut ExternCBuffer,
		value_out: *mut ExternCBuffer,
	) -> i32,

	pub iterator_free: extern "C" fn(iterator: *mut ExternCStoreIterator),
}
