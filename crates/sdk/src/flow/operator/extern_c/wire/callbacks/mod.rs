// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod state;

use self::state::StateCallbacks;
use crate::{
	common::extern_c::wire::{
		buffer::ExternCBuffer,
		callbacks::{
			builder::{BuilderCallbacks, ColumnBufferHandle},
			memory::MemoryCallbacks,
		},
		columns::ExternCColumnData,
	},
	flow::operator::extern_c::wire::context::ExternCContextRaw,
};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct OperatorCallbacks {
	pub memory: MemoryCallbacks,

	pub state: StateCallbacks,

	pub builder: BuilderCallbacks,

	pub dictionary_get_many: extern "C" fn(
		ctx: *mut ExternCContextRaw,
		dictionary: u64,
		ids: *const u128,
		n: usize,
		values_out: *mut ExternCBuffer,
		present_out: *mut u8,
	) -> i32,

	pub pass_through: extern "C" fn(
		ctx: *mut ExternCContextRaw,
		input: *const ExternCColumnData,
		mask: *const u8,
		mask_len: usize,
		handle_out: *mut *mut ColumnBufferHandle,
	) -> i32,

	pub dictionary_find_many: extern "C" fn(
		ctx: *mut ExternCContextRaw,
		dictionary: u64,
		values: *const ExternCBuffer,
		n: usize,
		ids_out: *mut u128,
		present_out: *mut u8,
		id_width_out: *mut u8,
	) -> i32,

	pub dictionary_intern_many: extern "C" fn(
		ctx: *mut ExternCContextRaw,
		dictionary: u64,
		values: *const ExternCBuffer,
		n: usize,
		ids_out: *mut u128,
		id_width_out: *mut u8,
	) -> i32,

	pub builder_bind_dictionary: unsafe extern "C" fn(handle: *mut ColumnBufferHandle, dictionary: u64) -> i32,
}
