// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use crate::{
	data::{buffer::BufferFFI, column::ColumnsFFI},
	operator::column::OperatorColumnsFFI,
};

pub type SinkMagicFnFFI = extern "C" fn() -> u32;

pub type SinkCreateFnFFI = extern "C" fn(config: *const u8, config_len: usize) -> *mut c_void;

#[repr(C)]
pub struct SinkDescriptorFFI {
	pub api: u32,

	pub name: BufferFFI,

	pub version: BufferFFI,

	pub description: BufferFFI,

	pub input_columns: OperatorColumnsFFI,

	pub vtable: SinkVTableFFI,
}

unsafe impl Send for SinkDescriptorFFI {}
unsafe impl Sync for SinkDescriptorFFI {}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SinkVTableFFI {
	pub write: extern "C" fn(instance: *mut c_void, records: *const SinkRecordFFI, count: usize) -> i32,

	pub destroy: extern "C" fn(instance: *mut c_void),
}

#[repr(C)]
pub struct SinkRecordFFI {
	pub op: u8,

	pub columns: ColumnsFFI,
}
