// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

use crate::{
	data::{buffer::ExternCBuffer, column::ExternCColumns},
	operator::column::ExternCOperatorColumns,
};

pub type ExternCSinkMagicFn = extern "C" fn() -> u32;

pub type ExternCSinkCreateFn = extern "C" fn(config: *const u8, config_len: usize) -> *mut c_void;

#[repr(C)]
pub struct ExternCSinkDescriptor {
	pub api: u32,

	pub name: ExternCBuffer,

	pub version: ExternCBuffer,

	pub description: ExternCBuffer,

	pub input_columns: ExternCOperatorColumns,

	pub vtable: ExternCSinkVTable,
}

// SAFETY: every pointer in the descriptor addresses immutable module-static data (strings, symbols).
unsafe impl Send for ExternCSinkDescriptor {}
unsafe impl Sync for ExternCSinkDescriptor {}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExternCSinkVTable {
	pub write: extern "C" fn(instance: *mut c_void, records: *const ExternCSinkRecord, count: usize) -> i32,

	pub destroy: extern "C" fn(instance: *mut c_void),
}

#[repr(C)]
pub struct ExternCSinkRecord {
	pub op: u8,

	pub columns: ExternCColumns,
}
