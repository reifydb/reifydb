// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ptr::null;

use crate::data::buffer::BufferFFI;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OperatorColumnFFI {
	pub name: BufferFFI,

	pub base_type: u8,

	pub constraint_type: u8,

	pub constraint_param1: u32,

	pub constraint_param2: u32,

	pub description: BufferFFI,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OperatorColumnsFFI {
	pub columns: *const OperatorColumnFFI,

	pub column_count: usize,
}

impl OperatorColumnsFFI {
	pub const fn empty() -> Self {
		Self {
			columns: null(),
			column_count: 0,
		}
	}
}
