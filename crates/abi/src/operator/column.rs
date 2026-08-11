// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ptr::null;

use crate::data::buffer::ExternCBuffer;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ExternCOperatorColumn {
	pub name: ExternCBuffer,

	pub base_type: u8,

	pub constraint_type: u8,

	pub constraint_param1: u32,

	pub constraint_param2: u32,

	pub description: ExternCBuffer,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ExternCOperatorColumns {
	pub columns: *const ExternCOperatorColumn,

	pub column_count: usize,
}

impl ExternCOperatorColumns {
	pub const fn empty() -> Self {
		Self {
			columns: null(),
			column_count: 0,
		}
	}
}
