// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::data::buffer::BufferFFI;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RowShapeFieldFFI {
	pub name: BufferFFI,

	pub base_type: u8,

	pub constraint_type: u8,

	pub constraint_param1: u32,

	pub constraint_param2: u32,

	pub offset: u32,

	pub size: u32,

	pub align: u8,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RowShapeFFI {
	pub fingerprint: u64,

	pub fields: *const RowShapeFieldFFI,

	pub field_count: usize,
}
