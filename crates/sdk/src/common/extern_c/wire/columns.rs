// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::tag::ValueKind;

use super::buffer::ExternCBuffer;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ExternCColumnData {
	pub type_code: ValueKind,

	pub row_count: usize,

	pub data: ExternCBuffer,

	pub defined_bitvec: ExternCBuffer,

	pub offsets: ExternCBuffer,
}

impl ExternCColumnData {
	pub const fn empty() -> Self {
		Self {
			type_code: ValueKind::None,
			row_count: 0,
			data: ExternCBuffer::empty(),
			defined_bitvec: ExternCBuffer::empty(),
			offsets: ExternCBuffer::empty(),
		}
	}
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ExternCColumn {
	pub name: ExternCBuffer,

	pub data: ExternCColumnData,
}

impl ExternCColumn {
	pub const fn empty() -> Self {
		Self {
			name: ExternCBuffer::empty(),
			data: ExternCColumnData::empty(),
		}
	}
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ExternCColumns {
	pub row_count: usize,

	pub column_count: usize,

	pub row_numbers: *const u64,

	pub columns: *const ExternCColumn,

	pub time: *const u64,
}

impl ExternCColumns {
	pub const fn empty() -> Self {
		Self {
			row_count: 0,
			column_count: 0,
			row_numbers: core::ptr::null(),
			columns: core::ptr::null(),
			time: core::ptr::null(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.row_count == 0
	}
}
