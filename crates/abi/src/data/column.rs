// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::buffer::BufferFFI;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnTypeCode {
	Bool = 0,
	Float4 = 1,
	Float8 = 2,
	Int1 = 3,
	Int2 = 4,
	Int4 = 5,
	Int8 = 6,
	Int16 = 7,
	Uint1 = 8,
	Uint2 = 9,
	Uint4 = 10,
	Uint8 = 11,
	Uint16 = 12,
	Utf8 = 13,
	Date = 14,
	DateTime = 15,
	Time = 16,
	Duration = 17,
	IdentityId = 18,
	Uuid4 = 19,
	Uuid7 = 20,
	Blob = 21,
	Int = 22,
	Uint = 23,
	Decimal = 24,
	Any = 25,
	DictionaryId = 26,
	Undefined = 27,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ColumnDataFFI {
	pub type_code: ColumnTypeCode,

	pub row_count: usize,

	pub data: BufferFFI,

	pub defined_bitvec: BufferFFI,

	pub offsets: BufferFFI,
}

impl ColumnDataFFI {
	pub const fn empty() -> Self {
		Self {
			type_code: ColumnTypeCode::Undefined,
			row_count: 0,
			data: BufferFFI::empty(),
			defined_bitvec: BufferFFI::empty(),
			offsets: BufferFFI::empty(),
		}
	}
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ColumnFFI {
	pub name: BufferFFI,

	pub data: ColumnDataFFI,
}

impl ColumnFFI {
	pub const fn empty() -> Self {
		Self {
			name: BufferFFI::empty(),
			data: ColumnDataFFI::empty(),
		}
	}
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ColumnsFFI {
	pub row_count: usize,

	pub column_count: usize,

	pub row_numbers: *const u64,

	pub columns: *const ColumnFFI,

	pub created_at: *const u64,

	pub updated_at: *const u64,
}

impl ColumnsFFI {
	pub const fn empty() -> Self {
		Self {
			row_count: 0,
			column_count: 0,
			row_numbers: core::ptr::null(),
			columns: core::ptr::null(),
			created_at: core::ptr::null(),
			updated_at: core::ptr::null(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.row_count == 0
	}
}
