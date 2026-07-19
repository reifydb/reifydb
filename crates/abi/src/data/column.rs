// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::buffer::BufferFFI;

macro_rules! column_type_codes {
	($($name:ident = $value:literal),* $(,)?) => {
		#[repr(C)]
		#[derive(Debug, Clone, Copy, PartialEq, Eq)]
		pub enum ColumnTypeCode {
			$($name = $value),*
		}

		impl ColumnTypeCode {
			pub const ALL: &'static [ColumnTypeCode] = &[$(ColumnTypeCode::$name),*];

			pub fn from_u8(value: u8) -> Option<Self> {
				match value {
					$($value => Some(ColumnTypeCode::$name),)*
					_ => None,
				}
			}
		}
	};
}

column_type_codes! {
	Undefined = 0,
	Bool = 1,
	Float4 = 2,
	Float8 = 3,
	Int1 = 4,
	Int2 = 5,
	Int4 = 6,
	Int8 = 7,
	Int16 = 8,
	Utf8 = 9,
	Uint1 = 10,
	Uint2 = 11,
	Uint4 = 12,
	Uint8 = 13,
	Uint16 = 14,
	Date = 15,
	DateTime = 16,
	Time = 17,
	Duration = 18,
	IdentityId = 19,
	Uuid4 = 20,
	Uuid7 = 21,
	Blob = 22,
	Int = 23,
	Uint = 24,
	Decimal = 25,
	Any = 26,
	DictionaryId = 27,
	Vector = 32,
}

impl ColumnTypeCode {
	pub fn byte(self) -> u8 {
		self as u8
	}

	pub fn elem_size(self) -> usize {
		match self {
			ColumnTypeCode::Int1 | ColumnTypeCode::Uint1 | ColumnTypeCode::Bool => 1,
			ColumnTypeCode::Int2 | ColumnTypeCode::Uint2 => 2,
			ColumnTypeCode::Float4
			| ColumnTypeCode::Int4
			| ColumnTypeCode::Uint4
			| ColumnTypeCode::Date => 4,
			ColumnTypeCode::Float8
			| ColumnTypeCode::Int8
			| ColumnTypeCode::Uint8
			| ColumnTypeCode::DateTime
			| ColumnTypeCode::Time => 8,
			ColumnTypeCode::Int16
			| ColumnTypeCode::Uint16
			| ColumnTypeCode::Duration
			| ColumnTypeCode::IdentityId
			| ColumnTypeCode::Uuid4
			| ColumnTypeCode::Uuid7
			| ColumnTypeCode::DictionaryId => 16,
			ColumnTypeCode::Utf8
			| ColumnTypeCode::Blob
			| ColumnTypeCode::Int
			| ColumnTypeCode::Uint
			| ColumnTypeCode::Decimal
			| ColumnTypeCode::Any
			| ColumnTypeCode::Vector
			| ColumnTypeCode::Undefined => 1,
		}
	}

	pub fn is_var_len(self) -> bool {
		matches!(
			self,
			ColumnTypeCode::Utf8
				| ColumnTypeCode::Blob | ColumnTypeCode::Int
				| ColumnTypeCode::Uint | ColumnTypeCode::Decimal
				| ColumnTypeCode::Any | ColumnTypeCode::DictionaryId
		)
	}
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
