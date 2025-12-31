// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use super::BufferFFI;

/// Type code for column data variant (maps to ColumnData enum)
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
	Undefined = 26,
}

/// FFI-safe column data representation
///
/// Contains typed column data in a format suitable for FFI transfer.
/// - For fixed-size types: `data` contains the raw values
/// - For variable-length types (Utf8, Blob): `data` contains concatenated bytes, `offsets` contains u64 offsets (length
///   = row_count + 1)
/// - `defined_bitvec` tracks which values are defined (bit=1 means defined)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ColumnDataFFI {
	/// Type code indicating data format
	pub type_code: ColumnTypeCode,
	/// Number of rows in the column
	pub row_count: usize,
	/// Raw data buffer (interpretation depends on type_code)
	pub data: BufferFFI,
	/// Defined/null bitvec (1 = defined, 0 = undefined)
	pub defined_bitvec: BufferFFI,
	/// Offsets for variable-length types (Utf8, Blob). Empty for fixed-size types.
	pub offsets: BufferFFI,
}

impl ColumnDataFFI {
	/// Create an empty column data
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

/// FFI-safe single column representation (name + data)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ColumnFFI {
	/// Column name (UTF-8 encoded)
	pub name: BufferFFI,
	/// Column data
	pub data: ColumnDataFFI,
}

impl ColumnFFI {
	/// Create an empty column
	pub const fn empty() -> Self {
		Self {
			name: BufferFFI::empty(),
			data: ColumnDataFFI::empty(),
		}
	}
}

/// FFI-safe multi-row columnar structure
///
/// Represents a batch of rows in columnar format, matching the Rust `Columns` type.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ColumnsFFI {
	/// Number of rows
	pub row_count: usize,
	/// Number of columns
	pub column_count: usize,
	/// Pointer to row numbers array (u64 per row, may be null if empty)
	pub row_numbers: *const u64,
	/// Pointer to array of ColumnFFI
	pub columns: *const ColumnFFI,
}

impl ColumnsFFI {
	/// Create an empty Columns
	pub const fn empty() -> Self {
		Self {
			row_count: 0,
			column_count: 0,
			row_numbers: core::ptr::null(),
			columns: core::ptr::null(),
		}
	}

	/// Check if the columns are empty
	pub fn is_empty(&self) -> bool {
		self.row_count == 0
	}
}
