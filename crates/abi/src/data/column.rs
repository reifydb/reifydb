// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::buffer::ExternCBuffer;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnTypeCode {
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
}

impl ColumnTypeCode {
	pub const ALL: [ColumnTypeCode; 28] = [
		ColumnTypeCode::Undefined,
		ColumnTypeCode::Bool,
		ColumnTypeCode::Float4,
		ColumnTypeCode::Float8,
		ColumnTypeCode::Int1,
		ColumnTypeCode::Int2,
		ColumnTypeCode::Int4,
		ColumnTypeCode::Int8,
		ColumnTypeCode::Int16,
		ColumnTypeCode::Utf8,
		ColumnTypeCode::Uint1,
		ColumnTypeCode::Uint2,
		ColumnTypeCode::Uint4,
		ColumnTypeCode::Uint8,
		ColumnTypeCode::Uint16,
		ColumnTypeCode::Date,
		ColumnTypeCode::DateTime,
		ColumnTypeCode::Time,
		ColumnTypeCode::Duration,
		ColumnTypeCode::IdentityId,
		ColumnTypeCode::Uuid4,
		ColumnTypeCode::Uuid7,
		ColumnTypeCode::Blob,
		ColumnTypeCode::Int,
		ColumnTypeCode::Uint,
		ColumnTypeCode::Decimal,
		ColumnTypeCode::Any,
		ColumnTypeCode::DictionaryId,
	];

	pub fn from_u32(value: u32) -> Option<Self> {
		if value < Self::ALL.len() as u32 {
			Some(Self::ALL[value as usize])
		} else {
			None
		}
	}
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ExternCColumnData {
	pub type_code: ColumnTypeCode,

	pub row_count: usize,

	pub data: ExternCBuffer,

	pub defined_bitvec: ExternCBuffer,

	pub offsets: ExternCBuffer,
}

impl ExternCColumnData {
	pub const fn empty() -> Self {
		Self {
			type_code: ColumnTypeCode::Undefined,
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

#[cfg(test)]
mod tests {
	use super::ColumnTypeCode;

	#[test]
	fn the_all_array_is_ordered_by_discriminant() {
		// from_u32 decodes by indexing ALL, so array order is the wire contract; nothing in the
		// language ties it to the `= N` discriminants.
		for (index, code) in ColumnTypeCode::ALL.iter().enumerate() {
			assert_eq!(
				*code as u32, index as u32,
				"ColumnTypeCode::{code:?} sits at ALL[{index}] but carries discriminant {}",
				*code as u32
			);
		}
	}

	#[test]
	fn every_variant_survives_an_encode_decode_round_trip() {
		// The marshalling boundary writes `code as u32` and reads it back through from_u32, so the
		// two must be inverse for every variant.
		for code in ColumnTypeCode::ALL {
			assert_eq!(ColumnTypeCode::from_u32(code as u32), Some(code));
		}
	}

	#[test]
	fn a_code_outside_the_table_does_not_decode() {
		// Returning Some would let a truncated or future-versioned payload decode as whatever
		// variant happens to sit at that index.
		assert_eq!(ColumnTypeCode::from_u32(ColumnTypeCode::ALL.len() as u32), None);
		assert_eq!(ColumnTypeCode::from_u32(u32::MAX), None);
	}
}
