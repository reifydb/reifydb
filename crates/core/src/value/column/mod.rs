// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt;

use reifydb_type::{
	fragment::Fragment,
	util::bitvec::BitVec,
	value::{
		dictionary::DictionaryEntryId,
		r#type::Type,
		uuid::{Uuid4, Uuid7},
	},
};

use crate::value::column::{array::Column, buffer::ColumnBuffer};

pub mod array;
pub mod buffer;
pub mod columns;
pub mod compressed;
pub mod encoding;
pub mod frame;
pub mod headers;
pub mod mask;
pub mod nones;
pub mod push;
pub mod row;
pub mod stats;
pub mod transform;
pub mod view;

pub struct ColumnWithName {
	pub name: Fragment,
	pub data: ColumnBuffer,
}

impl Clone for ColumnWithName {
	fn clone(&self) -> Self {
		Self {
			name: self.name.clone(),
			data: self.data.clone(),
		}
	}
}

impl PartialEq for ColumnWithName {
	fn eq(&self, other: &Self) -> bool {
		self.name == other.name && self.data == other.data
	}
}

impl fmt::Debug for ColumnWithName {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ColumnWithName").field("name", &self.name).field("data", &self.data).finish()
	}
}

impl ColumnWithName {
	pub fn new(name: impl Into<Fragment>, data: ColumnBuffer) -> Self {
		Self {
			name: name.into(),
			data,
		}
	}

	// Build a named column from a polymorphic `Column` trait-object handle.
	// Materializes via `to_canonical` + `to_buffer` (Arc-bumps the inner
	// buffer when the `Column` is already canonical).
	pub fn from_column(name: impl Into<Fragment>, column: Column) -> Self {
		let buffer = column
			.to_canonical()
			.map(|c| c.to_buffer())
			.unwrap_or_else(|_| panic!("ColumnWithName::from_column: to_canonical failed"));
		Self {
			name: name.into(),
			data: buffer,
		}
	}

	pub fn get_type(&self) -> Type {
		self.data.get_type()
	}

	pub fn with_new_data(&self, data: ColumnBuffer) -> ColumnWithName {
		ColumnWithName {
			name: self.name.clone(),
			data,
		}
	}

	pub fn name(&self) -> &Fragment {
		&self.name
	}

	pub fn name_owned(&self) -> Fragment {
		self.name.clone()
	}

	pub fn data(&self) -> &ColumnBuffer {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut ColumnBuffer {
		&mut self.data
	}

	// Return a polymorphic `Column` handle (trait-object wrapper around the
	// canonical form of `self.data`). Use this when you need encoding-agnostic
	// read operators (`filter`, `take`, `slice`, ...) or downcasting to
	// specialized impls - compressed encodings live behind this boundary.
	pub fn column(&self) -> Column {
		Column::from_column_buffer(self.data.clone())
	}

	pub fn to_static(&self) -> ColumnWithName {
		self.clone()
	}
}

impl ColumnWithName {
	pub fn int1(name: impl Into<Fragment>, data: impl IntoIterator<Item = i8>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::int1(data),
		}
	}

	pub fn int1_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = i8>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::int1_with_bitvec(data, bitvec),
		}
	}

	pub fn int2(name: impl Into<Fragment>, data: impl IntoIterator<Item = i16>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::int2(data),
		}
	}

	pub fn int2_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = i16>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::int2_with_bitvec(data, bitvec),
		}
	}

	pub fn int4(name: impl Into<Fragment>, data: impl IntoIterator<Item = i32>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::int4(data),
		}
	}

	pub fn int4_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = i32>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::int4_with_bitvec(data, bitvec),
		}
	}

	pub fn int8(name: impl Into<Fragment>, data: impl IntoIterator<Item = i64>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::int8(data),
		}
	}

	pub fn int8_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = i64>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::int8_with_bitvec(data, bitvec),
		}
	}

	pub fn int16(name: impl Into<Fragment>, data: impl IntoIterator<Item = i128>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::int16(data),
		}
	}

	pub fn int16_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = i128>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::int16_with_bitvec(data, bitvec),
		}
	}

	pub fn uint1(name: impl Into<Fragment>, data: impl IntoIterator<Item = u8>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uint1(data),
		}
	}

	pub fn uint1_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = u8>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uint1_with_bitvec(data, bitvec),
		}
	}

	pub fn uint2(name: impl Into<Fragment>, data: impl IntoIterator<Item = u16>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uint2(data),
		}
	}

	pub fn uint2_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = u16>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uint2_with_bitvec(data, bitvec),
		}
	}

	pub fn uint4(name: impl Into<Fragment>, data: impl IntoIterator<Item = u32>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uint4(data),
		}
	}

	pub fn uint4_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = u32>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uint4_with_bitvec(data, bitvec),
		}
	}

	pub fn uint8(name: impl Into<Fragment>, data: impl IntoIterator<Item = u64>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uint8(data),
		}
	}

	pub fn uint8_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = u64>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uint8_with_bitvec(data, bitvec),
		}
	}

	pub fn uint16(name: impl Into<Fragment>, data: impl IntoIterator<Item = u128>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uint16(data),
		}
	}

	pub fn uint16_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = u128>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uint16_with_bitvec(data, bitvec),
		}
	}

	pub fn float4(name: impl Into<Fragment>, data: impl IntoIterator<Item = f32>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::float4(data),
		}
	}

	pub fn float4_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = f32>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::float4_with_bitvec(data, bitvec),
		}
	}

	pub fn float8(name: impl Into<Fragment>, data: impl IntoIterator<Item = f64>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::float8(data),
		}
	}

	pub fn float8_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = f64>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::float8_with_bitvec(data, bitvec),
		}
	}

	pub fn bool(name: impl Into<Fragment>, data: impl IntoIterator<Item = bool>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::bool(data),
		}
	}

	pub fn bool_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = bool>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::bool_with_bitvec(data, bitvec),
		}
	}

	pub fn utf8(name: impl Into<Fragment>, data: impl IntoIterator<Item = String>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::utf8(data),
		}
	}

	pub fn utf8_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = String>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::utf8_with_bitvec(data, bitvec),
		}
	}

	pub fn uuid4(name: impl Into<Fragment>, data: impl IntoIterator<Item = Uuid4>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uuid4(data),
		}
	}

	pub fn uuid4_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = Uuid4>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uuid4_with_bitvec(data, bitvec),
		}
	}

	pub fn uuid7(name: impl Into<Fragment>, data: impl IntoIterator<Item = Uuid7>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uuid7(data),
		}
	}

	pub fn uuid7_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = Uuid7>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::uuid7_with_bitvec(data, bitvec),
		}
	}

	pub fn dictionary_id(name: impl Into<Fragment>, data: impl IntoIterator<Item = DictionaryEntryId>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::dictionary_id(data),
		}
	}

	pub fn dictionary_id_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = DictionaryEntryId>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::dictionary_id_with_bitvec(data, bitvec),
		}
	}

	pub fn undefined_typed(name: impl Into<Fragment>, ty: Type, row_count: usize) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::none_typed(ty, row_count),
		}
	}
}
