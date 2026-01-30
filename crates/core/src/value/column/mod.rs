// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	fragment::Fragment,
	util::bitvec::BitVec,
	value::{
		dictionary::DictionaryEntryId,
		r#type::Type,
		uuid::{Uuid4, Uuid7},
	},
};

use crate::value::column::data::ColumnData;

pub mod columns;
pub mod compressed;
pub mod data;
pub mod frame;
pub mod headers;
#[allow(dead_code, unused_variables)]
pub mod pool;
pub mod push;
pub mod row;
pub mod transform;
pub mod view;

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
	pub name: Fragment,
	pub data: ColumnData,
}

impl Column {
	pub fn new(name: impl Into<Fragment>, data: ColumnData) -> Self {
		Self {
			name: name.into(),
			data,
		}
	}

	pub fn get_type(&self) -> Type {
		self.data.get_type()
	}

	pub fn with_new_data(&self, data: ColumnData) -> Column {
		Column {
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

	pub fn data(&self) -> &ColumnData {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut ColumnData {
		&mut self.data
	}

	/// Convert to a 'static lifetime version
	pub fn to_static(&self) -> Column {
		Column {
			name: self.name.clone(),
			data: self.data.clone(),
		}
	}

	pub fn int1(name: impl Into<Fragment>, data: impl IntoIterator<Item = i8>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int1(data),
		}
	}

	pub fn int1_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = i8>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int1_with_bitvec(data, bitvec),
		}
	}

	pub fn int2(name: impl Into<Fragment>, data: impl IntoIterator<Item = i16>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int2(data),
		}
	}

	pub fn int2_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = i16>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int2_with_bitvec(data, bitvec),
		}
	}

	pub fn int4(name: impl Into<Fragment>, data: impl IntoIterator<Item = i32>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int4(data),
		}
	}

	pub fn int4_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = i32>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int4_with_bitvec(data, bitvec),
		}
	}

	pub fn int8(name: impl Into<Fragment>, data: impl IntoIterator<Item = i64>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int8(data),
		}
	}

	pub fn int8_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = i64>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int8_with_bitvec(data, bitvec),
		}
	}

	pub fn int16(name: impl Into<Fragment>, data: impl IntoIterator<Item = i128>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int16(data),
		}
	}

	pub fn int16_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = i128>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int16_with_bitvec(data, bitvec),
		}
	}

	pub fn uint1(name: impl Into<Fragment>, data: impl IntoIterator<Item = u8>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint1(data),
		}
	}

	pub fn uint1_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = u8>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint1_with_bitvec(data, bitvec),
		}
	}

	pub fn uint2(name: impl Into<Fragment>, data: impl IntoIterator<Item = u16>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint2(data),
		}
	}

	pub fn uint2_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = u16>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint2_with_bitvec(data, bitvec),
		}
	}

	pub fn uint4(name: impl Into<Fragment>, data: impl IntoIterator<Item = u32>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint4(data),
		}
	}

	pub fn uint4_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = u32>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint4_with_bitvec(data, bitvec),
		}
	}

	pub fn uint8(name: impl Into<Fragment>, data: impl IntoIterator<Item = u64>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint8(data),
		}
	}

	pub fn uint8_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = u64>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint8_with_bitvec(data, bitvec),
		}
	}

	pub fn uint16(name: impl Into<Fragment>, data: impl IntoIterator<Item = u128>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint16(data),
		}
	}

	pub fn uint16_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = u128>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint16_with_bitvec(data, bitvec),
		}
	}

	pub fn float4(name: impl Into<Fragment>, data: impl IntoIterator<Item = f32>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::float4(data),
		}
	}

	pub fn float4_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = f32>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::float4_with_bitvec(data, bitvec),
		}
	}

	pub fn float8(name: impl Into<Fragment>, data: impl IntoIterator<Item = f64>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::float8(data),
		}
	}

	pub fn float8_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = f64>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::float8_with_bitvec(data, bitvec),
		}
	}

	pub fn bool(name: impl Into<Fragment>, data: impl IntoIterator<Item = bool>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::bool(data),
		}
	}

	pub fn bool_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = bool>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::bool_with_bitvec(data, bitvec),
		}
	}

	pub fn utf8(name: impl Into<Fragment>, data: impl IntoIterator<Item = String>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::utf8(data),
		}
	}

	pub fn utf8_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = String>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::utf8_with_bitvec(data, bitvec),
		}
	}

	pub fn uuid4(name: impl Into<Fragment>, data: impl IntoIterator<Item = Uuid4>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uuid4(data),
		}
	}

	pub fn uuid4_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = Uuid4>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uuid4_with_bitvec(data, bitvec),
		}
	}

	pub fn uuid7(name: impl Into<Fragment>, data: impl IntoIterator<Item = Uuid7>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uuid7(data),
		}
	}

	pub fn uuid7_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = Uuid7>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uuid7_with_bitvec(data, bitvec),
		}
	}

	pub fn dictionary_id(name: impl Into<Fragment>, data: impl IntoIterator<Item = DictionaryEntryId>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::dictionary_id(data),
		}
	}

	pub fn dictionary_id_with_bitvec(
		name: impl Into<Fragment>,
		data: impl IntoIterator<Item = DictionaryEntryId>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::dictionary_id_with_bitvec(data, bitvec),
		}
	}

	pub fn undefined(name: impl Into<Fragment>, row_count: usize) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::undefined(row_count),
		}
	}

	pub fn undefined_typed(name: impl Into<Fragment>, ty: Type, row_count: usize) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::undefined_typed(ty, row_count),
		}
	}
}
