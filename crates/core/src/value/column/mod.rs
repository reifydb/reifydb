// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{Fragment, IntoFragment, Type};

mod columns;
mod data;
pub mod frame;
pub mod layout;
#[allow(dead_code, unused_variables)]
pub mod pool;
pub mod push;
mod transform;
mod view;

pub use columns::Columns;
pub use data::ColumnData;
pub use view::group_by::{GroupByView, GroupKey};

#[derive(Clone, Debug, PartialEq)]
pub struct Column<'a> {
	pub name: Fragment<'a>,
	pub data: ColumnData,
}

impl<'a> Column<'a> {
	pub fn new(name: impl IntoFragment<'a>, data: ColumnData) -> Self {
		Self {
			name: name.into_fragment(),
			data,
		}
	}

	pub fn get_type(&self) -> Type {
		self.data.get_type()
	}

	pub fn with_new_data(&self, data: ColumnData) -> Column<'a> {
		Column {
			name: self.name.clone(),
			data,
		}
	}

	pub fn name(&self) -> &Fragment<'a> {
		&self.name
	}

	pub fn name_owned(&self) -> Fragment<'a> {
		self.name.clone()
	}

	pub fn data(&self) -> &ColumnData {
		&self.data
	}

	pub fn data_mut(&mut self) -> &mut ColumnData {
		&mut self.data
	}

	/// Convert to a 'static lifetime version
	pub fn to_static(&self) -> Column<'static> {
		Column {
			name: self.name.clone().to_static(),
			data: self.data.clone(),
		}
	}

	pub fn int1(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = i8>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int1(data),
		}
	}

	pub fn int1_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = i8>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int1_with_bitvec(data, bitvec),
		}
	}

	pub fn int2(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = i16>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int2(data),
		}
	}

	pub fn int2_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = i16>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int2_with_bitvec(data, bitvec),
		}
	}

	pub fn int4(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = i32>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int4(data),
		}
	}

	pub fn int4_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = i32>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int4_with_bitvec(data, bitvec),
		}
	}

	pub fn int8(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = i64>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int8(data),
		}
	}

	pub fn int8_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = i64>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int8_with_bitvec(data, bitvec),
		}
	}

	pub fn int16(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = i128>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int16(data),
		}
	}

	pub fn int16_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = i128>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::int16_with_bitvec(data, bitvec),
		}
	}

	pub fn uint1(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = u8>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint1(data),
		}
	}

	pub fn uint1_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = u8>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint1_with_bitvec(data, bitvec),
		}
	}

	pub fn uint2(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = u16>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint2(data),
		}
	}

	pub fn uint2_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = u16>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint2_with_bitvec(data, bitvec),
		}
	}

	pub fn uint4(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = u32>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint4(data),
		}
	}

	pub fn uint4_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = u32>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint4_with_bitvec(data, bitvec),
		}
	}

	pub fn uint8(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = u64>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint8(data),
		}
	}

	pub fn uint8_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = u64>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint8_with_bitvec(data, bitvec),
		}
	}

	pub fn uint16(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = u128>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint16(data),
		}
	}

	pub fn uint16_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = u128>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uint16_with_bitvec(data, bitvec),
		}
	}

	pub fn float4(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = f32>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::float4(data),
		}
	}

	pub fn float4_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = f32>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::float4_with_bitvec(data, bitvec),
		}
	}

	pub fn float8(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = f64>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::float8(data),
		}
	}

	pub fn float8_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = f64>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::float8_with_bitvec(data, bitvec),
		}
	}

	pub fn bool(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = bool>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::bool(data),
		}
	}

	pub fn bool_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = bool>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::bool_with_bitvec(data, bitvec),
		}
	}

	pub fn utf8(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = String>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::utf8(data),
		}
	}

	pub fn utf8_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = String>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::utf8_with_bitvec(data, bitvec),
		}
	}

	pub fn undefined(name: impl Into<Fragment<'a>>, row_count: usize) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::undefined(row_count),
		}
	}

	pub fn uuid4(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = reifydb_type::Uuid4>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uuid4(data),
		}
	}

	pub fn uuid4_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = reifydb_type::Uuid4>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uuid4_with_bitvec(data, bitvec),
		}
	}

	pub fn uuid7(name: impl Into<Fragment<'a>>, data: impl IntoIterator<Item = reifydb_type::Uuid7>) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uuid7(data),
		}
	}

	pub fn uuid7_with_bitvec(
		name: impl Into<Fragment<'a>>,
		data: impl IntoIterator<Item = reifydb_type::Uuid7>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: name.into(),
			data: ColumnData::uuid7_with_bitvec(data, bitvec),
		}
	}

	pub fn row_number(data: impl IntoIterator<Item = reifydb_type::RowNumber>) -> Self {
		Column {
			name: Fragment::borrowed_internal("__row_number"),
			data: ColumnData::row_number(data),
		}
	}

	pub fn row_number_with_bitvec(
		data: impl IntoIterator<Item = reifydb_type::RowNumber>,
		bitvec: impl Into<crate::BitVec>,
	) -> Self {
		Column {
			name: Fragment::borrowed_internal("__row_number"),
			data: ColumnData::row_number_with_bitvec(data, bitvec),
		}
	}
}
