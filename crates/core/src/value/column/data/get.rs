// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use num_traits::NumCast;
use reifydb_type::value::{
	Value,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	duration::Duration,
	identity::IdentityId,
	int::Int,
	time::Time,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
};

use crate::value::column::ColumnData;

pub trait FromColumnData: Sized {
	fn from_column_data(data: &ColumnData, index: usize) -> Option<Self>;
}

impl ColumnData {
	pub fn get_value(&self, index: usize) -> Value {
		match self {
			ColumnData::Bool(container) => container.get_value(index),
			ColumnData::Float4(container) => container.get_value(index),
			ColumnData::Float8(container) => container.get_value(index),
			ColumnData::Int1(container) => container.get_value(index),
			ColumnData::Int2(container) => container.get_value(index),
			ColumnData::Int4(container) => container.get_value(index),
			ColumnData::Int8(container) => container.get_value(index),
			ColumnData::Int16(container) => container.get_value(index),
			ColumnData::Uint1(container) => container.get_value(index),
			ColumnData::Uint2(container) => container.get_value(index),
			ColumnData::Uint4(container) => container.get_value(index),
			ColumnData::Uint8(container) => container.get_value(index),
			ColumnData::Uint16(container) => container.get_value(index),
			ColumnData::Utf8 {
				container,
				..
			} => container.get_value(index),
			ColumnData::Date(container) => container.get_value(index),
			ColumnData::DateTime(container) => container.get_value(index),
			ColumnData::Time(container) => container.get_value(index),
			ColumnData::Duration(container) => container.get_value(index),
			ColumnData::IdentityId(container) => container.get_value(index),
			ColumnData::Uuid4(container) => container.get_value(index),
			ColumnData::Uuid7(container) => container.get_value(index),
			ColumnData::Blob {
				container,
				..
			} => container.get_value(index),
			ColumnData::Int {
				container,
				..
			} => container.get_value(index),
			ColumnData::Uint {
				container,
				..
			} => container.get_value(index),
			ColumnData::Decimal {
				container,
				..
			} => container.get_value(index),
			ColumnData::Any(container) => container.get_value(index),
			ColumnData::DictionaryId(container) => container.get_value(index),
			ColumnData::Undefined(container) => container.get_value(index),
		}
	}

	pub fn get_as<T: FromColumnData>(&self, index: usize) -> Option<T> {
		T::from_column_data(self, index)
	}
}

macro_rules! impl_from_column_data_numeric {
	($($t:ty),*) => { $(
		impl FromColumnData for $t {
			fn from_column_data(data: &ColumnData, index: usize) -> Option<Self> {
				match data {
					ColumnData::Int1(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnData::Int2(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnData::Int4(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnData::Int8(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnData::Int16(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnData::Uint1(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnData::Uint2(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnData::Uint4(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnData::Uint8(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnData::Uint16(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnData::Float4(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnData::Float8(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnData::Int { container, .. } => container.get(index).and_then(|v| NumCast::from(v.0.clone())),
					ColumnData::Uint { container, .. } => container.get(index).and_then(|v| NumCast::from(v.0.clone())),
					_ => None,
				}
			}
		}
	)* };
}

impl_from_column_data_numeric!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64);

impl FromColumnData for bool {
	fn from_column_data(data: &ColumnData, index: usize) -> Option<Self> {
		match data {
			ColumnData::Bool(c) => c.get(index),
			_ => None,
		}
	}
}

impl FromColumnData for String {
	fn from_column_data(data: &ColumnData, index: usize) -> Option<Self> {
		match data {
			ColumnData::Utf8 {
				container,
				..
			} => container.get(index).cloned(),
			_ => None,
		}
	}
}

impl FromColumnData for Date {
	fn from_column_data(data: &ColumnData, index: usize) -> Option<Self> {
		match data {
			ColumnData::Date(c) => c.get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnData for DateTime {
	fn from_column_data(data: &ColumnData, index: usize) -> Option<Self> {
		match data {
			ColumnData::DateTime(c) => c.get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnData for Time {
	fn from_column_data(data: &ColumnData, index: usize) -> Option<Self> {
		match data {
			ColumnData::Time(c) => c.get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnData for Duration {
	fn from_column_data(data: &ColumnData, index: usize) -> Option<Self> {
		match data {
			ColumnData::Duration(c) => c.get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnData for Uuid4 {
	fn from_column_data(data: &ColumnData, index: usize) -> Option<Self> {
		match data {
			ColumnData::Uuid4(c) => c.get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnData for Uuid7 {
	fn from_column_data(data: &ColumnData, index: usize) -> Option<Self> {
		match data {
			ColumnData::Uuid7(c) => c.get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnData for Int {
	fn from_column_data(data: &ColumnData, index: usize) -> Option<Self> {
		match data {
			ColumnData::Int {
				container,
				..
			} => container.get(index).cloned(),
			_ => None,
		}
	}
}

impl FromColumnData for Uint {
	fn from_column_data(data: &ColumnData, index: usize) -> Option<Self> {
		match data {
			ColumnData::Uint {
				container,
				..
			} => container.get(index).cloned(),
			_ => None,
		}
	}
}

impl FromColumnData for Decimal {
	fn from_column_data(data: &ColumnData, index: usize) -> Option<Self> {
		match data {
			ColumnData::Decimal {
				container,
				..
			} => container.get(index).cloned(),
			_ => None,
		}
	}
}

impl FromColumnData for IdentityId {
	fn from_column_data(data: &ColumnData, index: usize) -> Option<Self> {
		match data {
			ColumnData::IdentityId(c) => c.get(index),
			_ => None,
		}
	}
}
