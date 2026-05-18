// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_traits::NumCast;
use reifydb_type::{
	storage::DataBitVec,
	value::{
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
	},
};

use crate::value::column::{ColumnBuffer, buffer::with_container};

pub trait FromColumnBuffer: Sized {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self>;
}

impl ColumnBuffer {
	pub fn get_value(&self, index: usize) -> Value {
		match self {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				if index < DataBitVec::len(bitvec) && DataBitVec::get(bitvec, index) {
					inner.get_value(index)
				} else {
					Value::None {
						inner: inner.get_type(),
					}
				}
			}
			_ => with_container!(self, |c| c.get_value(index)),
		}
	}

	pub fn get_as<T: FromColumnBuffer>(&self, index: usize) -> Option<T> {
		T::from_column_buffer(self, index)
	}
}

macro_rules! impl_from_column_data_numeric {
	($($t:ty),*) => { $(
		impl FromColumnBuffer for $t {
			fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
				match data {
					ColumnBuffer::Int1(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Int2(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Int4(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Int8(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Int16(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Uint1(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Uint2(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Uint4(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Uint8(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Uint16(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Float4(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Float8(c) => c.get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Int { container, .. } => container.get(index).and_then(|v| NumCast::from(v.0.clone())),
					ColumnBuffer::Uint { container, .. } => container.get(index).and_then(|v| NumCast::from(v.0.clone())),
					_ => None,
				}
			}
		}
	)* };
}

impl_from_column_data_numeric!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64);

impl FromColumnBuffer for bool {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Bool(c) => c.get(index),
			_ => None,
		}
	}
}

impl FromColumnBuffer for String {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Utf8 {
				container,
				..
			} => container.get(index).map(str::to_string),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Date {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Date(c) => c.get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for DateTime {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::DateTime(c) => c.get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Time {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Time(c) => c.get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Duration {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Duration(c) => c.get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Uuid4 {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Uuid4(c) => c.get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Uuid7 {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Uuid7(c) => c.get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Int {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Int {
				container,
				..
			} => container.get(index).cloned(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Uint {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Uint {
				container,
				..
			} => container.get(index).cloned(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Decimal {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Decimal {
				container,
				..
			} => container.get(index).cloned(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for IdentityId {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::IdentityId(c) => c.get(index),
			_ => None,
		}
	}
}
