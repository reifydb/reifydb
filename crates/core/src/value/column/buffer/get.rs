// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use num_traits::NumCast;
use reifydb_codec::{key::serializer::KeySerializer, tag::ValueKind};
use reifydb_value::{
	Result,
	value::{
		Value,
		container::{
			bool_array,
			decimal_array::{u128_at, uint16_get_value},
			dictionary_array, primitive,
			temporal_array::{self, dates, datetimes, durations, times},
			uuid_array::{self, identity_ids, uuid4s, uuid7s},
			varlen_array::{self, blob_get_value, utf8_get_value},
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
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
				if index < bitvec.len() && bitvec.value(index) {
					inner.get_value(index)
				} else {
					Value::None {
						inner: inner.get_type(),
					}
				}
			}
			ColumnBuffer::Date(a) => temporal_array::get_value(dates(a), index),
			ColumnBuffer::DateTime(a) => temporal_array::get_value(datetimes(a), index),
			ColumnBuffer::Time(a) => temporal_array::get_value(times(a), index),
			ColumnBuffer::Duration(a) => temporal_array::get_value(durations(a), index),
			ColumnBuffer::IdentityId(a) => uuid_array::identity_id_get_value(identity_ids(a), index),
			ColumnBuffer::Uuid4(a) => uuid_array::get_value(uuid4s(a), index),
			ColumnBuffer::Uuid7(a) => uuid_array::get_value(uuid7s(a), index),
			ColumnBuffer::Utf8 {
				container,
				..
			} => utf8_get_value(container, index),
			ColumnBuffer::Blob {
				container,
				..
			} => blob_get_value(container, index),
			ColumnBuffer::Bool(c) => bool_array::get_value(c, index),
			ColumnBuffer::Uint16(a) => uint16_get_value(a, index),
			ColumnBuffer::DictionaryId {
				container,
				..
			} => dictionary_array::get_value(container, index),
			_ => with_container!(self, |c| c.get_value(index), |a| primitive::get_value(a, index)),
		}
	}

	pub fn get_as<T: FromColumnBuffer>(&self, index: usize) -> Option<T> {
		T::from_column_buffer(self, index)
	}

	pub fn extend_key(&self, index: usize, serializer: &mut KeySerializer) -> Result<()> {
		match self {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				if index < bitvec.len() && bitvec.value(index) {
					inner.extend_key(index, serializer)?;
				} else {
					serializer.try_extend_value(&Value::None {
						inner: inner.get_type(),
					})?;
				}
			}
			ColumnBuffer::Utf8 {
				container,
				..
			} => match varlen_array::get(container, index) {
				Some(text) => {
					serializer.extend_kind(ValueKind::Utf8).extend_str(text);
				}
				None => {
					serializer.extend_value(&Value::none_of(ValueType::Utf8));
				}
			},
			ColumnBuffer::Blob {
				container,
				..
			} => match varlen_array::get(container, index) {
				Some(bytes) => {
					serializer.extend_kind(ValueKind::Blob).extend_bytes(bytes);
				}
				None => {
					serializer.extend_value(&Value::none_of(ValueType::Blob));
				}
			},
			other => {
				serializer.try_extend_value(&other.get_value(index))?;
			}
		}
		Ok(())
	}

	pub fn get_str(&self, index: usize) -> Option<&str> {
		match self {
			ColumnBuffer::Utf8 {
				container,
				..
			} => varlen_array::get(container, index),
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				if index < bitvec.len() && bitvec.value(index) {
					inner.get_str(index)
				} else {
					None
				}
			}
			_ => None,
		}
	}

	pub fn get_bytes(&self, index: usize) -> Option<&[u8]> {
		match self {
			ColumnBuffer::Blob {
				container,
				..
			} => varlen_array::get(container, index),
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				if index < bitvec.len() && bitvec.value(index) {
					inner.get_bytes(index)
				} else {
					None
				}
			}
			_ => None,
		}
	}
}

macro_rules! impl_from_column_data_numeric {
	($($t:ty),*) => { $(
		impl FromColumnBuffer for $t {
			fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
				match data {
					ColumnBuffer::Int1(c) => c.values().get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Int2(c) => c.values().get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Int4(c) => c.values().get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Int8(c) => c.values().get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Int16(c) => c.values().get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Uint1(c) => c.values().get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Uint2(c) => c.values().get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Uint4(c) => c.values().get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Uint8(c) => c.values().get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Uint16(c) => u128_at(c, index).and_then(NumCast::from),
					ColumnBuffer::Float4(c) => c.values().get(index).and_then(|v| NumCast::from(*v)),
					ColumnBuffer::Float8(c) => c.values().get(index).and_then(|v| NumCast::from(*v)),
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
			ColumnBuffer::Bool(c) => (index < c.len()).then(|| c.value(index)),
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
			} => varlen_array::get(container, index).map(str::to_string),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Vec<u8> {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Blob {
				container,
				..
			} => varlen_array::get(container, index).map(<[u8]>::to_vec),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Date {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Date(c) => dates(c).get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for DateTime {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::DateTime(c) => datetimes(c).get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Time {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Time(c) => times(c).get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Duration {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Duration(c) => durations(c).get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Uuid4 {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Uuid4(c) => uuid4s(c).get(index).copied(),
			_ => None,
		}
	}
}

impl FromColumnBuffer for Uuid7 {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Option<Self> {
		match data {
			ColumnBuffer::Uuid7(c) => uuid7s(c).get(index).copied(),
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
			ColumnBuffer::IdentityId(c) => identity_ids(c).get(index).copied(),
			_ => None,
		}
	}
}
