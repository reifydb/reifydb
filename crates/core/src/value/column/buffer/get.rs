// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{any::type_name, result::Result as StdResult};

use reifydb_codec::{key::serializer::KeySerializer, tag::ValueKind};
use reifydb_value::{
	Result,
	error::{ColumnReadReason, Error, TypeError},
	value::{
		Value,
		container::{
			any_array, bool_array,
			decimal_array::{
				decimal_at, decimal_get_value, int_at, int_get_value, u128_at, uint_at, uint_get_value,
				uint16_get_value,
			},
			dictionary_array, digest_array, primitive,
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
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason>;
}

impl ColumnBuffer {
	pub fn get_value(&self, index: usize) -> Value {
		if self.none_at(index) {
			return Value::None {
				inner: self.base_type(),
			};
		}
		match self {
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
			ColumnBuffer::Int(a) => int_get_value(a, index),
			ColumnBuffer::Uint(a) => uint_get_value(a, index),
			ColumnBuffer::Decimal(a) => decimal_get_value(a, index),
			ColumnBuffer::Any {
				container,
				declared_type,
			} => any_array::get_value(container, declared_type.as_ref(), index),
			ColumnBuffer::Digest {
				container,
				..
			} => digest_array::get_value(container, index),
			_ => with_container!(self, |a| primitive::get_value(a, index)),
		}
	}

	pub fn get_as<T: FromColumnBuffer>(&self, index: usize) -> Result<Option<T>> {
		if self.none_at(index) {
			return Ok(None);
		}
		T::from_column_buffer(self, index).map_err(|reason| read_error::<T>(self, reason))
	}

	pub fn extend_key(&self, index: usize, serializer: &mut KeySerializer) -> Result<()> {
		if self.none_at(index) {
			serializer.try_extend_value(&Value::None {
				inner: self.base_type(),
			})?;
			return Ok(());
		}
		match self {
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
			} if !self.none_at(index) => varlen_array::get(container, index),
			_ => None,
		}
	}

	pub fn get_bytes(&self, index: usize) -> Option<&[u8]> {
		match self {
			ColumnBuffer::Blob {
				container,
				..
			} if !self.none_at(index) => varlen_array::get(container, index),
			_ => None,
		}
	}
}

fn read_error<T>(data: &ColumnBuffer, reason: ColumnReadReason) -> Error {
	TypeError::ColumnRead {
		column_type: data.base_type(),
		target: type_name::<T>(),
		reason,
	}
	.into()
}

fn wrong_type<T>() -> StdResult<Option<T>, ColumnReadReason> {
	Err(ColumnReadReason::WrongType)
}

macro_rules! impl_from_column_data_widening {
	($($t:ty => [$($variant:ident),*]),* $(,)?) => { $(
		impl FromColumnBuffer for $t {
			fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
				match data {
					$(ColumnBuffer::$variant(c) => Ok(c.values().get(index).map(|&v| <$t>::from(v))),)*
					_ => wrong_type(),
				}
			}
		}
	)* };
}

impl_from_column_data_widening!(
	i8 => [Int1],
	i16 => [Int1, Int2],
	i32 => [Int1, Int2, Int4],
	i64 => [Int1, Int2, Int4, Int8],
	i128 => [Int1, Int2, Int4, Int8, Int16],
	u8 => [Uint1],
	u16 => [Uint1, Uint2],
	u32 => [Uint1, Uint2, Uint4],
	u64 => [Uint1, Uint2, Uint4, Uint8],
	f32 => [Float4],
	f64 => [Float4, Float8],
);

impl FromColumnBuffer for u128 {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::Uint1(c) => Ok(c.values().get(index).map(|&v| u128::from(v))),
			ColumnBuffer::Uint2(c) => Ok(c.values().get(index).map(|&v| u128::from(v))),
			ColumnBuffer::Uint4(c) => Ok(c.values().get(index).map(|&v| u128::from(v))),
			ColumnBuffer::Uint8(c) => Ok(c.values().get(index).map(|&v| u128::from(v))),
			ColumnBuffer::Uint16(c) => Ok(u128_at(c, index)),
			_ => wrong_type(),
		}
	}
}

impl FromColumnBuffer for bool {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::Bool(c) => Ok((index < c.len()).then(|| c.value(index))),
			_ => wrong_type(),
		}
	}
}

impl FromColumnBuffer for String {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::Utf8 {
				container,
				..
			} => Ok(varlen_array::get(container, index).map(str::to_string)),
			_ => wrong_type(),
		}
	}
}

impl FromColumnBuffer for Vec<u8> {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::Blob {
				container,
				..
			} => Ok(varlen_array::get(container, index).map(<[u8]>::to_vec)),
			_ => wrong_type(),
		}
	}
}

impl FromColumnBuffer for Date {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::Date(c) => Ok(dates(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}

impl FromColumnBuffer for DateTime {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::DateTime(c) => Ok(datetimes(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}

impl FromColumnBuffer for Time {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::Time(c) => Ok(times(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}

impl FromColumnBuffer for Duration {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::Duration(c) => Ok(durations(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}

impl FromColumnBuffer for Uuid4 {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::Uuid4(c) => Ok(uuid4s(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}

impl FromColumnBuffer for Uuid7 {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::Uuid7(c) => Ok(uuid7s(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}

impl FromColumnBuffer for Int {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::Int(a) => Ok(int_at(a, index)),
			_ => wrong_type(),
		}
	}
}

impl FromColumnBuffer for Uint {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::Uint(a) => Ok(uint_at(a, index)),
			_ => wrong_type(),
		}
	}
}

impl FromColumnBuffer for Decimal {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::Decimal(a) => Ok(decimal_at(a, index)),
			ColumnBuffer::Float4(c) => c
				.values()
				.get(index)
				.map(|&v| Decimal::from_f32(v).ok_or(ColumnReadReason::DoesNotFit))
				.transpose(),
			ColumnBuffer::Float8(c) => c
				.values()
				.get(index)
				.map(|&v| Decimal::from_f64(v).ok_or(ColumnReadReason::DoesNotFit))
				.transpose(),
			_ => wrong_type(),
		}
	}
}

impl FromColumnBuffer for IdentityId {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> StdResult<Option<Self>, ColumnReadReason> {
		match data {
			ColumnBuffer::IdentityId(c) => Ok(identity_ids(c).get(index).copied()),
			_ => wrong_type(),
		}
	}
}
