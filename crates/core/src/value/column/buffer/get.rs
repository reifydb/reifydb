// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::any::type_name;

use arrow_buffer::i256;
use num_traits::{NumCast, ToPrimitive};
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
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>>;
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
		T::from_column_buffer(self, index)
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

fn wrong_type<T>(data: &ColumnBuffer) -> Result<Option<T>> {
	Err(read_error::<T>(data, ColumnReadReason::WrongType))
}

fn fit<T: NumCast, V: ToPrimitive>(data: &ColumnBuffer, value: Option<V>) -> Result<Option<T>> {
	value.map(|v| <T as NumCast>::from(v).ok_or_else(|| read_error::<T>(data, ColumnReadReason::DoesNotFit)))
		.transpose()
}

fn fit_wide<T: NumCast>(data: &ColumnBuffer, value: Option<i256>) -> Result<Option<T>> {
	value.map(|v| wide_cast(v).ok_or_else(|| read_error::<T>(data, ColumnReadReason::DoesNotFit))).transpose()
}

fn wide_cast<T: NumCast>(value: i256) -> Option<T> {
	if let Some(narrow) = value.to_i128() {
		return <T as NumCast>::from(narrow);
	}
	let (low, high) = value.to_parts();
	if high == 0
		&& let Some(cast) = <T as NumCast>::from(low)
	{
		return Some(cast);
	}
	<T as NumCast>::from(ToPrimitive::to_f64(&value)?)
}

macro_rules! impl_from_column_data_numeric {
	($($t:ty),*) => { $(
		impl FromColumnBuffer for $t {
			fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
				match data {
					ColumnBuffer::Int1(c) => fit(data, c.values().get(index).copied()),
					ColumnBuffer::Int2(c) => fit(data, c.values().get(index).copied()),
					ColumnBuffer::Int4(c) => fit(data, c.values().get(index).copied()),
					ColumnBuffer::Int8(c) => fit(data, c.values().get(index).copied()),
					ColumnBuffer::Int16(c) => fit(data, c.values().get(index).copied()),
					ColumnBuffer::Uint1(c) => fit(data, c.values().get(index).copied()),
					ColumnBuffer::Uint2(c) => fit(data, c.values().get(index).copied()),
					ColumnBuffer::Uint4(c) => fit(data, c.values().get(index).copied()),
					ColumnBuffer::Uint8(c) => fit(data, c.values().get(index).copied()),
					ColumnBuffer::Uint16(c) => fit(data, u128_at(c, index)),
					ColumnBuffer::Float4(c) => fit(data, c.values().get(index).copied()),
					ColumnBuffer::Float8(c) => fit(data, c.values().get(index).copied()),
					ColumnBuffer::Int(a) => fit_wide(data, int_at(a, index).map(|v| v.to_i256())),
					ColumnBuffer::Uint(a) => fit_wide(data, uint_at(a, index).map(|v| v.to_i256())),
					_ => wrong_type(data),
				}
			}
		}
	)* };
}

impl_from_column_data_numeric!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64);

impl FromColumnBuffer for bool {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
		match data {
			ColumnBuffer::Bool(c) => Ok((index < c.len()).then(|| c.value(index))),
			_ => wrong_type(data),
		}
	}
}

impl FromColumnBuffer for String {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
		match data {
			ColumnBuffer::Utf8 {
				container,
				..
			} => Ok(varlen_array::get(container, index).map(str::to_string)),
			_ => wrong_type(data),
		}
	}
}

impl FromColumnBuffer for Vec<u8> {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
		match data {
			ColumnBuffer::Blob {
				container,
				..
			} => Ok(varlen_array::get(container, index).map(<[u8]>::to_vec)),
			_ => wrong_type(data),
		}
	}
}

impl FromColumnBuffer for Date {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
		match data {
			ColumnBuffer::Date(c) => Ok(dates(c).get(index).copied()),
			_ => wrong_type(data),
		}
	}
}

impl FromColumnBuffer for DateTime {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
		match data {
			ColumnBuffer::DateTime(c) => Ok(datetimes(c).get(index).copied()),
			_ => wrong_type(data),
		}
	}
}

impl FromColumnBuffer for Time {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
		match data {
			ColumnBuffer::Time(c) => Ok(times(c).get(index).copied()),
			_ => wrong_type(data),
		}
	}
}

impl FromColumnBuffer for Duration {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
		match data {
			ColumnBuffer::Duration(c) => Ok(durations(c).get(index).copied()),
			_ => wrong_type(data),
		}
	}
}

impl FromColumnBuffer for Uuid4 {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
		match data {
			ColumnBuffer::Uuid4(c) => Ok(uuid4s(c).get(index).copied()),
			_ => wrong_type(data),
		}
	}
}

impl FromColumnBuffer for Uuid7 {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
		match data {
			ColumnBuffer::Uuid7(c) => Ok(uuid7s(c).get(index).copied()),
			_ => wrong_type(data),
		}
	}
}

impl FromColumnBuffer for Int {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
		match data {
			ColumnBuffer::Int(a) => Ok(int_at(a, index)),
			_ => wrong_type(data),
		}
	}
}

impl FromColumnBuffer for Uint {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
		match data {
			ColumnBuffer::Uint(a) => Ok(uint_at(a, index)),
			_ => wrong_type(data),
		}
	}
}

impl FromColumnBuffer for Decimal {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
		match data {
			ColumnBuffer::Decimal(a) => Ok(decimal_at(a, index)),
			_ => wrong_type(data),
		}
	}
}

impl FromColumnBuffer for IdentityId {
	fn from_column_buffer(data: &ColumnBuffer, index: usize) -> Result<Option<Self>> {
		match data {
			ColumnBuffer::IdentityId(c) => Ok(identity_ids(c).get(index).copied()),
			_ => wrong_type(data),
		}
	}
}
