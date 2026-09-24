// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{
	Array, BooleanArray, Date32Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float32Array,
	Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray, LargeBinaryArray,
	LargeStringArray, Time64NanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::BooleanBuffer;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

use crate::{
	util::{
		bitmap,
		float_format::{format_f32, format_f64},
	},
	value::{
		Value,
		container::{
			any_array,
			bignum_array::{
				decimal_as_string, decimal_get_value, decimals_equal, deserialize_decimals,
				deserialize_ints, deserialize_uints, int_as_string, int_get_value, serialize_decimals,
				serialize_ints, serialize_uints, uint_as_string, uint_get_value,
			},
			bool_array,
			decimal_array::{
				deserialize_int16s, deserialize_uint16s, serialize_uint16s, uint16_as_string,
				uint16_get_value,
			},
			dictionary_array, digest_array, primitive,
			temporal_array::{
				self, dates, datetimes, deserialize_dates, deserialize_datetimes,
				deserialize_durations, deserialize_times, durations, serialize_dates,
				serialize_datetimes, serialize_durations, serialize_times, times,
			},
			uuid_array::{
				self, deserialize_identity_ids, deserialize_uuid4s, deserialize_uuid7s, identity_ids,
				serialize_identity_ids, serialize_uuid4s, serialize_uuid7s, uuid4s, uuid7s,
			},
			varlen_array::{self, blob_as_string, blob_get_value, utf8_as_string, utf8_get_value},
		},
		dictionary::DictionaryId,
		value_type::ValueType,
	},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(remote = "Self")]
pub enum FrameColumnData {
	Bool(#[serde(with = "bool_array")] BooleanArray),
	Float4(#[serde(with = "primitive")] Float32Array),
	Float8(#[serde(with = "primitive")] Float64Array),
	Int1(#[serde(with = "primitive")] Int8Array),
	Int2(#[serde(with = "primitive")] Int16Array),
	Int4(#[serde(with = "primitive")] Int32Array),
	Int8(#[serde(with = "primitive")] Int64Array),
	Int16(
		#[serde(serialize_with = "primitive::serialize", deserialize_with = "deserialize_int16s")]
		Decimal128Array,
	),
	Uint1(#[serde(with = "primitive")] UInt8Array),
	Uint2(#[serde(with = "primitive")] UInt16Array),
	Uint4(#[serde(with = "primitive")] UInt32Array),
	Uint8(#[serde(with = "primitive")] UInt64Array),
	Uint16(
		#[serde(serialize_with = "serialize_uint16s", deserialize_with = "deserialize_uint16s")]
		Decimal256Array,
	),
	Utf8(
		#[serde(
			serialize_with = "varlen_array::serialize",
			deserialize_with = "varlen_array::deserialize_utf8"
		)]
		LargeStringArray,
	),
	Date(#[serde(serialize_with = "serialize_dates", deserialize_with = "deserialize_dates")] Date32Array),
	DateTime(
		#[serde(serialize_with = "serialize_datetimes", deserialize_with = "deserialize_datetimes")]
		UInt64Array,
	),
	Time(
		#[serde(serialize_with = "serialize_times", deserialize_with = "deserialize_times")]
		Time64NanosecondArray,
	),
	Duration(
		#[serde(serialize_with = "serialize_durations", deserialize_with = "deserialize_durations")]
		IntervalMonthDayNanoArray,
	),
	IdentityId(
		#[serde(serialize_with = "serialize_identity_ids", deserialize_with = "deserialize_identity_ids")]
		FixedSizeBinaryArray,
	),
	Uuid4(
		#[serde(serialize_with = "serialize_uuid4s", deserialize_with = "deserialize_uuid4s")]
		FixedSizeBinaryArray,
	),
	Uuid7(
		#[serde(serialize_with = "serialize_uuid7s", deserialize_with = "deserialize_uuid7s")]
		FixedSizeBinaryArray,
	),
	Blob(
		#[serde(
			serialize_with = "varlen_array::serialize",
			deserialize_with = "varlen_array::deserialize_blob"
		)]
		LargeBinaryArray,
	),
	Int(#[serde(serialize_with = "serialize_ints", deserialize_with = "deserialize_ints")] LargeBinaryArray),
	Uint(#[serde(serialize_with = "serialize_uints", deserialize_with = "deserialize_uints")] LargeBinaryArray),
	Decimal(
		#[serde(serialize_with = "serialize_decimals", deserialize_with = "deserialize_decimals")]
		LargeBinaryArray,
	),
	Any {
		#[serde(
			rename = "data",
			serialize_with = "any_array::serialize",
			deserialize_with = "any_array::deserialize"
		)]
		container: LargeBinaryArray,
		#[serde(default)]
		declared_type: Option<ValueType>,
	},
	DictionaryId {
		#[serde(
			rename = "data",
			serialize_with = "dictionary_array::serialize",
			deserialize_with = "dictionary_array::deserialize"
		)]
		container: FixedSizeBinaryArray,
		dictionary_id: Option<DictionaryId>,
	},

	Option {
		inner: Box<FrameColumnData>,
		#[serde(with = "bitmap")]
		bitvec: BooleanBuffer,
	},

	Digest {
		#[serde(serialize_with = "digest_array::serialize", deserialize_with = "digest_array::deserialize")]
		container: LargeBinaryArray,
		inner: ValueType,
		accuracy: u32,
	},
}

impl Serialize for FrameColumnData {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		FrameColumnData::serialize(self, serializer)
	}
}

impl<'de> Deserialize<'de> for FrameColumnData {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let data = FrameColumnData::deserialize(deserializer)?;
		if let FrameColumnData::Option {
			inner,
			bitvec,
		} = &data && bitvec.len() != inner.len()
		{
			return Err(D::Error::custom(format!(
				"Option column bitvec of {} bits does not match its {} rows",
				bitvec.len(),
				inner.len()
			)));
		}
		Ok(data)
	}
}

impl PartialEq for FrameColumnData {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(FrameColumnData::Bool(a), FrameColumnData::Bool(b)) => a.values() == b.values(),
			(FrameColumnData::Float4(a), FrameColumnData::Float4(b)) => a.values() == b.values(),
			(FrameColumnData::Float8(a), FrameColumnData::Float8(b)) => a.values() == b.values(),
			(FrameColumnData::Int1(a), FrameColumnData::Int1(b)) => a.values() == b.values(),
			(FrameColumnData::Int2(a), FrameColumnData::Int2(b)) => a.values() == b.values(),
			(FrameColumnData::Int4(a), FrameColumnData::Int4(b)) => a.values() == b.values(),
			(FrameColumnData::Int8(a), FrameColumnData::Int8(b)) => a.values() == b.values(),
			(FrameColumnData::Int16(a), FrameColumnData::Int16(b)) => a.values() == b.values(),
			(FrameColumnData::Uint1(a), FrameColumnData::Uint1(b)) => a.values() == b.values(),
			(FrameColumnData::Uint2(a), FrameColumnData::Uint2(b)) => a.values() == b.values(),
			(FrameColumnData::Uint4(a), FrameColumnData::Uint4(b)) => a.values() == b.values(),
			(FrameColumnData::Uint8(a), FrameColumnData::Uint8(b)) => a.values() == b.values(),
			(FrameColumnData::Uint16(a), FrameColumnData::Uint16(b)) => a.values() == b.values(),
			(FrameColumnData::Utf8(a), FrameColumnData::Utf8(b)) => varlen_array::equals(a, b),
			(FrameColumnData::Date(a), FrameColumnData::Date(b)) => dates(a) == dates(b),
			(FrameColumnData::DateTime(a), FrameColumnData::DateTime(b)) => datetimes(a) == datetimes(b),
			(FrameColumnData::Time(a), FrameColumnData::Time(b)) => times(a) == times(b),
			(FrameColumnData::Duration(a), FrameColumnData::Duration(b)) => durations(a) == durations(b),
			(FrameColumnData::IdentityId(a), FrameColumnData::IdentityId(b)) => {
				identity_ids(a) == identity_ids(b)
			}
			(FrameColumnData::Uuid4(a), FrameColumnData::Uuid4(b)) => uuid4s(a) == uuid4s(b),
			(FrameColumnData::Uuid7(a), FrameColumnData::Uuid7(b)) => uuid7s(a) == uuid7s(b),
			(FrameColumnData::Blob(a), FrameColumnData::Blob(b)) => varlen_array::equals(a, b),
			(FrameColumnData::Int(a), FrameColumnData::Int(b)) => varlen_array::equals(a, b),
			(FrameColumnData::Uint(a), FrameColumnData::Uint(b)) => varlen_array::equals(a, b),
			(FrameColumnData::Decimal(a), FrameColumnData::Decimal(b)) => decimals_equal(a, b),
			(
				FrameColumnData::Any {
					container: a_container,
					declared_type: a_declared_type,
				},
				FrameColumnData::Any {
					container: b_container,
					declared_type: b_declared_type,
				},
			) => any_array::equals(a_container, b_container) && a_declared_type == b_declared_type,
			(
				FrameColumnData::DictionaryId {
					container: a_container,
					dictionary_id: a_dictionary_id,
				},
				FrameColumnData::DictionaryId {
					container: b_container,
					dictionary_id: b_dictionary_id,
				},
			) => {
				dictionary_array::iter(a_container).eq(dictionary_array::iter(b_container))
					&& a_dictionary_id == b_dictionary_id
			}
			(
				FrameColumnData::Option {
					inner: a_inner,
					bitvec: a_bitvec,
				},
				FrameColumnData::Option {
					inner: b_inner,
					bitvec: b_bitvec,
				},
			) => a_inner == b_inner && a_bitvec == b_bitvec,
			(
				FrameColumnData::Digest {
					container: a_container,
					inner: a_inner,
					accuracy: a_accuracy,
				},
				FrameColumnData::Digest {
					container: b_container,
					inner: b_inner,
					accuracy: b_accuracy,
				},
			) => {
				varlen_array::equals(a_container, b_container)
					&& a_inner == b_inner && a_accuracy == b_accuracy
			}
			_ => false,
		}
	}
}

impl FrameColumnData {
	pub fn get_type(&self) -> ValueType {
		match self {
			FrameColumnData::Bool(_) => ValueType::Boolean,
			FrameColumnData::Float4(_) => ValueType::Float4,
			FrameColumnData::Float8(_) => ValueType::Float8,
			FrameColumnData::Int1(_) => ValueType::Int1,
			FrameColumnData::Int2(_) => ValueType::Int2,
			FrameColumnData::Int4(_) => ValueType::Int4,
			FrameColumnData::Int8(_) => ValueType::Int8,
			FrameColumnData::Int16(_) => ValueType::Int16,
			FrameColumnData::Uint1(_) => ValueType::Uint1,
			FrameColumnData::Uint2(_) => ValueType::Uint2,
			FrameColumnData::Uint4(_) => ValueType::Uint4,
			FrameColumnData::Uint8(_) => ValueType::Uint8,
			FrameColumnData::Uint16(_) => ValueType::Uint16,
			FrameColumnData::Utf8(_) => ValueType::Utf8,
			FrameColumnData::Date(_) => ValueType::Date,
			FrameColumnData::DateTime(_) => ValueType::DateTime,
			FrameColumnData::Time(_) => ValueType::Time,
			FrameColumnData::Duration(_) => ValueType::Duration,
			FrameColumnData::IdentityId(_) => ValueType::IdentityId,
			FrameColumnData::Uuid4(_) => ValueType::Uuid4,
			FrameColumnData::Uuid7(_) => ValueType::Uuid7,
			FrameColumnData::Blob(_) => ValueType::Blob,
			FrameColumnData::Int(_) => ValueType::Int,
			FrameColumnData::Uint(_) => ValueType::Uint,
			FrameColumnData::Decimal(_) => ValueType::Decimal,
			FrameColumnData::Any {
				declared_type,
				..
			} => declared_type.clone().unwrap_or(ValueType::Any),
			FrameColumnData::DictionaryId {
				..
			} => ValueType::DictionaryId,
			FrameColumnData::Option {
				inner,
				..
			} => ValueType::Option(Box::new(inner.get_type())),
			FrameColumnData::Digest {
				inner,
				accuracy,
				..
			} => ValueType::Digest {
				inner: Box::new(inner.clone()),
				accuracy: *accuracy,
			},
		}
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		match self {
			FrameColumnData::Bool(container) => idx < container.len(),
			FrameColumnData::Float4(container) => idx < container.len(),
			FrameColumnData::Float8(container) => idx < container.len(),
			FrameColumnData::Int1(container) => idx < container.len(),
			FrameColumnData::Int2(container) => idx < container.len(),
			FrameColumnData::Int4(container) => idx < container.len(),
			FrameColumnData::Int8(container) => idx < container.len(),
			FrameColumnData::Int16(container) => idx < container.len(),
			FrameColumnData::Uint1(container) => idx < container.len(),
			FrameColumnData::Uint2(container) => idx < container.len(),
			FrameColumnData::Uint4(container) => idx < container.len(),
			FrameColumnData::Uint8(container) => idx < container.len(),
			FrameColumnData::Uint16(container) => idx < container.len(),
			FrameColumnData::Utf8(container) => idx < container.len(),
			FrameColumnData::Date(container) => idx < container.len(),
			FrameColumnData::DateTime(container) => idx < container.len(),
			FrameColumnData::Time(container) => idx < container.len(),
			FrameColumnData::Duration(container) => idx < container.len(),
			FrameColumnData::IdentityId(container) => idx < container.len(),
			FrameColumnData::Uuid4(container) => idx < container.len(),
			FrameColumnData::Uuid7(container) => idx < container.len(),
			FrameColumnData::Blob(container) => idx < container.len(),
			FrameColumnData::Int(container) => idx < container.len(),
			FrameColumnData::Uint(container) => idx < container.len(),
			FrameColumnData::Decimal(container) => idx < container.len(),
			FrameColumnData::Any {
				container,
				..
			} => idx < container.len(),
			FrameColumnData::DictionaryId {
				container,
				..
			} => idx < container.len(),
			FrameColumnData::Option {
				bitvec,
				..
			} => idx < bitvec.len() && bitvec.value(idx),
			FrameColumnData::Digest {
				container,
				..
			} => digest_array::is_defined(container, idx),
		}
	}

	pub fn is_utf8(&self) -> bool {
		self.get_type() == ValueType::Utf8
	}

	pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Value> + 'a> {
		Box::new((0..self.len()).map(move |i| self.get_value(i)))
	}
}

impl FrameColumnData {
	pub fn len(&self) -> usize {
		match self {
			FrameColumnData::Bool(container) => container.len(),
			FrameColumnData::Float4(container) => container.len(),
			FrameColumnData::Float8(container) => container.len(),
			FrameColumnData::Int1(container) => container.len(),
			FrameColumnData::Int2(container) => container.len(),
			FrameColumnData::Int4(container) => container.len(),
			FrameColumnData::Int8(container) => container.len(),
			FrameColumnData::Int16(container) => container.len(),
			FrameColumnData::Uint1(container) => container.len(),
			FrameColumnData::Uint2(container) => container.len(),
			FrameColumnData::Uint4(container) => container.len(),
			FrameColumnData::Uint8(container) => container.len(),
			FrameColumnData::Uint16(container) => container.len(),
			FrameColumnData::Utf8(container) => container.len(),
			FrameColumnData::Date(container) => container.len(),
			FrameColumnData::DateTime(container) => container.len(),
			FrameColumnData::Time(container) => container.len(),
			FrameColumnData::Duration(container) => container.len(),
			FrameColumnData::IdentityId(container) => container.len(),
			FrameColumnData::Uuid4(container) => container.len(),
			FrameColumnData::Uuid7(container) => container.len(),
			FrameColumnData::Blob(container) => container.len(),
			FrameColumnData::Int(container) => container.len(),
			FrameColumnData::Uint(container) => container.len(),
			FrameColumnData::Decimal(container) => container.len(),
			FrameColumnData::Any {
				container,
				..
			} => container.len(),
			FrameColumnData::DictionaryId {
				container,
				..
			} => container.len(),
			FrameColumnData::Option {
				inner,
				..
			} => inner.len(),
			FrameColumnData::Digest {
				container,
				..
			} => container.len(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn as_string(&self, index: usize) -> String {
		match self {
			FrameColumnData::Bool(container) => bool_array::as_string(container, index),
			FrameColumnData::Float4(container) => {
				if index < container.len() {
					format_f32(container.value(index))
				} else {
					"none".to_string()
				}
			}
			FrameColumnData::Float8(container) => {
				if index < container.len() {
					format_f64(container.value(index))
				} else {
					"none".to_string()
				}
			}
			FrameColumnData::Int1(container) => primitive::as_string(container, index),
			FrameColumnData::Int2(container) => primitive::as_string(container, index),
			FrameColumnData::Int4(container) => primitive::as_string(container, index),
			FrameColumnData::Int8(container) => primitive::as_string(container, index),
			FrameColumnData::Int16(container) => primitive::as_string(container, index),
			FrameColumnData::Uint1(container) => primitive::as_string(container, index),
			FrameColumnData::Uint2(container) => primitive::as_string(container, index),
			FrameColumnData::Uint4(container) => primitive::as_string(container, index),
			FrameColumnData::Uint8(container) => primitive::as_string(container, index),
			FrameColumnData::Uint16(container) => uint16_as_string(container, index),
			FrameColumnData::Utf8(container) => utf8_as_string(container, index),
			FrameColumnData::Date(container) => temporal_array::as_string(dates(container), index),
			FrameColumnData::DateTime(container) => temporal_array::as_string(datetimes(container), index),
			FrameColumnData::Time(container) => temporal_array::as_string(times(container), index),
			FrameColumnData::Duration(container) => temporal_array::as_string(durations(container), index),
			FrameColumnData::IdentityId(container) => uuid_array::as_string(identity_ids(container), index),
			FrameColumnData::Uuid4(container) => uuid_array::as_string(uuid4s(container), index),
			FrameColumnData::Uuid7(container) => uuid_array::as_string(uuid7s(container), index),
			FrameColumnData::Blob(container) => blob_as_string(container, index),
			FrameColumnData::Int(container) => int_as_string(container, index),
			FrameColumnData::Uint(container) => uint_as_string(container, index),
			FrameColumnData::Decimal(container) => decimal_as_string(container, index),
			FrameColumnData::Any {
				container,
				..
			} => any_array::as_string(container, index),
			FrameColumnData::DictionaryId {
				container,
				..
			} => dictionary_array::as_string(container, index),
			FrameColumnData::Option {
				inner,
				bitvec,
			} => {
				if index < bitvec.len() && bitvec.value(index) {
					inner.as_string(index)
				} else {
					"none".to_string()
				}
			}
			FrameColumnData::Digest {
				container,
				..
			} => digest_array::as_string(container, index),
		}
	}
}

impl FrameColumnData {
	pub fn get_value(&self, index: usize) -> Value {
		match self {
			FrameColumnData::Bool(container) => bool_array::get_value(container, index),
			FrameColumnData::Float4(container) => primitive::get_value(container, index),
			FrameColumnData::Float8(container) => primitive::get_value(container, index),
			FrameColumnData::Int1(container) => primitive::get_value(container, index),
			FrameColumnData::Int2(container) => primitive::get_value(container, index),
			FrameColumnData::Int4(container) => primitive::get_value(container, index),
			FrameColumnData::Int8(container) => primitive::get_value(container, index),
			FrameColumnData::Int16(container) => primitive::get_value(container, index),
			FrameColumnData::Uint1(container) => primitive::get_value(container, index),
			FrameColumnData::Uint2(container) => primitive::get_value(container, index),
			FrameColumnData::Uint4(container) => primitive::get_value(container, index),
			FrameColumnData::Uint8(container) => primitive::get_value(container, index),
			FrameColumnData::Uint16(container) => uint16_get_value(container, index),
			FrameColumnData::Utf8(container) => utf8_get_value(container, index),
			FrameColumnData::Date(container) => temporal_array::get_value(dates(container), index),
			FrameColumnData::DateTime(container) => temporal_array::get_value(datetimes(container), index),
			FrameColumnData::Time(container) => temporal_array::get_value(times(container), index),
			FrameColumnData::Duration(container) => temporal_array::get_value(durations(container), index),
			FrameColumnData::IdentityId(container) => {
				uuid_array::identity_id_get_value(identity_ids(container), index)
			}
			FrameColumnData::Uuid4(container) => uuid_array::get_value(uuid4s(container), index),
			FrameColumnData::Uuid7(container) => uuid_array::get_value(uuid7s(container), index),
			FrameColumnData::Blob(container) => blob_get_value(container, index),
			FrameColumnData::Int(container) => int_get_value(container, index),
			FrameColumnData::Uint(container) => uint_get_value(container, index),
			FrameColumnData::Decimal(container) => decimal_get_value(container, index),
			FrameColumnData::Any {
				container,
				declared_type,
			} => any_array::get_value(container, declared_type.as_ref(), index),
			FrameColumnData::DictionaryId {
				container,
				..
			} => dictionary_array::get_value(container, index),
			FrameColumnData::Option {
				inner,
				bitvec,
			} => {
				if index < bitvec.len() && bitvec.value(index) {
					inner.get_value(index)
				} else {
					Value::none_of(inner.get_type())
				}
			}
			FrameColumnData::Digest {
				container,
				..
			} => digest_array::get_value(container, index),
		}
	}
}
