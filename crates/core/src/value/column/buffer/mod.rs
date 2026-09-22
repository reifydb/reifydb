// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod extend;
pub mod factory;
pub mod filter;
pub mod from;
pub mod get;
pub mod reorder;
pub mod scatter;
pub mod slice;
pub mod take;
pub mod write;

use std::fmt;

use arrow_array::{
	Array, BooleanArray, Date32Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float32Array,
	Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray, LargeBinaryArray,
	LargeStringArray, Time64NanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
	builder::LargeBinaryBuilder,
};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use reifydb_value::{
	util::bitmap,
	value::{
		Value,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		container::{
			any_array,
			bignum_array::{
				decimal_as_string, decimals_equal, deserialize_decimals, deserialize_ints,
				deserialize_uints, int_as_string, serialize_decimals, serialize_ints, serialize_uints,
				uint_as_string,
			},
			bool_array,
			decimal_array::{deserialize_int16s, deserialize_uint16s, serialize_uint16s, uint16_as_string},
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
			varlen_array,
		},
		dictionary::DictionaryId,
		digest::Digest,
		value_type::ValueType,
	},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError};

use crate::metrics::heap::HeapSize;

pub enum ColumnBuffer {
	Bool(BooleanArray),
	Float4(Float32Array),
	Float8(Float64Array),
	Int1(Int8Array),
	Int2(Int16Array),
	Int4(Int32Array),
	Int8(Int64Array),
	Int16(Decimal128Array),
	Uint1(UInt8Array),
	Uint2(UInt16Array),
	Uint4(UInt32Array),
	Uint8(UInt64Array),
	Uint16(Decimal256Array),
	Utf8 {
		container: LargeStringArray,
		max_bytes: MaxBytes,
	},
	Date(Date32Array),
	DateTime(UInt64Array),
	Time(Time64NanosecondArray),
	Duration(IntervalMonthDayNanoArray),
	IdentityId(FixedSizeBinaryArray),
	Uuid4(FixedSizeBinaryArray),
	Uuid7(FixedSizeBinaryArray),
	Blob {
		container: LargeBinaryArray,
		max_bytes: MaxBytes,
	},
	Int {
		container: LargeBinaryArray,
		max_bytes: MaxBytes,
	},
	Uint {
		container: LargeBinaryArray,
		max_bytes: MaxBytes,
	},
	Decimal {
		container: LargeBinaryArray,
		precision: Precision,
		scale: Scale,
	},

	Any {
		container: LargeBinaryArray,
		declared_type: Option<ValueType>,
	},

	DictionaryId {
		container: FixedSizeBinaryArray,
		dictionary_id: Option<DictionaryId>,
	},

	Digest {
		container: LargeBinaryArray,
		inner: ValueType,
		accuracy: u32,
	},
}

impl Clone for ColumnBuffer {
	fn clone(&self) -> Self {
		match self {
			ColumnBuffer::Bool(c) => ColumnBuffer::Bool(c.clone()),
			ColumnBuffer::Float4(c) => ColumnBuffer::Float4(c.clone()),
			ColumnBuffer::Float8(c) => ColumnBuffer::Float8(c.clone()),
			ColumnBuffer::Int1(c) => ColumnBuffer::Int1(c.clone()),
			ColumnBuffer::Int2(c) => ColumnBuffer::Int2(c.clone()),
			ColumnBuffer::Int4(c) => ColumnBuffer::Int4(c.clone()),
			ColumnBuffer::Int8(c) => ColumnBuffer::Int8(c.clone()),
			ColumnBuffer::Int16(c) => ColumnBuffer::Int16(c.clone()),
			ColumnBuffer::Uint1(c) => ColumnBuffer::Uint1(c.clone()),
			ColumnBuffer::Uint2(c) => ColumnBuffer::Uint2(c.clone()),
			ColumnBuffer::Uint4(c) => ColumnBuffer::Uint4(c.clone()),
			ColumnBuffer::Uint8(c) => ColumnBuffer::Uint8(c.clone()),
			ColumnBuffer::Uint16(c) => ColumnBuffer::Uint16(c.clone()),
			ColumnBuffer::Utf8 {
				container,
				max_bytes,
			} => ColumnBuffer::Utf8 {
				container: container.clone(),
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Date(c) => ColumnBuffer::Date(c.clone()),
			ColumnBuffer::DateTime(c) => ColumnBuffer::DateTime(c.clone()),
			ColumnBuffer::Time(c) => ColumnBuffer::Time(c.clone()),
			ColumnBuffer::Duration(c) => ColumnBuffer::Duration(c.clone()),
			ColumnBuffer::IdentityId(c) => ColumnBuffer::IdentityId(c.clone()),
			ColumnBuffer::Uuid4(c) => ColumnBuffer::Uuid4(c.clone()),
			ColumnBuffer::Uuid7(c) => ColumnBuffer::Uuid7(c.clone()),
			ColumnBuffer::Blob {
				container,
				max_bytes,
			} => ColumnBuffer::Blob {
				container: container.clone(),
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Int {
				container,
				max_bytes,
			} => ColumnBuffer::Int {
				container: container.clone(),
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Uint {
				container,
				max_bytes,
			} => ColumnBuffer::Uint {
				container: container.clone(),
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Decimal {
				container,
				precision,
				scale,
			} => ColumnBuffer::Decimal {
				container: container.clone(),
				precision: *precision,
				scale: *scale,
			},
			ColumnBuffer::Any {
				container,
				declared_type,
			} => ColumnBuffer::Any {
				container: container.clone(),
				declared_type: declared_type.clone(),
			},
			ColumnBuffer::DictionaryId {
				container,
				dictionary_id,
			} => ColumnBuffer::DictionaryId {
				container: container.clone(),
				dictionary_id: *dictionary_id,
			},
			ColumnBuffer::Digest {
				container,
				inner,
				accuracy,
			} => ColumnBuffer::Digest {
				container: container.clone(),
				inner: inner.clone(),
				accuracy: *accuracy,
			},
		}
	}
}

impl PartialEq for ColumnBuffer {
	fn eq(&self, other: &Self) -> bool {
		if self.nulls() != other.nulls() {
			return false;
		}
		match (self, other) {
			(ColumnBuffer::Bool(a), ColumnBuffer::Bool(b)) => a.values() == b.values(),
			(ColumnBuffer::Float4(a), ColumnBuffer::Float4(b)) => a.values() == b.values(),
			(ColumnBuffer::Float8(a), ColumnBuffer::Float8(b)) => a.values() == b.values(),
			(ColumnBuffer::Int1(a), ColumnBuffer::Int1(b)) => a.values() == b.values(),
			(ColumnBuffer::Int2(a), ColumnBuffer::Int2(b)) => a.values() == b.values(),
			(ColumnBuffer::Int4(a), ColumnBuffer::Int4(b)) => a.values() == b.values(),
			(ColumnBuffer::Int8(a), ColumnBuffer::Int8(b)) => a.values() == b.values(),
			(ColumnBuffer::Int16(a), ColumnBuffer::Int16(b)) => a.values() == b.values(),
			(ColumnBuffer::Uint1(a), ColumnBuffer::Uint1(b)) => a.values() == b.values(),
			(ColumnBuffer::Uint2(a), ColumnBuffer::Uint2(b)) => a.values() == b.values(),
			(ColumnBuffer::Uint4(a), ColumnBuffer::Uint4(b)) => a.values() == b.values(),
			(ColumnBuffer::Uint8(a), ColumnBuffer::Uint8(b)) => a.values() == b.values(),
			(ColumnBuffer::Uint16(a), ColumnBuffer::Uint16(b)) => a.values() == b.values(),
			(
				ColumnBuffer::Utf8 {
					container: a,
					max_bytes: am,
				},
				ColumnBuffer::Utf8 {
					container: b,
					max_bytes: bm,
				},
			) => varlen_array::equals(a, b) && am == bm,
			(ColumnBuffer::Date(a), ColumnBuffer::Date(b)) => dates(a) == dates(b),
			(ColumnBuffer::DateTime(a), ColumnBuffer::DateTime(b)) => datetimes(a) == datetimes(b),
			(ColumnBuffer::Time(a), ColumnBuffer::Time(b)) => times(a) == times(b),
			(ColumnBuffer::Duration(a), ColumnBuffer::Duration(b)) => durations(a) == durations(b),
			(ColumnBuffer::IdentityId(a), ColumnBuffer::IdentityId(b)) => {
				identity_ids(a) == identity_ids(b)
			}
			(ColumnBuffer::Uuid4(a), ColumnBuffer::Uuid4(b)) => uuid4s(a) == uuid4s(b),
			(ColumnBuffer::Uuid7(a), ColumnBuffer::Uuid7(b)) => uuid7s(a) == uuid7s(b),
			(
				ColumnBuffer::Blob {
					container: a,
					max_bytes: am,
				},
				ColumnBuffer::Blob {
					container: b,
					max_bytes: bm,
				},
			) => varlen_array::equals(a, b) && am == bm,
			(
				ColumnBuffer::Int {
					container: a,
					max_bytes: am,
				},
				ColumnBuffer::Int {
					container: b,
					max_bytes: bm,
				},
			) => varlen_array::equals(a, b) && am == bm,
			(
				ColumnBuffer::Uint {
					container: a,
					max_bytes: am,
				},
				ColumnBuffer::Uint {
					container: b,
					max_bytes: bm,
				},
			) => varlen_array::equals(a, b) && am == bm,
			(
				ColumnBuffer::Decimal {
					container: a,
					precision: ap,
					scale: as_,
				},
				ColumnBuffer::Decimal {
					container: b,
					precision: bp,
					scale: bs,
				},
			) => decimals_equal(a, b) && ap == bp && as_ == bs,
			(
				ColumnBuffer::Any {
					container: a,
					declared_type: ad,
				},
				ColumnBuffer::Any {
					container: b,
					declared_type: bd,
				},
			) => any_array::equals(a, b) && ad == bd,
			(
				ColumnBuffer::DictionaryId {
					container: a,
					dictionary_id: ad,
				},
				ColumnBuffer::DictionaryId {
					container: b,
					dictionary_id: bd,
				},
			) => dictionary_array::iter(a).eq(dictionary_array::iter(b)) && ad == bd,
			(
				ColumnBuffer::Digest {
					container: a,
					inner: ai,
					accuracy: aa,
				},
				ColumnBuffer::Digest {
					container: b,
					inner: bi,
					accuracy: ba,
				},
			) => varlen_array::equals(a, b) && ai == bi && aa == ba,
			_ => false,
		}
	}
}

impl fmt::Debug for ColumnBuffer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		if let Some(nulls) = self.nulls() {
			let (inner, _) = self.clone().split_nulls();
			return f.debug_struct("Option").field("inner", &inner).field("bitvec", nulls.inner()).finish();
		}
		match self {
			ColumnBuffer::Bool(c) => f.debug_tuple("Bool").field(c).finish(),
			ColumnBuffer::Float4(c) => f.debug_tuple("Float4").field(c).finish(),
			ColumnBuffer::Float8(c) => f.debug_tuple("Float8").field(c).finish(),
			ColumnBuffer::Int1(c) => f.debug_tuple("Int1").field(c).finish(),
			ColumnBuffer::Int2(c) => f.debug_tuple("Int2").field(c).finish(),
			ColumnBuffer::Int4(c) => f.debug_tuple("Int4").field(c).finish(),
			ColumnBuffer::Int8(c) => f.debug_tuple("Int8").field(c).finish(),
			ColumnBuffer::Int16(c) => f.debug_tuple("Int16").field(c).finish(),
			ColumnBuffer::Uint1(c) => f.debug_tuple("Uint1").field(c).finish(),
			ColumnBuffer::Uint2(c) => f.debug_tuple("Uint2").field(c).finish(),
			ColumnBuffer::Uint4(c) => f.debug_tuple("Uint4").field(c).finish(),
			ColumnBuffer::Uint8(c) => f.debug_tuple("Uint8").field(c).finish(),
			ColumnBuffer::Uint16(c) => f.debug_tuple("Uint16").field(c).finish(),
			ColumnBuffer::Utf8 {
				container,
				max_bytes,
			} => f.debug_struct("Utf8").field("container", container).field("max_bytes", max_bytes).finish(),
			ColumnBuffer::Date(c) => f.debug_tuple("Date").field(c).finish(),
			ColumnBuffer::DateTime(c) => f.debug_tuple("DateTime").field(c).finish(),
			ColumnBuffer::Time(c) => f.debug_tuple("Time").field(c).finish(),
			ColumnBuffer::Duration(c) => f.debug_tuple("Duration").field(c).finish(),
			ColumnBuffer::IdentityId(c) => f.debug_tuple("IdentityId").field(c).finish(),
			ColumnBuffer::Uuid4(c) => f.debug_tuple("Uuid4").field(c).finish(),
			ColumnBuffer::Uuid7(c) => f.debug_tuple("Uuid7").field(c).finish(),
			ColumnBuffer::Blob {
				container,
				max_bytes,
			} => f.debug_struct("Blob").field("container", container).field("max_bytes", max_bytes).finish(),
			ColumnBuffer::Int {
				container,
				max_bytes,
			} => f.debug_struct("Int").field("container", container).field("max_bytes", max_bytes).finish(),
			ColumnBuffer::Uint {
				container,
				max_bytes,
			} => f.debug_struct("Uint").field("container", container).field("max_bytes", max_bytes).finish(),
			ColumnBuffer::Decimal {
				container,
				precision,
				scale,
			} => f.debug_struct("Decimal")
				.field("container", container)
				.field("precision", precision)
				.field("scale", scale)
				.finish(),
			ColumnBuffer::Any {
				container,
				declared_type,
			} => f.debug_struct("Any")
				.field("container", container)
				.field("declared_type", declared_type)
				.finish(),
			ColumnBuffer::DictionaryId {
				container,
				dictionary_id,
			} => f.debug_struct("DictionaryId")
				.field("container", container)
				.field("dictionary_id", dictionary_id)
				.finish(),
			ColumnBuffer::Digest {
				container,
				inner,
				accuracy,
			} => f.debug_struct("Digest")
				.field("container", container)
				.field("inner", inner)
				.field("accuracy", accuracy)
				.finish(),
		}
	}
}

impl Serialize for ColumnBuffer {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		enum Helper<'a> {
			Bool(#[serde(serialize_with = "bool_array::serialize")] &'a BooleanArray),
			Float4(#[serde(serialize_with = "primitive::serialize")] &'a Float32Array),
			Float8(#[serde(serialize_with = "primitive::serialize")] &'a Float64Array),
			Int1(#[serde(serialize_with = "primitive::serialize")] &'a Int8Array),
			Int2(#[serde(serialize_with = "primitive::serialize")] &'a Int16Array),
			Int4(#[serde(serialize_with = "primitive::serialize")] &'a Int32Array),
			Int8(#[serde(serialize_with = "primitive::serialize")] &'a Int64Array),
			Int16(#[serde(serialize_with = "primitive::serialize")] &'a Decimal128Array),
			Uint1(#[serde(serialize_with = "primitive::serialize")] &'a UInt8Array),
			Uint2(#[serde(serialize_with = "primitive::serialize")] &'a UInt16Array),
			Uint4(#[serde(serialize_with = "primitive::serialize")] &'a UInt32Array),
			Uint8(#[serde(serialize_with = "primitive::serialize")] &'a UInt64Array),
			Uint16(#[serde(serialize_with = "serialize_uint16s")] &'a Decimal256Array),
			Utf8 {
				#[serde(serialize_with = "varlen_array::serialize")]
				container: &'a LargeStringArray,
				max_bytes: MaxBytes,
			},
			Date(#[serde(serialize_with = "serialize_dates")] &'a Date32Array),
			DateTime(#[serde(serialize_with = "serialize_datetimes")] &'a UInt64Array),
			Time(#[serde(serialize_with = "serialize_times")] &'a Time64NanosecondArray),
			Duration(#[serde(serialize_with = "serialize_durations")] &'a IntervalMonthDayNanoArray),
			IdentityId(#[serde(serialize_with = "serialize_identity_ids")] &'a FixedSizeBinaryArray),
			Uuid4(#[serde(serialize_with = "serialize_uuid4s")] &'a FixedSizeBinaryArray),
			Uuid7(#[serde(serialize_with = "serialize_uuid7s")] &'a FixedSizeBinaryArray),
			Blob {
				#[serde(serialize_with = "varlen_array::serialize")]
				container: &'a LargeBinaryArray,
				max_bytes: MaxBytes,
			},
			Int {
				#[serde(serialize_with = "serialize_ints")]
				container: &'a LargeBinaryArray,
				max_bytes: MaxBytes,
			},
			Uint {
				#[serde(serialize_with = "serialize_uints")]
				container: &'a LargeBinaryArray,
				max_bytes: MaxBytes,
			},
			Decimal {
				#[serde(serialize_with = "serialize_decimals")]
				container: &'a LargeBinaryArray,
				precision: Precision,
				scale: Scale,
			},
			Any(AnyShape<'a>),
			DictionaryId {
				#[serde(rename = "data", serialize_with = "dictionary_array::serialize")]
				container: &'a FixedSizeBinaryArray,
				dictionary_id: Option<DictionaryId>,
			},
			Option {
				inner: Bare<'a>,
				#[serde(serialize_with = "bitmap::serialize")]
				bitvec: &'a BooleanBuffer,
			},
			Digest {
				#[serde(serialize_with = "digest_array::serialize")]
				container: &'a LargeBinaryArray,
				inner: &'a ValueType,
				accuracy: u32,
			},
		}
		#[derive(Serialize)]
		struct AnyShape<'a> {
			#[serde(serialize_with = "any_array::serialize")]
			data: &'a LargeBinaryArray,
			declared_type: &'a Option<ValueType>,
		}
		struct Bare<'a>(&'a ColumnBuffer);
		impl Serialize for Bare<'_> {
			fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
				let helper = match self.0 {
					ColumnBuffer::Bool(c) => Helper::Bool(c),
					ColumnBuffer::Float4(c) => Helper::Float4(c),
					ColumnBuffer::Float8(c) => Helper::Float8(c),
					ColumnBuffer::Int1(c) => Helper::Int1(c),
					ColumnBuffer::Int2(c) => Helper::Int2(c),
					ColumnBuffer::Int4(c) => Helper::Int4(c),
					ColumnBuffer::Int8(c) => Helper::Int8(c),
					ColumnBuffer::Int16(c) => Helper::Int16(c),
					ColumnBuffer::Uint1(c) => Helper::Uint1(c),
					ColumnBuffer::Uint2(c) => Helper::Uint2(c),
					ColumnBuffer::Uint4(c) => Helper::Uint4(c),
					ColumnBuffer::Uint8(c) => Helper::Uint8(c),
					ColumnBuffer::Uint16(c) => Helper::Uint16(c),
					ColumnBuffer::Utf8 {
						container,
						max_bytes,
					} => Helper::Utf8 {
						container,
						max_bytes: *max_bytes,
					},
					ColumnBuffer::Date(c) => Helper::Date(c),
					ColumnBuffer::DateTime(c) => Helper::DateTime(c),
					ColumnBuffer::Time(c) => Helper::Time(c),
					ColumnBuffer::Duration(c) => Helper::Duration(c),
					ColumnBuffer::IdentityId(c) => Helper::IdentityId(c),
					ColumnBuffer::Uuid4(c) => Helper::Uuid4(c),
					ColumnBuffer::Uuid7(c) => Helper::Uuid7(c),
					ColumnBuffer::Blob {
						container,
						max_bytes,
					} => Helper::Blob {
						container,
						max_bytes: *max_bytes,
					},
					ColumnBuffer::Int {
						container,
						max_bytes,
					} => Helper::Int {
						container,
						max_bytes: *max_bytes,
					},
					ColumnBuffer::Uint {
						container,
						max_bytes,
					} => Helper::Uint {
						container,
						max_bytes: *max_bytes,
					},
					ColumnBuffer::Decimal {
						container,
						precision,
						scale,
					} => Helper::Decimal {
						container,
						precision: *precision,
						scale: *scale,
					},
					ColumnBuffer::Any {
						container,
						declared_type,
					} => Helper::Any(AnyShape {
						data: container,
						declared_type,
					}),
					ColumnBuffer::DictionaryId {
						container,
						dictionary_id,
					} => Helper::DictionaryId {
						container,
						dictionary_id: *dictionary_id,
					},
					ColumnBuffer::Digest {
						container,
						inner,
						accuracy,
					} => Helper::Digest {
						container,
						inner,
						accuracy: *accuracy,
					},
				};
				helper.serialize(serializer)
			}
		}
		match self.nulls() {
			Some(nulls) => Helper::Option {
				inner: Bare(self),
				bitvec: nulls.inner(),
			}
			.serialize(serializer),
			None => Bare(self).serialize(serializer),
		}
	}
}

impl<'de> Deserialize<'de> for ColumnBuffer {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		enum Helper {
			Bool(#[serde(deserialize_with = "bool_array::deserialize")] BooleanArray),
			Float4(#[serde(deserialize_with = "primitive::deserialize")] Float32Array),
			Float8(#[serde(deserialize_with = "primitive::deserialize")] Float64Array),
			Int1(#[serde(deserialize_with = "primitive::deserialize")] Int8Array),
			Int2(#[serde(deserialize_with = "primitive::deserialize")] Int16Array),
			Int4(#[serde(deserialize_with = "primitive::deserialize")] Int32Array),
			Int8(#[serde(deserialize_with = "primitive::deserialize")] Int64Array),
			Int16(#[serde(deserialize_with = "deserialize_int16s")] Decimal128Array),
			Uint1(#[serde(deserialize_with = "primitive::deserialize")] UInt8Array),
			Uint2(#[serde(deserialize_with = "primitive::deserialize")] UInt16Array),
			Uint4(#[serde(deserialize_with = "primitive::deserialize")] UInt32Array),
			Uint8(#[serde(deserialize_with = "primitive::deserialize")] UInt64Array),
			Uint16(#[serde(deserialize_with = "deserialize_uint16s")] Decimal256Array),
			Utf8 {
				#[serde(deserialize_with = "varlen_array::deserialize_utf8")]
				container: LargeStringArray,
				max_bytes: MaxBytes,
			},
			Date(#[serde(deserialize_with = "deserialize_dates")] Date32Array),
			DateTime(#[serde(deserialize_with = "deserialize_datetimes")] UInt64Array),
			Time(#[serde(deserialize_with = "deserialize_times")] Time64NanosecondArray),
			Duration(#[serde(deserialize_with = "deserialize_durations")] IntervalMonthDayNanoArray),
			IdentityId(#[serde(deserialize_with = "deserialize_identity_ids")] FixedSizeBinaryArray),
			Uuid4(#[serde(deserialize_with = "deserialize_uuid4s")] FixedSizeBinaryArray),
			Uuid7(#[serde(deserialize_with = "deserialize_uuid7s")] FixedSizeBinaryArray),
			Blob {
				#[serde(deserialize_with = "varlen_array::deserialize_blob")]
				container: LargeBinaryArray,
				max_bytes: MaxBytes,
			},
			Int {
				#[serde(deserialize_with = "deserialize_ints")]
				container: LargeBinaryArray,
				max_bytes: MaxBytes,
			},
			Uint {
				#[serde(deserialize_with = "deserialize_uints")]
				container: LargeBinaryArray,
				max_bytes: MaxBytes,
			},
			Decimal {
				#[serde(deserialize_with = "deserialize_decimals")]
				container: LargeBinaryArray,
				precision: Precision,
				scale: Scale,
			},
			Any(AnyShape),
			DictionaryId {
				#[serde(rename = "data", deserialize_with = "dictionary_array::deserialize")]
				container: FixedSizeBinaryArray,
				dictionary_id: Option<DictionaryId>,
			},
			Option {
				inner: Box<ColumnBuffer>,
				#[serde(deserialize_with = "bitmap::deserialize")]
				bitvec: BooleanBuffer,
			},
			Digest {
				#[serde(deserialize_with = "digest_array::deserialize")]
				container: LargeBinaryArray,
				inner: ValueType,
				accuracy: u32,
			},
		}
		#[derive(Deserialize)]
		struct AnyShape {
			#[serde(deserialize_with = "any_array::deserialize")]
			data: LargeBinaryArray,
			#[serde(default)]
			declared_type: Option<ValueType>,
		}
		let helper = Helper::deserialize(deserializer)?;
		Ok(match helper {
			Helper::Bool(c) => ColumnBuffer::Bool(c),
			Helper::Float4(c) => ColumnBuffer::Float4(c),
			Helper::Float8(c) => ColumnBuffer::Float8(c),
			Helper::Int1(c) => ColumnBuffer::Int1(c),
			Helper::Int2(c) => ColumnBuffer::Int2(c),
			Helper::Int4(c) => ColumnBuffer::Int4(c),
			Helper::Int8(c) => ColumnBuffer::Int8(c),
			Helper::Int16(c) => ColumnBuffer::Int16(c),
			Helper::Uint1(c) => ColumnBuffer::Uint1(c),
			Helper::Uint2(c) => ColumnBuffer::Uint2(c),
			Helper::Uint4(c) => ColumnBuffer::Uint4(c),
			Helper::Uint8(c) => ColumnBuffer::Uint8(c),
			Helper::Uint16(c) => ColumnBuffer::Uint16(c),
			Helper::Utf8 {
				container,
				max_bytes,
			} => ColumnBuffer::Utf8 {
				container,
				max_bytes,
			},
			Helper::Date(c) => ColumnBuffer::Date(c),
			Helper::DateTime(c) => ColumnBuffer::DateTime(c),
			Helper::Time(c) => ColumnBuffer::Time(c),
			Helper::Duration(c) => ColumnBuffer::Duration(c),
			Helper::IdentityId(c) => ColumnBuffer::IdentityId(c),
			Helper::Uuid4(c) => ColumnBuffer::Uuid4(c),
			Helper::Uuid7(c) => ColumnBuffer::Uuid7(c),
			Helper::Blob {
				container,
				max_bytes,
			} => ColumnBuffer::Blob {
				container,
				max_bytes,
			},
			Helper::Int {
				container,
				max_bytes,
			} => ColumnBuffer::Int {
				container,
				max_bytes,
			},
			Helper::Uint {
				container,
				max_bytes,
			} => ColumnBuffer::Uint {
				container,
				max_bytes,
			},
			Helper::Decimal {
				container,
				precision,
				scale,
			} => ColumnBuffer::Decimal {
				container,
				precision,
				scale,
			},
			Helper::Any(AnyShape {
				data,
				declared_type,
			}) => ColumnBuffer::Any {
				container: data,
				declared_type,
			},
			Helper::DictionaryId {
				container,
				dictionary_id,
			} => ColumnBuffer::DictionaryId {
				container,
				dictionary_id,
			},
			Helper::Option {
				inner,
				bitvec,
			} => {
				if bitvec.len() != inner.len() {
					return Err(DeError::custom(format!(
						"Option column bitvec of {} bits does not match its {} rows",
						bitvec.len(),
						inner.len()
					)));
				}
				inner.with_nulls(NullBuffer::new(bitvec))
			}
			Helper::Digest {
				container,
				inner,
				accuracy,
			} => ColumnBuffer::Digest {
				container,
				inner,
				accuracy,
			},
		})
	}
}

macro_rules! with_container {
	($self:expr, |$a:ident| $native:expr) => {
		with_container!(
			$self,
			|$a| $native,
			|_temporal| unreachable!(
				"with_container! must not be called on a temporal variant without a temporal body"
			),
			|_fixed| unreachable!("with_container! must not be called on an id variant without an id body"),
			|_varlen| unreachable!(
				"with_container! must not be called on a varlen variant without a varlen body"
			)
		)
	};
	($self:expr, |$a:ident| $native:expr, |$t:ident| $temporal:expr, |$u:ident| $fixed:expr, |$v:ident| $varlen:expr) => {
		match $self {
			ColumnBuffer::Float4($a) => $native,
			ColumnBuffer::Float8($a) => $native,
			ColumnBuffer::Int1($a) => $native,
			ColumnBuffer::Int2($a) => $native,
			ColumnBuffer::Int4($a) => $native,
			ColumnBuffer::Int8($a) => $native,
			ColumnBuffer::Uint1($a) => $native,
			ColumnBuffer::Uint2($a) => $native,
			ColumnBuffer::Uint4($a) => $native,
			ColumnBuffer::Uint8($a) => $native,
			ColumnBuffer::Int16($a) => $native,
			ColumnBuffer::Date($t) => $temporal,
			ColumnBuffer::DateTime($t) => $temporal,
			ColumnBuffer::Time($t) => $temporal,
			ColumnBuffer::Duration($t) => $temporal,
			ColumnBuffer::IdentityId($u) => $fixed,
			ColumnBuffer::Uuid4($u) => $fixed,
			ColumnBuffer::Uuid7($u) => $fixed,
			ColumnBuffer::Utf8 {
				container: $v,
				..
			} => $varlen,
			ColumnBuffer::Blob {
				container: $v,
				..
			} => $varlen,
			ColumnBuffer::Int {
				container: $v,
				..
			} => $varlen,
			ColumnBuffer::Uint {
				container: $v,
				..
			} => $varlen,
			ColumnBuffer::Decimal {
				container: $v,
				..
			} => $varlen,
			ColumnBuffer::Any {
				container: $v,
				..
			} => $varlen,
			ColumnBuffer::Digest {
				container: $v,
				..
			} => $varlen,
			ColumnBuffer::Bool(_) => {
				unreachable!(
					"with_container! must not be called on Bool variant directly; handle it explicitly"
				)
			}
			ColumnBuffer::Uint16(_) => {
				unreachable!(
					"with_container! must not be called on Uint16 variant directly; handle it explicitly"
				)
			}
			ColumnBuffer::DictionaryId {
				..
			} => {
				unreachable!(
					"with_container! must not be called on DictionaryId variant directly; handle it explicitly"
				)
			}
		}
	};
}

pub(crate) use with_container;

impl ColumnBuffer {
	pub fn nulls(&self) -> Option<&NullBuffer> {
		match self {
			ColumnBuffer::Bool(a) => a.nulls(),
			ColumnBuffer::Uint16(a) => a.nulls(),
			ColumnBuffer::DictionaryId {
				container,
				..
			} => container.nulls(),
			_ => with_container!(self, |a| a.nulls(), |t| t.nulls(), |u| u.nulls(), |v| v.nulls()),
		}
	}

	pub fn split_nulls(self) -> (ColumnBuffer, Option<NullBuffer>) {
		match self.nulls().cloned() {
			Some(nulls) => (self.replace_nulls(None), Some(nulls)),
			None => (self, None),
		}
	}

	pub fn with_nulls(self, nulls: NullBuffer) -> ColumnBuffer {
		let len = self.len();
		assert_eq!(
			nulls.len(),
			len,
			"validity of {} rows does not match a column of {len} rows",
			nulls.len()
		);
		let nulls = match self.nulls() {
			Some(existing) => bitmap::and_nulls(existing, &nulls),
			None => nulls,
		};
		self.replace_nulls(Some(nulls))
	}

	pub(crate) fn replace_nulls(self, nulls: Option<NullBuffer>) -> ColumnBuffer {
		match self {
			ColumnBuffer::Bool(a) => ColumnBuffer::Bool(bool_array::attach_nulls(a, nulls)),
			ColumnBuffer::Float4(a) => ColumnBuffer::Float4(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Float8(a) => ColumnBuffer::Float8(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Int1(a) => ColumnBuffer::Int1(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Int2(a) => ColumnBuffer::Int2(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Int4(a) => ColumnBuffer::Int4(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Int8(a) => ColumnBuffer::Int8(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Int16(a) => ColumnBuffer::Int16(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Uint1(a) => ColumnBuffer::Uint1(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Uint2(a) => ColumnBuffer::Uint2(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Uint4(a) => ColumnBuffer::Uint4(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Uint8(a) => ColumnBuffer::Uint8(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Uint16(a) => ColumnBuffer::Uint16(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Utf8 {
				container,
				max_bytes,
			} => ColumnBuffer::Utf8 {
				container: varlen_array::attach_nulls(container, nulls),
				max_bytes,
			},
			ColumnBuffer::Date(a) => ColumnBuffer::Date(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::DateTime(a) => ColumnBuffer::DateTime(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Time(a) => ColumnBuffer::Time(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::Duration(a) => ColumnBuffer::Duration(primitive::attach_nulls(a, nulls)),
			ColumnBuffer::IdentityId(a) => ColumnBuffer::IdentityId(uuid_array::attach_nulls(a, nulls)),
			ColumnBuffer::Uuid4(a) => ColumnBuffer::Uuid4(uuid_array::attach_nulls(a, nulls)),
			ColumnBuffer::Uuid7(a) => ColumnBuffer::Uuid7(uuid_array::attach_nulls(a, nulls)),
			ColumnBuffer::Blob {
				container,
				max_bytes,
			} => ColumnBuffer::Blob {
				container: varlen_array::attach_nulls(container, nulls),
				max_bytes,
			},
			ColumnBuffer::Int {
				container,
				max_bytes,
			} => ColumnBuffer::Int {
				container: varlen_array::attach_nulls(container, nulls),
				max_bytes,
			},
			ColumnBuffer::Uint {
				container,
				max_bytes,
			} => ColumnBuffer::Uint {
				container: varlen_array::attach_nulls(container, nulls),
				max_bytes,
			},
			ColumnBuffer::Decimal {
				container,
				precision,
				scale,
			} => ColumnBuffer::Decimal {
				container: varlen_array::attach_nulls(container, nulls),
				precision,
				scale,
			},
			ColumnBuffer::Any {
				container,
				declared_type,
			} => ColumnBuffer::Any {
				container: varlen_array::attach_nulls(container, nulls),
				declared_type,
			},
			ColumnBuffer::DictionaryId {
				container,
				dictionary_id,
			} => ColumnBuffer::DictionaryId {
				container: uuid_array::attach_nulls(container, nulls),
				dictionary_id,
			},
			ColumnBuffer::Digest {
				container,
				inner,
				accuracy,
			} => ColumnBuffer::Digest {
				container: varlen_array::attach_nulls(container, nulls),
				inner,
				accuracy,
			},
		}
	}

	pub(crate) fn none_at(&self, index: usize) -> bool {
		self.nulls().is_some_and(|nulls| !(index < nulls.len() && nulls.is_valid(index)))
	}

	pub(crate) fn base_type(&self) -> ValueType {
		match self.get_type() {
			ValueType::Option(base) if self.nulls().is_some() => *base,
			other => other,
		}
	}

	pub fn get_type(&self) -> ValueType {
		let base = match self {
			ColumnBuffer::Bool(_) => ValueType::Boolean,
			ColumnBuffer::Float4(_) => ValueType::Float4,
			ColumnBuffer::Float8(_) => ValueType::Float8,
			ColumnBuffer::Int1(_) => ValueType::Int1,
			ColumnBuffer::Int2(_) => ValueType::Int2,
			ColumnBuffer::Int4(_) => ValueType::Int4,
			ColumnBuffer::Int8(_) => ValueType::Int8,
			ColumnBuffer::Int16(_) => ValueType::Int16,
			ColumnBuffer::Uint1(_) => ValueType::Uint1,
			ColumnBuffer::Uint2(_) => ValueType::Uint2,
			ColumnBuffer::Uint4(_) => ValueType::Uint4,
			ColumnBuffer::Uint8(_) => ValueType::Uint8,
			ColumnBuffer::Uint16(_) => ValueType::Uint16,
			ColumnBuffer::Utf8 {
				..
			} => ValueType::Utf8,
			ColumnBuffer::Date(_) => ValueType::Date,
			ColumnBuffer::DateTime(_) => ValueType::DateTime,
			ColumnBuffer::Time(_) => ValueType::Time,
			ColumnBuffer::Duration(_) => ValueType::Duration,
			ColumnBuffer::IdentityId(_) => ValueType::IdentityId,
			ColumnBuffer::Uuid4(_) => ValueType::Uuid4,
			ColumnBuffer::Uuid7(_) => ValueType::Uuid7,
			ColumnBuffer::Blob {
				..
			} => ValueType::Blob,
			ColumnBuffer::Int {
				..
			} => ValueType::Int,
			ColumnBuffer::Uint {
				..
			} => ValueType::Uint,
			ColumnBuffer::Decimal {
				..
			} => ValueType::Decimal,
			ColumnBuffer::DictionaryId {
				..
			} => ValueType::DictionaryId,
			ColumnBuffer::Any {
				declared_type,
				..
			} => declared_type.clone().unwrap_or(ValueType::Any),
			ColumnBuffer::Digest {
				inner,
				accuracy,
				..
			} => ValueType::Digest {
				inner: Box::new(inner.clone()),
				accuracy: *accuracy,
			},
		};
		match self.nulls() {
			Some(_) => ValueType::Option(Box::new(base)),
			None => base,
		}
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		if self.none_at(idx) {
			return false;
		}
		match self {
			ColumnBuffer::Bool(c) => idx < c.len(),
			ColumnBuffer::Float4(c) => idx < c.len(),
			ColumnBuffer::Float8(c) => idx < c.len(),
			ColumnBuffer::Int1(c) => idx < c.len(),
			ColumnBuffer::Int2(c) => idx < c.len(),
			ColumnBuffer::Int4(c) => idx < c.len(),
			ColumnBuffer::Int8(c) => idx < c.len(),
			ColumnBuffer::Int16(c) => idx < c.len(),
			ColumnBuffer::Uint1(c) => idx < c.len(),
			ColumnBuffer::Uint2(c) => idx < c.len(),
			ColumnBuffer::Uint4(c) => idx < c.len(),
			ColumnBuffer::Uint8(c) => idx < c.len(),
			ColumnBuffer::Uint16(c) => idx < c.len(),
			ColumnBuffer::Utf8 {
				container: c,
				..
			} => idx < c.len(),
			ColumnBuffer::Date(c) => idx < c.len(),
			ColumnBuffer::DateTime(c) => idx < c.len(),
			ColumnBuffer::Time(c) => idx < c.len(),
			ColumnBuffer::Duration(c) => idx < c.len(),
			ColumnBuffer::IdentityId(c) => idx < c.len(),
			ColumnBuffer::Uuid4(c) => idx < c.len(),
			ColumnBuffer::Uuid7(c) => idx < c.len(),
			ColumnBuffer::Blob {
				container: c,
				..
			} => idx < c.len(),
			ColumnBuffer::Int {
				container: c,
				..
			} => idx < c.len(),
			ColumnBuffer::Uint {
				container: c,
				..
			} => idx < c.len(),
			ColumnBuffer::Decimal {
				container: c,
				..
			} => idx < c.len(),
			ColumnBuffer::DictionaryId {
				container: c,
				..
			} => idx < c.len(),
			ColumnBuffer::Any {
				container: c,
				..
			} => idx < c.len(),
			ColumnBuffer::Digest {
				container,
				..
			} => digest_array::is_defined(container, idx),
		}
	}

	pub fn is_bool(&self) -> bool {
		self.get_type() == ValueType::Boolean
	}

	pub fn is_float(&self) -> bool {
		self.get_type() == ValueType::Float4 || self.get_type() == ValueType::Float8
	}

	pub fn is_utf8(&self) -> bool {
		self.get_type() == ValueType::Utf8
	}

	pub fn is_number(&self) -> bool {
		matches!(
			self.get_type(),
			ValueType::Float4
				| ValueType::Float8 | ValueType::Int1
				| ValueType::Int2 | ValueType::Int4
				| ValueType::Int8 | ValueType::Int16
				| ValueType::Uint1 | ValueType::Uint2
				| ValueType::Uint4 | ValueType::Uint8
				| ValueType::Uint16 | ValueType::Int
				| ValueType::Uint | ValueType::Decimal
		)
	}

	pub fn is_text(&self) -> bool {
		self.get_type() == ValueType::Utf8
	}

	pub fn is_temporal(&self) -> bool {
		matches!(self.get_type(), ValueType::Date | ValueType::DateTime | ValueType::Time | ValueType::Duration)
	}

	pub fn is_uuid(&self) -> bool {
		matches!(self.get_type(), ValueType::Uuid4 | ValueType::Uuid7)
	}
}

impl ColumnBuffer {
	pub fn none_count(&self) -> usize {
		self.nulls().map_or(0, |nulls| nulls.null_count())
	}
}

impl ColumnBuffer {
	pub fn len(&self) -> usize {
		match self {
			ColumnBuffer::Bool(a) => a.len(),
			ColumnBuffer::Uint16(a) => a.len(),
			ColumnBuffer::DictionaryId {
				container,
				..
			} => container.len(),
			_ => with_container!(self, |a| a.len(), |t| t.len(), |u| u.len(), |v| v.len()),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn capacity(&self) -> usize {
		match self {
			ColumnBuffer::Bool(a) => bool_array::capacity(a),
			ColumnBuffer::Uint16(a) => primitive::capacity(a),
			ColumnBuffer::DictionaryId {
				container,
				..
			} => dictionary_array::capacity(container),
			_ => with_container!(
				self,
				|a| primitive::capacity(a),
				|t| primitive::capacity(t),
				|u| uuid_array::capacity(u),
				|v| varlen_array::capacity(v)
			),
		}
	}

	pub fn heap_size(&self) -> usize {
		let nulls = self.nulls().map_or(0, |nulls| nulls.len().div_ceil(8));
		nulls + match self {
			ColumnBuffer::Digest {
				container,
				..
			} => {
				varlen_array::heap_size(container)
					+ digest_array::iter(container)
						.flatten()
						.map(|digest| size_of::<Digest>() + digest.heap_size())
						.sum::<usize>()
			}
			ColumnBuffer::Bool(a) => bool_array::heap_size(a),
			ColumnBuffer::Uint16(a) => primitive::heap_size(a),
			ColumnBuffer::DictionaryId {
				container,
				..
			} => dictionary_array::heap_size(container),
			_ => with_container!(
				self,
				|a| primitive::heap_size(a),
				|t| primitive::heap_size(t),
				|u| uuid_array::heap_size(u),
				|v| varlen_array::heap_size(v)
			),
		}
	}

	pub(crate) fn freeze(&mut self) {
		match self {
			ColumnBuffer::Bool(_)
			| ColumnBuffer::Uint16(_)
			| ColumnBuffer::DictionaryId {
				..
			} => {}
			_ => with_container!(self, |_a| {}, |_t| {}, |_u| {}, |_v| {}),
		}
	}

	pub fn as_string(&self, index: usize) -> String {
		if self.none_at(index) {
			return "none".to_string();
		}
		match self {
			ColumnBuffer::Bool(a) => bool_array::as_string(a, index),
			ColumnBuffer::Date(a) => temporal_array::as_string(dates(a), index),
			ColumnBuffer::DateTime(a) => temporal_array::as_string(datetimes(a), index),
			ColumnBuffer::Time(a) => temporal_array::as_string(times(a), index),
			ColumnBuffer::Duration(a) => temporal_array::as_string(durations(a), index),
			ColumnBuffer::IdentityId(a) => uuid_array::as_string(identity_ids(a), index),
			ColumnBuffer::Uuid4(a) => uuid_array::as_string(uuid4s(a), index),
			ColumnBuffer::Uuid7(a) => uuid_array::as_string(uuid7s(a), index),
			ColumnBuffer::Utf8 {
				container,
				..
			} => varlen_array::utf8_as_string(container, index),
			ColumnBuffer::Blob {
				container,
				..
			} => varlen_array::blob_as_string(container, index),
			ColumnBuffer::Uint16(a) => uint16_as_string(a, index),
			ColumnBuffer::DictionaryId {
				container,
				..
			} => dictionary_array::as_string(container, index),
			ColumnBuffer::Int {
				container,
				..
			} => int_as_string(container, index),
			ColumnBuffer::Uint {
				container,
				..
			} => uint_as_string(container, index),
			ColumnBuffer::Decimal {
				container,
				..
			} => decimal_as_string(container, index),
			ColumnBuffer::Any {
				container,
				..
			} => any_array::as_string(container, index),
			ColumnBuffer::Digest {
				container,
				..
			} => digest_array::as_string(container, index),
			_ => with_container!(self, |a| primitive::as_string(a, index)),
		}
	}
}

impl ColumnBuffer {
	pub(crate) fn with_capacity(target: ValueType, capacity: usize) -> Self {
		match target {
			ValueType::Boolean => Self::bool_with_capacity(capacity),
			ValueType::Float4 => Self::float4_with_capacity(capacity),
			ValueType::Float8 => Self::float8_with_capacity(capacity),
			ValueType::Int1 => Self::int1_with_capacity(capacity),
			ValueType::Int2 => Self::int2_with_capacity(capacity),
			ValueType::Int4 => Self::int4_with_capacity(capacity),
			ValueType::Int8 => Self::int8_with_capacity(capacity),
			ValueType::Int16 => Self::int16_with_capacity(capacity),
			ValueType::Uint1 => Self::uint1_with_capacity(capacity),
			ValueType::Uint2 => Self::uint2_with_capacity(capacity),
			ValueType::Uint4 => Self::uint4_with_capacity(capacity),
			ValueType::Uint8 => Self::uint8_with_capacity(capacity),
			ValueType::Uint16 => Self::uint16_with_capacity(capacity),
			ValueType::Utf8 => Self::utf8_with_capacity(capacity),
			ValueType::Date => Self::date_with_capacity(capacity),
			ValueType::DateTime => Self::datetime_with_capacity(capacity),
			ValueType::Time => Self::time_with_capacity(capacity),
			ValueType::Duration => Self::duration_with_capacity(capacity),
			ValueType::IdentityId => Self::identity_id_with_capacity(capacity),
			ValueType::Uuid4 => Self::uuid4_with_capacity(capacity),
			ValueType::Uuid7 => Self::uuid7_with_capacity(capacity),
			ValueType::Blob => Self::blob_with_capacity(capacity),
			ValueType::Int => Self::int_with_capacity(capacity),
			ValueType::Uint => Self::uint_with_capacity(capacity),
			ValueType::Decimal => Self::decimal_with_capacity(capacity),
			ValueType::DictionaryId => Self::dictionary_id_with_capacity(capacity),
			ValueType::Option(inner) => ColumnBuffer::with_capacity(*inner, capacity)
				.replace_nulls(Some(NullBuffer::new_valid(0))),
			ValueType::Any | ValueType::Tuple(_) => Self::any_with_capacity(capacity),
			declared @ (ValueType::List(_) | ValueType::Record(_)) => {
				Self::any_with_capacity_typed(capacity, declared)
			}
			ValueType::Digest {
				inner,
				accuracy,
			} => ColumnBuffer::Digest {
				container: LargeBinaryBuilder::with_capacity(capacity, 0).finish(),
				inner: *inner,
				accuracy,
			},
		}
	}

	pub(crate) fn empty_like(&self, capacity: usize) -> Self {
		let mut buffer = Self::with_capacity(self.get_type(), capacity);
		match (self, &mut buffer) {
			(
				ColumnBuffer::Utf8 {
					max_bytes: src,
					..
				},
				ColumnBuffer::Utf8 {
					max_bytes: dst,
					..
				},
			)
			| (
				ColumnBuffer::Blob {
					max_bytes: src,
					..
				},
				ColumnBuffer::Blob {
					max_bytes: dst,
					..
				},
			)
			| (
				ColumnBuffer::Int {
					max_bytes: src,
					..
				},
				ColumnBuffer::Int {
					max_bytes: dst,
					..
				},
			)
			| (
				ColumnBuffer::Uint {
					max_bytes: src,
					..
				},
				ColumnBuffer::Uint {
					max_bytes: dst,
					..
				},
			) => *dst = *src,
			(
				ColumnBuffer::Decimal {
					precision: src_precision,
					scale: src_scale,
					..
				},
				ColumnBuffer::Decimal {
					precision: dst_precision,
					scale: dst_scale,
					..
				},
			) => {
				*dst_precision = *src_precision;
				*dst_scale = *src_scale;
			}
			(
				ColumnBuffer::DictionaryId {
					dictionary_id: Some(src),
					..
				},
				ColumnBuffer::DictionaryId {
					dictionary_id: dst,
					..
				},
			) => *dst = Some(*src),
			_ => {}
		}
		buffer
	}

	pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Value> + 'a> {
		Box::new((0..self.len()).map(move |i| self.get_value(i)))
	}
}
