// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod extend;
pub mod factory;
pub mod filter;
pub mod from;
pub mod get;
pub mod reorder;
pub mod slice;
pub mod take;

use reifydb_type::{
	util::bitvec::BitVec,
	value::{
		Value,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		container::{
			any::AnyContainer, blob::BlobContainer, bool::BoolContainer, identity_id::IdentityIdContainer,
			number::NumberContainer, temporal::TemporalContainer, undefined::UndefinedContainer,
			utf8::Utf8Container, uuid::UuidContainer,
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		int::Int,
		time::Time,
		r#type::Type,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ColumnData {
	Bool(BoolContainer),
	Float4(NumberContainer<f32>),
	Float8(NumberContainer<f64>),
	Int1(NumberContainer<i8>),
	Int2(NumberContainer<i16>),
	Int4(NumberContainer<i32>),
	Int8(NumberContainer<i64>),
	Int16(NumberContainer<i128>),
	Uint1(NumberContainer<u8>),
	Uint2(NumberContainer<u16>),
	Uint4(NumberContainer<u32>),
	Uint8(NumberContainer<u64>),
	Uint16(NumberContainer<u128>),
	Utf8 {
		container: Utf8Container,
		max_bytes: MaxBytes,
	},
	Date(TemporalContainer<Date>),
	DateTime(TemporalContainer<DateTime>),
	Time(TemporalContainer<Time>),
	Duration(TemporalContainer<Duration>),
	IdentityId(IdentityIdContainer),
	Uuid4(UuidContainer<Uuid4>),
	Uuid7(UuidContainer<Uuid7>),
	Blob {
		container: BlobContainer,
		max_bytes: MaxBytes,
	},
	Int {
		container: NumberContainer<Int>,
		max_bytes: MaxBytes,
	},
	Uint {
		container: NumberContainer<Uint>,
		max_bytes: MaxBytes,
	},
	Decimal {
		container: NumberContainer<Decimal>,
		precision: Precision,
		scale: Scale,
	},
	// Container for Any type (heterogeneous values)
	Any(AnyContainer),
	// special case: all undefined
	Undefined(UndefinedContainer),
}

impl ColumnData {
	pub fn get_type(&self) -> Type {
		match self {
			ColumnData::Bool(_) => Type::Boolean,
			ColumnData::Float4(_) => Type::Float4,
			ColumnData::Float8(_) => Type::Float8,
			ColumnData::Int1(_) => Type::Int1,
			ColumnData::Int2(_) => Type::Int2,
			ColumnData::Int4(_) => Type::Int4,
			ColumnData::Int8(_) => Type::Int8,
			ColumnData::Int16(_) => Type::Int16,
			ColumnData::Uint1(_) => Type::Uint1,
			ColumnData::Uint2(_) => Type::Uint2,
			ColumnData::Uint4(_) => Type::Uint4,
			ColumnData::Uint8(_) => Type::Uint8,
			ColumnData::Uint16(_) => Type::Uint16,
			ColumnData::Utf8 {
				..
			} => Type::Utf8,
			ColumnData::Date(_) => Type::Date,
			ColumnData::DateTime(_) => Type::DateTime,
			ColumnData::Time(_) => Type::Time,
			ColumnData::Duration(_) => Type::Duration,
			ColumnData::IdentityId(_) => Type::IdentityId,
			ColumnData::Uuid4(_) => Type::Uuid4,
			ColumnData::Uuid7(_) => Type::Uuid7,
			ColumnData::Blob {
				..
			} => Type::Blob,
			ColumnData::Int {
				..
			} => Type::Int,
			ColumnData::Uint {
				..
			} => Type::Uint,
			ColumnData::Decimal {
				..
			} => Type::Decimal,
			ColumnData::Any(_) => Type::Any,
			ColumnData::Undefined(_) => Type::Undefined,
		}
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		match self {
			ColumnData::Bool(container) => container.is_defined(idx),
			ColumnData::Float4(container) => container.is_defined(idx),
			ColumnData::Float8(container) => container.is_defined(idx),
			ColumnData::Int1(container) => container.is_defined(idx),
			ColumnData::Int2(container) => container.is_defined(idx),
			ColumnData::Int4(container) => container.is_defined(idx),
			ColumnData::Int8(container) => container.is_defined(idx),
			ColumnData::Int16(container) => container.is_defined(idx),
			ColumnData::Uint1(container) => container.is_defined(idx),
			ColumnData::Uint2(container) => container.is_defined(idx),
			ColumnData::Uint4(container) => container.is_defined(idx),
			ColumnData::Uint8(container) => container.is_defined(idx),
			ColumnData::Uint16(container) => container.is_defined(idx),
			ColumnData::Utf8 {
				container,
				..
			} => container.is_defined(idx),
			ColumnData::Date(container) => container.is_defined(idx),
			ColumnData::DateTime(container) => container.is_defined(idx),
			ColumnData::Time(container) => container.is_defined(idx),
			ColumnData::Duration(container) => container.is_defined(idx),
			ColumnData::IdentityId(container) => container.get(idx).is_some(),
			ColumnData::Uuid4(container) => container.is_defined(idx),
			ColumnData::Uuid7(container) => container.is_defined(idx),
			ColumnData::Blob {
				container,
				..
			} => container.is_defined(idx),
			ColumnData::Int {
				container,
				..
			} => container.is_defined(idx),
			ColumnData::Uint {
				container,
				..
			} => container.is_defined(idx),
			ColumnData::Decimal {
				container,
				..
			} => container.is_defined(idx),
			ColumnData::Any(container) => container.is_defined(idx),
			ColumnData::Undefined(_) => false,
		}
	}

	pub fn is_bool(&self) -> bool {
		self.get_type() == Type::Boolean
	}

	pub fn is_float(&self) -> bool {
		self.get_type() == Type::Float4 || self.get_type() == Type::Float8
	}

	pub fn is_utf8(&self) -> bool {
		self.get_type() == Type::Utf8
	}

	pub fn is_number(&self) -> bool {
		matches!(
			self.get_type(),
			Type::Float4
				| Type::Float8 | Type::Int1 | Type::Int2
				| Type::Int4 | Type::Int8 | Type::Int16
				| Type::Uint1 | Type::Uint2 | Type::Uint4
				| Type::Uint8 | Type::Uint16 | Type::Int
				| Type::Uint | Type::Decimal { .. }
		)
	}

	pub fn is_text(&self) -> bool {
		self.get_type() == Type::Utf8
	}

	pub fn is_temporal(&self) -> bool {
		matches!(self.get_type(), Type::Date | Type::DateTime | Type::Time | Type::Duration)
	}

	pub fn is_uuid(&self) -> bool {
		matches!(self.get_type(), Type::Uuid4 | Type::Uuid7)
	}
}

impl ColumnData {
	pub fn bitvec(&self) -> &BitVec {
		match self {
			ColumnData::Bool(container) => container.bitvec(),
			ColumnData::Float4(container) => container.bitvec(),
			ColumnData::Float8(container) => container.bitvec(),
			ColumnData::Int1(container) => container.bitvec(),
			ColumnData::Int2(container) => container.bitvec(),
			ColumnData::Int4(container) => container.bitvec(),
			ColumnData::Int8(container) => container.bitvec(),
			ColumnData::Int16(container) => container.bitvec(),
			ColumnData::Uint1(container) => container.bitvec(),
			ColumnData::Uint2(container) => container.bitvec(),
			ColumnData::Uint4(container) => container.bitvec(),
			ColumnData::Uint8(container) => container.bitvec(),
			ColumnData::Uint16(container) => container.bitvec(),
			ColumnData::Utf8 {
				container,
				..
			} => container.bitvec(),
			ColumnData::Date(container) => container.bitvec(),
			ColumnData::DateTime(container) => container.bitvec(),
			ColumnData::Time(container) => container.bitvec(),
			ColumnData::Duration(container) => container.bitvec(),
			ColumnData::IdentityId(container) => container.bitvec(),
			ColumnData::Uuid4(container) => container.bitvec(),
			ColumnData::Uuid7(container) => container.bitvec(),
			ColumnData::Blob {
				container,
				..
			} => container.bitvec(),
			ColumnData::Int {
				container,
				..
			} => container.bitvec(),
			ColumnData::Uint {
				container,
				..
			} => container.bitvec(),
			ColumnData::Decimal {
				container,
				..
			} => container.bitvec(),
			ColumnData::Any(container) => container.bitvec(),
			ColumnData::Undefined(_) => unreachable!(),
		}
	}

	pub fn undefined_count(&self) -> usize {
		self.bitvec().count_zeros()
	}
}

impl ColumnData {
	pub fn with_capacity(target: Type, capacity: usize) -> Self {
		match target {
			Type::Boolean => Self::bool_with_capacity(capacity),
			Type::Float4 => Self::float4_with_capacity(capacity),
			Type::Float8 => Self::float8_with_capacity(capacity),
			Type::Int1 => Self::int1_with_capacity(capacity),
			Type::Int2 => Self::int2_with_capacity(capacity),
			Type::Int4 => Self::int4_with_capacity(capacity),
			Type::Int8 => Self::int8_with_capacity(capacity),
			Type::Int16 => Self::int16_with_capacity(capacity),
			Type::Uint1 => Self::uint1_with_capacity(capacity),
			Type::Uint2 => Self::uint2_with_capacity(capacity),
			Type::Uint4 => Self::uint4_with_capacity(capacity),
			Type::Uint8 => Self::uint8_with_capacity(capacity),
			Type::Uint16 => Self::uint16_with_capacity(capacity),
			Type::Utf8 => Self::utf8_with_capacity(capacity),
			Type::Date => Self::date_with_capacity(capacity),
			Type::DateTime => Self::datetime_with_capacity(capacity),
			Type::Time => Self::time_with_capacity(capacity),
			Type::Duration => Self::duration_with_capacity(capacity),
			Type::IdentityId => Self::identity_id_with_capacity(capacity),
			Type::Uuid4 => Self::uuid4_with_capacity(capacity),
			Type::Uuid7 => Self::uuid7_with_capacity(capacity),
			Type::Blob => Self::blob_with_capacity(capacity),
			Type::Int => Self::int_with_capacity(capacity),
			Type::Uint => Self::uint_with_capacity(capacity),
			Type::Decimal => Self::decimal_with_capacity(capacity),
			Type::Undefined => ColumnData::undefined(0),
			Type::Any => Self::any_with_capacity(capacity),
		}
	}

	pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Value> + 'a> {
		Box::new((0..self.len()).map(move |i| self.get_value(i)))
	}
}

impl ColumnData {
	pub fn len(&self) -> usize {
		match self {
			ColumnData::Bool(container) => container.len(),
			ColumnData::Float4(container) => container.len(),
			ColumnData::Float8(container) => container.len(),
			ColumnData::Int1(container) => container.len(),
			ColumnData::Int2(container) => container.len(),
			ColumnData::Int4(container) => container.len(),
			ColumnData::Int8(container) => container.len(),
			ColumnData::Int16(container) => container.len(),
			ColumnData::Uint1(container) => container.len(),
			ColumnData::Uint2(container) => container.len(),
			ColumnData::Uint4(container) => container.len(),
			ColumnData::Uint8(container) => container.len(),
			ColumnData::Uint16(container) => container.len(),
			ColumnData::Utf8 {
				container,
				..
			} => container.len(),
			ColumnData::Date(container) => container.len(),
			ColumnData::DateTime(container) => container.len(),
			ColumnData::Time(container) => container.len(),
			ColumnData::Duration(container) => container.len(),
			ColumnData::IdentityId(container) => container.len(),
			ColumnData::Uuid4(container) => container.len(),
			ColumnData::Uuid7(container) => container.len(),
			ColumnData::Blob {
				container,
				..
			} => container.len(),
			ColumnData::Int {
				container,
				..
			} => container.len(),
			ColumnData::Uint {
				container,
				..
			} => container.len(),
			ColumnData::Decimal {
				container,
				..
			} => container.len(),
			ColumnData::Any(container) => container.len(),
			ColumnData::Undefined(container) => container.len(),
		}
	}

	pub fn capacity(&self) -> usize {
		match self {
			ColumnData::Bool(container) => container.capacity(),
			ColumnData::Float4(container) => container.capacity(),
			ColumnData::Float8(container) => container.capacity(),
			ColumnData::Int1(container) => container.capacity(),
			ColumnData::Int2(container) => container.capacity(),
			ColumnData::Int4(container) => container.capacity(),
			ColumnData::Int8(container) => container.capacity(),
			ColumnData::Int16(container) => container.capacity(),
			ColumnData::Uint1(container) => container.capacity(),
			ColumnData::Uint2(container) => container.capacity(),
			ColumnData::Uint4(container) => container.capacity(),
			ColumnData::Uint8(container) => container.capacity(),
			ColumnData::Uint16(container) => container.capacity(),
			ColumnData::Utf8 {
				container,
				..
			} => container.capacity(),
			ColumnData::Date(container) => container.capacity(),
			ColumnData::DateTime(container) => container.capacity(),
			ColumnData::Time(container) => container.capacity(),
			ColumnData::Duration(container) => container.capacity(),
			ColumnData::IdentityId(container) => container.capacity(),
			ColumnData::Uuid4(container) => container.capacity(),
			ColumnData::Uuid7(container) => container.capacity(),
			ColumnData::Blob {
				container,
				..
			} => container.capacity(),
			ColumnData::Int {
				container,
				..
			} => container.capacity(),
			ColumnData::Uint {
				container,
				..
			} => container.capacity(),
			ColumnData::Decimal {
				container,
				..
			} => container.capacity(),
			ColumnData::Any(container) => container.capacity(),
			ColumnData::Undefined(container) => container.capacity(),
		}
	}

	pub fn as_string(&self, index: usize) -> String {
		match self {
			ColumnData::Bool(container) => container.as_string(index),
			ColumnData::Float4(container) => container.as_string(index),
			ColumnData::Float8(container) => container.as_string(index),
			ColumnData::Int1(container) => container.as_string(index),
			ColumnData::Int2(container) => container.as_string(index),
			ColumnData::Int4(container) => container.as_string(index),
			ColumnData::Int8(container) => container.as_string(index),
			ColumnData::Int16(container) => container.as_string(index),
			ColumnData::Uint1(container) => container.as_string(index),
			ColumnData::Uint2(container) => container.as_string(index),
			ColumnData::Uint4(container) => container.as_string(index),
			ColumnData::Uint8(container) => container.as_string(index),
			ColumnData::Uint16(container) => container.as_string(index),
			ColumnData::Utf8 {
				container,
				..
			} => container.as_string(index),
			ColumnData::Date(container) => container.as_string(index),
			ColumnData::DateTime(container) => container.as_string(index),
			ColumnData::Time(container) => container.as_string(index),
			ColumnData::Duration(container) => container.as_string(index),
			ColumnData::IdentityId(container) => container.as_string(index),
			ColumnData::Uuid4(container) => container.as_string(index),
			ColumnData::Uuid7(container) => container.as_string(index),
			ColumnData::Blob {
				container,
				..
			} => container.as_string(index),
			ColumnData::Int {
				container,
				..
			} => container.as_string(index),
			ColumnData::Uint {
				container,
				..
			} => container.as_string(index),
			ColumnData::Decimal {
				container,
				..
			} => container.as_string(index),
			ColumnData::Any(container) => container.as_string(index),
			ColumnData::Undefined(container) => container.as_string(index),
		}
	}
}
