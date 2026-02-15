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

use std::fmt;

use reifydb_type::{
	storage::{Cow, DataBitVec, Storage},
	value::{
		Value,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		container::{
			any::AnyContainer, blob::BlobContainer, bool::BoolContainer, dictionary::DictionaryContainer,
			identity_id::IdentityIdContainer, number::NumberContainer, temporal::TemporalContainer,
			undefined::UndefinedContainer, utf8::Utf8Container, uuid::UuidContainer,
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
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub enum ColumnData<S: Storage = Cow> {
	Bool(BoolContainer<S>),
	Float4(NumberContainer<f32, S>),
	Float8(NumberContainer<f64, S>),
	Int1(NumberContainer<i8, S>),
	Int2(NumberContainer<i16, S>),
	Int4(NumberContainer<i32, S>),
	Int8(NumberContainer<i64, S>),
	Int16(NumberContainer<i128, S>),
	Uint1(NumberContainer<u8, S>),
	Uint2(NumberContainer<u16, S>),
	Uint4(NumberContainer<u32, S>),
	Uint8(NumberContainer<u64, S>),
	Uint16(NumberContainer<u128, S>),
	Utf8 {
		container: Utf8Container<S>,
		max_bytes: MaxBytes,
	},
	Date(TemporalContainer<Date, S>),
	DateTime(TemporalContainer<DateTime, S>),
	Time(TemporalContainer<Time, S>),
	Duration(TemporalContainer<Duration, S>),
	IdentityId(IdentityIdContainer<S>),
	Uuid4(UuidContainer<Uuid4, S>),
	Uuid7(UuidContainer<Uuid7, S>),
	Blob {
		container: BlobContainer<S>,
		max_bytes: MaxBytes,
	},
	Int {
		container: NumberContainer<Int, S>,
		max_bytes: MaxBytes,
	},
	Uint {
		container: NumberContainer<Uint, S>,
		max_bytes: MaxBytes,
	},
	Decimal {
		container: NumberContainer<Decimal, S>,
		precision: Precision,
		scale: Scale,
	},
	// Container for Any type (heterogeneous values)
	Any(AnyContainer<S>),
	// Container for DictionaryEntryId values
	DictionaryId(DictionaryContainer<S>),
	// special case: all undefined
	Undefined(UndefinedContainer),
}

impl<S: Storage> Clone for ColumnData<S> {
	fn clone(&self) -> Self {
		match self {
			ColumnData::Bool(c) => ColumnData::Bool(c.clone()),
			ColumnData::Float4(c) => ColumnData::Float4(c.clone()),
			ColumnData::Float8(c) => ColumnData::Float8(c.clone()),
			ColumnData::Int1(c) => ColumnData::Int1(c.clone()),
			ColumnData::Int2(c) => ColumnData::Int2(c.clone()),
			ColumnData::Int4(c) => ColumnData::Int4(c.clone()),
			ColumnData::Int8(c) => ColumnData::Int8(c.clone()),
			ColumnData::Int16(c) => ColumnData::Int16(c.clone()),
			ColumnData::Uint1(c) => ColumnData::Uint1(c.clone()),
			ColumnData::Uint2(c) => ColumnData::Uint2(c.clone()),
			ColumnData::Uint4(c) => ColumnData::Uint4(c.clone()),
			ColumnData::Uint8(c) => ColumnData::Uint8(c.clone()),
			ColumnData::Uint16(c) => ColumnData::Uint16(c.clone()),
			ColumnData::Utf8 {
				container,
				max_bytes,
			} => ColumnData::Utf8 {
				container: container.clone(),
				max_bytes: *max_bytes,
			},
			ColumnData::Date(c) => ColumnData::Date(c.clone()),
			ColumnData::DateTime(c) => ColumnData::DateTime(c.clone()),
			ColumnData::Time(c) => ColumnData::Time(c.clone()),
			ColumnData::Duration(c) => ColumnData::Duration(c.clone()),
			ColumnData::IdentityId(c) => ColumnData::IdentityId(c.clone()),
			ColumnData::Uuid4(c) => ColumnData::Uuid4(c.clone()),
			ColumnData::Uuid7(c) => ColumnData::Uuid7(c.clone()),
			ColumnData::Blob {
				container,
				max_bytes,
			} => ColumnData::Blob {
				container: container.clone(),
				max_bytes: *max_bytes,
			},
			ColumnData::Int {
				container,
				max_bytes,
			} => ColumnData::Int {
				container: container.clone(),
				max_bytes: *max_bytes,
			},
			ColumnData::Uint {
				container,
				max_bytes,
			} => ColumnData::Uint {
				container: container.clone(),
				max_bytes: *max_bytes,
			},
			ColumnData::Decimal {
				container,
				precision,
				scale,
			} => ColumnData::Decimal {
				container: container.clone(),
				precision: *precision,
				scale: *scale,
			},
			ColumnData::Any(c) => ColumnData::Any(c.clone()),
			ColumnData::DictionaryId(c) => ColumnData::DictionaryId(c.clone()),
			ColumnData::Undefined(c) => ColumnData::Undefined(c.clone()),
		}
	}
}

impl<S: Storage> PartialEq for ColumnData<S> {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(ColumnData::Bool(a), ColumnData::Bool(b)) => a == b,
			(ColumnData::Float4(a), ColumnData::Float4(b)) => a == b,
			(ColumnData::Float8(a), ColumnData::Float8(b)) => a == b,
			(ColumnData::Int1(a), ColumnData::Int1(b)) => a == b,
			(ColumnData::Int2(a), ColumnData::Int2(b)) => a == b,
			(ColumnData::Int4(a), ColumnData::Int4(b)) => a == b,
			(ColumnData::Int8(a), ColumnData::Int8(b)) => a == b,
			(ColumnData::Int16(a), ColumnData::Int16(b)) => a == b,
			(ColumnData::Uint1(a), ColumnData::Uint1(b)) => a == b,
			(ColumnData::Uint2(a), ColumnData::Uint2(b)) => a == b,
			(ColumnData::Uint4(a), ColumnData::Uint4(b)) => a == b,
			(ColumnData::Uint8(a), ColumnData::Uint8(b)) => a == b,
			(ColumnData::Uint16(a), ColumnData::Uint16(b)) => a == b,
			(
				ColumnData::Utf8 {
					container: a,
					max_bytes: am,
				},
				ColumnData::Utf8 {
					container: b,
					max_bytes: bm,
				},
			) => a == b && am == bm,
			(ColumnData::Date(a), ColumnData::Date(b)) => a == b,
			(ColumnData::DateTime(a), ColumnData::DateTime(b)) => a == b,
			(ColumnData::Time(a), ColumnData::Time(b)) => a == b,
			(ColumnData::Duration(a), ColumnData::Duration(b)) => a == b,
			(ColumnData::IdentityId(a), ColumnData::IdentityId(b)) => a == b,
			(ColumnData::Uuid4(a), ColumnData::Uuid4(b)) => a == b,
			(ColumnData::Uuid7(a), ColumnData::Uuid7(b)) => a == b,
			(
				ColumnData::Blob {
					container: a,
					max_bytes: am,
				},
				ColumnData::Blob {
					container: b,
					max_bytes: bm,
				},
			) => a == b && am == bm,
			(
				ColumnData::Int {
					container: a,
					max_bytes: am,
				},
				ColumnData::Int {
					container: b,
					max_bytes: bm,
				},
			) => a == b && am == bm,
			(
				ColumnData::Uint {
					container: a,
					max_bytes: am,
				},
				ColumnData::Uint {
					container: b,
					max_bytes: bm,
				},
			) => a == b && am == bm,
			(
				ColumnData::Decimal {
					container: a,
					precision: ap,
					scale: as_,
				},
				ColumnData::Decimal {
					container: b,
					precision: bp,
					scale: bs,
				},
			) => a == b && ap == bp && as_ == bs,
			(ColumnData::Any(a), ColumnData::Any(b)) => a == b,
			(ColumnData::DictionaryId(a), ColumnData::DictionaryId(b)) => a == b,
			(ColumnData::Undefined(a), ColumnData::Undefined(b)) => a == b,
			_ => false,
		}
	}
}

impl fmt::Debug for ColumnData<Cow> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ColumnData::Bool(c) => f.debug_tuple("Bool").field(c).finish(),
			ColumnData::Float4(c) => f.debug_tuple("Float4").field(c).finish(),
			ColumnData::Float8(c) => f.debug_tuple("Float8").field(c).finish(),
			ColumnData::Int1(c) => f.debug_tuple("Int1").field(c).finish(),
			ColumnData::Int2(c) => f.debug_tuple("Int2").field(c).finish(),
			ColumnData::Int4(c) => f.debug_tuple("Int4").field(c).finish(),
			ColumnData::Int8(c) => f.debug_tuple("Int8").field(c).finish(),
			ColumnData::Int16(c) => f.debug_tuple("Int16").field(c).finish(),
			ColumnData::Uint1(c) => f.debug_tuple("Uint1").field(c).finish(),
			ColumnData::Uint2(c) => f.debug_tuple("Uint2").field(c).finish(),
			ColumnData::Uint4(c) => f.debug_tuple("Uint4").field(c).finish(),
			ColumnData::Uint8(c) => f.debug_tuple("Uint8").field(c).finish(),
			ColumnData::Uint16(c) => f.debug_tuple("Uint16").field(c).finish(),
			ColumnData::Utf8 {
				container,
				max_bytes,
			} => f.debug_struct("Utf8").field("container", container).field("max_bytes", max_bytes).finish(),
			ColumnData::Date(c) => f.debug_tuple("Date").field(c).finish(),
			ColumnData::DateTime(c) => f.debug_tuple("DateTime").field(c).finish(),
			ColumnData::Time(c) => f.debug_tuple("Time").field(c).finish(),
			ColumnData::Duration(c) => f.debug_tuple("Duration").field(c).finish(),
			ColumnData::IdentityId(c) => f.debug_tuple("IdentityId").field(c).finish(),
			ColumnData::Uuid4(c) => f.debug_tuple("Uuid4").field(c).finish(),
			ColumnData::Uuid7(c) => f.debug_tuple("Uuid7").field(c).finish(),
			ColumnData::Blob {
				container,
				max_bytes,
			} => f.debug_struct("Blob").field("container", container).field("max_bytes", max_bytes).finish(),
			ColumnData::Int {
				container,
				max_bytes,
			} => f.debug_struct("Int").field("container", container).field("max_bytes", max_bytes).finish(),
			ColumnData::Uint {
				container,
				max_bytes,
			} => f.debug_struct("Uint").field("container", container).field("max_bytes", max_bytes).finish(),
			ColumnData::Decimal {
				container,
				precision,
				scale,
			} => f.debug_struct("Decimal")
				.field("container", container)
				.field("precision", precision)
				.field("scale", scale)
				.finish(),
			ColumnData::Any(c) => f.debug_tuple("Any").field(c).finish(),
			ColumnData::DictionaryId(c) => f.debug_tuple("DictionaryId").field(c).finish(),
			ColumnData::Undefined(c) => f.debug_tuple("Undefined").field(c).finish(),
		}
	}
}

impl Serialize for ColumnData<Cow> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
		#[derive(Serialize)]
		enum Helper<'a> {
			Bool(&'a BoolContainer),
			Float4(&'a NumberContainer<f32>),
			Float8(&'a NumberContainer<f64>),
			Int1(&'a NumberContainer<i8>),
			Int2(&'a NumberContainer<i16>),
			Int4(&'a NumberContainer<i32>),
			Int8(&'a NumberContainer<i64>),
			Int16(&'a NumberContainer<i128>),
			Uint1(&'a NumberContainer<u8>),
			Uint2(&'a NumberContainer<u16>),
			Uint4(&'a NumberContainer<u32>),
			Uint8(&'a NumberContainer<u64>),
			Uint16(&'a NumberContainer<u128>),
			Utf8 {
				container: &'a Utf8Container,
				max_bytes: MaxBytes,
			},
			Date(&'a TemporalContainer<Date>),
			DateTime(&'a TemporalContainer<DateTime>),
			Time(&'a TemporalContainer<Time>),
			Duration(&'a TemporalContainer<Duration>),
			IdentityId(&'a IdentityIdContainer),
			Uuid4(&'a UuidContainer<Uuid4>),
			Uuid7(&'a UuidContainer<Uuid7>),
			Blob {
				container: &'a BlobContainer,
				max_bytes: MaxBytes,
			},
			Int {
				container: &'a NumberContainer<Int>,
				max_bytes: MaxBytes,
			},
			Uint {
				container: &'a NumberContainer<Uint>,
				max_bytes: MaxBytes,
			},
			Decimal {
				container: &'a NumberContainer<Decimal>,
				precision: Precision,
				scale: Scale,
			},
			Any(&'a AnyContainer),
			DictionaryId(&'a DictionaryContainer),
			Undefined(&'a UndefinedContainer),
		}
		let helper = match self {
			ColumnData::Bool(c) => Helper::Bool(c),
			ColumnData::Float4(c) => Helper::Float4(c),
			ColumnData::Float8(c) => Helper::Float8(c),
			ColumnData::Int1(c) => Helper::Int1(c),
			ColumnData::Int2(c) => Helper::Int2(c),
			ColumnData::Int4(c) => Helper::Int4(c),
			ColumnData::Int8(c) => Helper::Int8(c),
			ColumnData::Int16(c) => Helper::Int16(c),
			ColumnData::Uint1(c) => Helper::Uint1(c),
			ColumnData::Uint2(c) => Helper::Uint2(c),
			ColumnData::Uint4(c) => Helper::Uint4(c),
			ColumnData::Uint8(c) => Helper::Uint8(c),
			ColumnData::Uint16(c) => Helper::Uint16(c),
			ColumnData::Utf8 {
				container,
				max_bytes,
			} => Helper::Utf8 {
				container,
				max_bytes: *max_bytes,
			},
			ColumnData::Date(c) => Helper::Date(c),
			ColumnData::DateTime(c) => Helper::DateTime(c),
			ColumnData::Time(c) => Helper::Time(c),
			ColumnData::Duration(c) => Helper::Duration(c),
			ColumnData::IdentityId(c) => Helper::IdentityId(c),
			ColumnData::Uuid4(c) => Helper::Uuid4(c),
			ColumnData::Uuid7(c) => Helper::Uuid7(c),
			ColumnData::Blob {
				container,
				max_bytes,
			} => Helper::Blob {
				container,
				max_bytes: *max_bytes,
			},
			ColumnData::Int {
				container,
				max_bytes,
			} => Helper::Int {
				container,
				max_bytes: *max_bytes,
			},
			ColumnData::Uint {
				container,
				max_bytes,
			} => Helper::Uint {
				container,
				max_bytes: *max_bytes,
			},
			ColumnData::Decimal {
				container,
				precision,
				scale,
			} => Helper::Decimal {
				container,
				precision: *precision,
				scale: *scale,
			},
			ColumnData::Any(c) => Helper::Any(c),
			ColumnData::DictionaryId(c) => Helper::DictionaryId(c),
			ColumnData::Undefined(c) => Helper::Undefined(c),
		};
		helper.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for ColumnData<Cow> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		enum Helper {
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
			Any(AnyContainer),
			DictionaryId(DictionaryContainer),
			Undefined(UndefinedContainer),
		}
		let helper = Helper::deserialize(deserializer)?;
		Ok(match helper {
			Helper::Bool(c) => ColumnData::Bool(c),
			Helper::Float4(c) => ColumnData::Float4(c),
			Helper::Float8(c) => ColumnData::Float8(c),
			Helper::Int1(c) => ColumnData::Int1(c),
			Helper::Int2(c) => ColumnData::Int2(c),
			Helper::Int4(c) => ColumnData::Int4(c),
			Helper::Int8(c) => ColumnData::Int8(c),
			Helper::Int16(c) => ColumnData::Int16(c),
			Helper::Uint1(c) => ColumnData::Uint1(c),
			Helper::Uint2(c) => ColumnData::Uint2(c),
			Helper::Uint4(c) => ColumnData::Uint4(c),
			Helper::Uint8(c) => ColumnData::Uint8(c),
			Helper::Uint16(c) => ColumnData::Uint16(c),
			Helper::Utf8 {
				container,
				max_bytes,
			} => ColumnData::Utf8 {
				container,
				max_bytes,
			},
			Helper::Date(c) => ColumnData::Date(c),
			Helper::DateTime(c) => ColumnData::DateTime(c),
			Helper::Time(c) => ColumnData::Time(c),
			Helper::Duration(c) => ColumnData::Duration(c),
			Helper::IdentityId(c) => ColumnData::IdentityId(c),
			Helper::Uuid4(c) => ColumnData::Uuid4(c),
			Helper::Uuid7(c) => ColumnData::Uuid7(c),
			Helper::Blob {
				container,
				max_bytes,
			} => ColumnData::Blob {
				container,
				max_bytes,
			},
			Helper::Int {
				container,
				max_bytes,
			} => ColumnData::Int {
				container,
				max_bytes,
			},
			Helper::Uint {
				container,
				max_bytes,
			} => ColumnData::Uint {
				container,
				max_bytes,
			},
			Helper::Decimal {
				container,
				precision,
				scale,
			} => ColumnData::Decimal {
				container,
				precision,
				scale,
			},
			Helper::Any(c) => ColumnData::Any(c),
			Helper::DictionaryId(c) => ColumnData::DictionaryId(c),
			Helper::Undefined(c) => ColumnData::Undefined(c),
		})
	}
}

/// Extracts the container from every ColumnData variant and evaluates an expression.
macro_rules! with_container {
	($self:expr, |$c:ident| $body:expr) => {
		match $self {
			ColumnData::Bool($c) => $body,
			ColumnData::Float4($c) => $body,
			ColumnData::Float8($c) => $body,
			ColumnData::Int1($c) => $body,
			ColumnData::Int2($c) => $body,
			ColumnData::Int4($c) => $body,
			ColumnData::Int8($c) => $body,
			ColumnData::Int16($c) => $body,
			ColumnData::Uint1($c) => $body,
			ColumnData::Uint2($c) => $body,
			ColumnData::Uint4($c) => $body,
			ColumnData::Uint8($c) => $body,
			ColumnData::Uint16($c) => $body,
			ColumnData::Utf8 {
				container: $c,
				..
			} => $body,
			ColumnData::Date($c) => $body,
			ColumnData::DateTime($c) => $body,
			ColumnData::Time($c) => $body,
			ColumnData::Duration($c) => $body,
			ColumnData::IdentityId($c) => $body,
			ColumnData::Uuid4($c) => $body,
			ColumnData::Uuid7($c) => $body,
			ColumnData::Blob {
				container: $c,
				..
			} => $body,
			ColumnData::Int {
				container: $c,
				..
			} => $body,
			ColumnData::Uint {
				container: $c,
				..
			} => $body,
			ColumnData::Decimal {
				container: $c,
				..
			} => $body,
			ColumnData::Any($c) => $body,
			ColumnData::DictionaryId($c) => $body,
			ColumnData::Undefined($c) => $body,
		}
	};
}

pub(crate) use with_container;

impl<S: Storage> ColumnData<S> {
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
			ColumnData::DictionaryId(_) => Type::DictionaryId,
			ColumnData::Any(_) => Type::Any,
			ColumnData::Undefined(_) => Type::Option(Box::new(Type::Boolean)),
		}
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		match self {
			ColumnData::Bool(c) => c.is_defined(idx),
			ColumnData::Float4(c) => c.is_defined(idx),
			ColumnData::Float8(c) => c.is_defined(idx),
			ColumnData::Int1(c) => c.is_defined(idx),
			ColumnData::Int2(c) => c.is_defined(idx),
			ColumnData::Int4(c) => c.is_defined(idx),
			ColumnData::Int8(c) => c.is_defined(idx),
			ColumnData::Int16(c) => c.is_defined(idx),
			ColumnData::Uint1(c) => c.is_defined(idx),
			ColumnData::Uint2(c) => c.is_defined(idx),
			ColumnData::Uint4(c) => c.is_defined(idx),
			ColumnData::Uint8(c) => c.is_defined(idx),
			ColumnData::Uint16(c) => c.is_defined(idx),
			ColumnData::Utf8 {
				container: c,
				..
			} => c.is_defined(idx),
			ColumnData::Date(c) => c.is_defined(idx),
			ColumnData::DateTime(c) => c.is_defined(idx),
			ColumnData::Time(c) => c.is_defined(idx),
			ColumnData::Duration(c) => c.is_defined(idx),
			ColumnData::IdentityId(container) => container.get(idx).is_some(),
			ColumnData::Uuid4(c) => c.is_defined(idx),
			ColumnData::Uuid7(c) => c.is_defined(idx),
			ColumnData::Blob {
				container: c,
				..
			} => c.is_defined(idx),
			ColumnData::Int {
				container: c,
				..
			} => c.is_defined(idx),
			ColumnData::Uint {
				container: c,
				..
			} => c.is_defined(idx),
			ColumnData::Decimal {
				container: c,
				..
			} => c.is_defined(idx),
			ColumnData::DictionaryId(c) => c.is_defined(idx),
			ColumnData::Any(c) => c.is_defined(idx),
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

impl<S: Storage> ColumnData<S> {
	pub fn bitvec(&self) -> &S::BitVec {
		match self {
			ColumnData::Bool(c) => c.bitvec(),
			ColumnData::Float4(c) => c.bitvec(),
			ColumnData::Float8(c) => c.bitvec(),
			ColumnData::Int1(c) => c.bitvec(),
			ColumnData::Int2(c) => c.bitvec(),
			ColumnData::Int4(c) => c.bitvec(),
			ColumnData::Int8(c) => c.bitvec(),
			ColumnData::Int16(c) => c.bitvec(),
			ColumnData::Uint1(c) => c.bitvec(),
			ColumnData::Uint2(c) => c.bitvec(),
			ColumnData::Uint4(c) => c.bitvec(),
			ColumnData::Uint8(c) => c.bitvec(),
			ColumnData::Uint16(c) => c.bitvec(),
			ColumnData::Utf8 {
				container: c,
				..
			} => c.bitvec(),
			ColumnData::Date(c) => c.bitvec(),
			ColumnData::DateTime(c) => c.bitvec(),
			ColumnData::Time(c) => c.bitvec(),
			ColumnData::Duration(c) => c.bitvec(),
			ColumnData::IdentityId(c) => c.bitvec(),
			ColumnData::Uuid4(c) => c.bitvec(),
			ColumnData::Uuid7(c) => c.bitvec(),
			ColumnData::Blob {
				container: c,
				..
			} => c.bitvec(),
			ColumnData::Int {
				container: c,
				..
			} => c.bitvec(),
			ColumnData::Uint {
				container: c,
				..
			} => c.bitvec(),
			ColumnData::Decimal {
				container: c,
				..
			} => c.bitvec(),
			ColumnData::DictionaryId(c) => c.bitvec(),
			ColumnData::Any(c) => c.bitvec(),
			ColumnData::Undefined(_) => unreachable!(),
		}
	}

	pub fn undefined_count(&self) -> usize {
		DataBitVec::count_zeros(self.bitvec())
	}
}

impl<S: Storage> ColumnData<S> {
	pub fn len(&self) -> usize {
		with_container!(self, |c| c.len())
	}

	pub fn capacity(&self) -> usize {
		with_container!(self, |c| c.capacity())
	}

	/// Clear all data, retaining the allocated capacity for reuse.
	pub fn clear(&mut self) {
		match self {
			ColumnData::Undefined(c) => c.clear(),
			_ => with_container!(self, |c| c.clear()),
		}
	}

	pub fn as_string(&self, index: usize) -> String {
		with_container!(self, |c| c.as_string(index))
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
			Type::DictionaryId => Self::dictionary_id_with_capacity(capacity),
			Type::Option(_) => ColumnData::undefined(0),
			Type::Any => Self::any_with_capacity(capacity),
		}
	}

	pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Value> + 'a> {
		Box::new((0..self.len()).map(move |i| self.get_value(i)))
	}
}
