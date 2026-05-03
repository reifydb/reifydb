// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod extend;
pub mod factory;
pub mod filter;
pub mod from;
pub mod get;
pub mod pool;
pub mod reorder;
pub mod scatter;
pub mod slice;
pub mod take;

use std::fmt;

use reifydb_type::{
	storage::{Cow, DataBitVec, Storage},
	util::bitvec::BitVec,
	value::{
		Value,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		container::{
			any::AnyContainer, blob::BlobContainer, bool::BoolContainer, dictionary::DictionaryContainer,
			identity_id::IdentityIdContainer, number::NumberContainer, temporal::TemporalContainer,
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
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub enum ColumnBuffer<S: Storage = Cow> {
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

	Any(AnyContainer<S>),

	DictionaryId(DictionaryContainer<S>),

	Option {
		inner: Box<ColumnBuffer<S>>,
		bitvec: S::BitVec,
	},
}

impl<S: Storage> Clone for ColumnBuffer<S> {
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
			ColumnBuffer::Any(c) => ColumnBuffer::Any(c.clone()),
			ColumnBuffer::DictionaryId(c) => ColumnBuffer::DictionaryId(c.clone()),
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => ColumnBuffer::Option {
				inner: inner.clone(),
				bitvec: bitvec.clone(),
			},
		}
	}
}

impl<S: Storage> PartialEq for ColumnBuffer<S> {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(ColumnBuffer::Bool(a), ColumnBuffer::Bool(b)) => a == b,
			(ColumnBuffer::Float4(a), ColumnBuffer::Float4(b)) => a == b,
			(ColumnBuffer::Float8(a), ColumnBuffer::Float8(b)) => a == b,
			(ColumnBuffer::Int1(a), ColumnBuffer::Int1(b)) => a == b,
			(ColumnBuffer::Int2(a), ColumnBuffer::Int2(b)) => a == b,
			(ColumnBuffer::Int4(a), ColumnBuffer::Int4(b)) => a == b,
			(ColumnBuffer::Int8(a), ColumnBuffer::Int8(b)) => a == b,
			(ColumnBuffer::Int16(a), ColumnBuffer::Int16(b)) => a == b,
			(ColumnBuffer::Uint1(a), ColumnBuffer::Uint1(b)) => a == b,
			(ColumnBuffer::Uint2(a), ColumnBuffer::Uint2(b)) => a == b,
			(ColumnBuffer::Uint4(a), ColumnBuffer::Uint4(b)) => a == b,
			(ColumnBuffer::Uint8(a), ColumnBuffer::Uint8(b)) => a == b,
			(ColumnBuffer::Uint16(a), ColumnBuffer::Uint16(b)) => a == b,
			(
				ColumnBuffer::Utf8 {
					container: a,
					max_bytes: am,
				},
				ColumnBuffer::Utf8 {
					container: b,
					max_bytes: bm,
				},
			) => a == b && am == bm,
			(ColumnBuffer::Date(a), ColumnBuffer::Date(b)) => a == b,
			(ColumnBuffer::DateTime(a), ColumnBuffer::DateTime(b)) => a == b,
			(ColumnBuffer::Time(a), ColumnBuffer::Time(b)) => a == b,
			(ColumnBuffer::Duration(a), ColumnBuffer::Duration(b)) => a == b,
			(ColumnBuffer::IdentityId(a), ColumnBuffer::IdentityId(b)) => a == b,
			(ColumnBuffer::Uuid4(a), ColumnBuffer::Uuid4(b)) => a == b,
			(ColumnBuffer::Uuid7(a), ColumnBuffer::Uuid7(b)) => a == b,
			(
				ColumnBuffer::Blob {
					container: a,
					max_bytes: am,
				},
				ColumnBuffer::Blob {
					container: b,
					max_bytes: bm,
				},
			) => a == b && am == bm,
			(
				ColumnBuffer::Int {
					container: a,
					max_bytes: am,
				},
				ColumnBuffer::Int {
					container: b,
					max_bytes: bm,
				},
			) => a == b && am == bm,
			(
				ColumnBuffer::Uint {
					container: a,
					max_bytes: am,
				},
				ColumnBuffer::Uint {
					container: b,
					max_bytes: bm,
				},
			) => a == b && am == bm,
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
			) => a == b && ap == bp && as_ == bs,
			(ColumnBuffer::Any(a), ColumnBuffer::Any(b)) => a == b,
			(ColumnBuffer::DictionaryId(a), ColumnBuffer::DictionaryId(b)) => a == b,
			(
				ColumnBuffer::Option {
					inner: ai,
					bitvec: ab,
				},
				ColumnBuffer::Option {
					inner: bi,
					bitvec: bb,
				},
			) => ai == bi && ab == bb,
			_ => false,
		}
	}
}

impl fmt::Debug for ColumnBuffer<Cow> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
			ColumnBuffer::Any(c) => f.debug_tuple("Any").field(c).finish(),
			ColumnBuffer::DictionaryId(c) => f.debug_tuple("DictionaryId").field(c).finish(),
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => f.debug_struct("Option").field("inner", inner).field("bitvec", bitvec).finish(),
		}
	}
}

impl Serialize for ColumnBuffer<Cow> {
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
			Option {
				inner: &'a ColumnBuffer,
				bitvec: &'a BitVec,
			},
		}
		let helper = match self {
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
			ColumnBuffer::Any(c) => Helper::Any(c),
			ColumnBuffer::DictionaryId(c) => Helper::DictionaryId(c),
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => Helper::Option {
				inner: inner.as_ref(),
				bitvec,
			},
		};
		helper.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for ColumnBuffer<Cow> {
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
			Option {
				inner: Box<ColumnBuffer>,
				bitvec: BitVec,
			},
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
			Helper::Any(c) => ColumnBuffer::Any(c),
			Helper::DictionaryId(c) => ColumnBuffer::DictionaryId(c),
			Helper::Option {
				inner,
				bitvec,
			} => ColumnBuffer::Option {
				inner,
				bitvec,
			},
		})
	}
}

macro_rules! with_container {
	($self:expr, |$c:ident| $body:expr) => {
		match $self {
			ColumnBuffer::Bool($c) => $body,
			ColumnBuffer::Float4($c) => $body,
			ColumnBuffer::Float8($c) => $body,
			ColumnBuffer::Int1($c) => $body,
			ColumnBuffer::Int2($c) => $body,
			ColumnBuffer::Int4($c) => $body,
			ColumnBuffer::Int8($c) => $body,
			ColumnBuffer::Int16($c) => $body,
			ColumnBuffer::Uint1($c) => $body,
			ColumnBuffer::Uint2($c) => $body,
			ColumnBuffer::Uint4($c) => $body,
			ColumnBuffer::Uint8($c) => $body,
			ColumnBuffer::Uint16($c) => $body,
			ColumnBuffer::Utf8 {
				container: $c,
				..
			} => $body,
			ColumnBuffer::Date($c) => $body,
			ColumnBuffer::DateTime($c) => $body,
			ColumnBuffer::Time($c) => $body,
			ColumnBuffer::Duration($c) => $body,
			ColumnBuffer::IdentityId($c) => $body,
			ColumnBuffer::Uuid4($c) => $body,
			ColumnBuffer::Uuid7($c) => $body,
			ColumnBuffer::Blob {
				container: $c,
				..
			} => $body,
			ColumnBuffer::Int {
				container: $c,
				..
			} => $body,
			ColumnBuffer::Uint {
				container: $c,
				..
			} => $body,
			ColumnBuffer::Decimal {
				container: $c,
				..
			} => $body,
			ColumnBuffer::Any($c) => $body,
			ColumnBuffer::DictionaryId($c) => $body,
			ColumnBuffer::Option {
				..
			} => {
				unreachable!(
					"with_container! must not be called on Option variant directly; handle it explicitly"
				)
			}
		}
	};
}

pub(crate) use with_container;

impl<S: Storage> ColumnBuffer<S> {
	pub fn unwrap_option(&self) -> (&ColumnBuffer<S>, Option<&S::BitVec>) {
		match self {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => (inner.as_ref(), Some(bitvec)),
			other => (other, None),
		}
	}

	pub fn into_unwrap_option(self) -> (ColumnBuffer<S>, Option<S::BitVec>) {
		match self {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => (*inner, Some(bitvec)),
			other => (other, None),
		}
	}

	pub fn get_type(&self) -> Type {
		match self {
			ColumnBuffer::Bool(_) => Type::Boolean,
			ColumnBuffer::Float4(_) => Type::Float4,
			ColumnBuffer::Float8(_) => Type::Float8,
			ColumnBuffer::Int1(_) => Type::Int1,
			ColumnBuffer::Int2(_) => Type::Int2,
			ColumnBuffer::Int4(_) => Type::Int4,
			ColumnBuffer::Int8(_) => Type::Int8,
			ColumnBuffer::Int16(_) => Type::Int16,
			ColumnBuffer::Uint1(_) => Type::Uint1,
			ColumnBuffer::Uint2(_) => Type::Uint2,
			ColumnBuffer::Uint4(_) => Type::Uint4,
			ColumnBuffer::Uint8(_) => Type::Uint8,
			ColumnBuffer::Uint16(_) => Type::Uint16,
			ColumnBuffer::Utf8 {
				..
			} => Type::Utf8,
			ColumnBuffer::Date(_) => Type::Date,
			ColumnBuffer::DateTime(_) => Type::DateTime,
			ColumnBuffer::Time(_) => Type::Time,
			ColumnBuffer::Duration(_) => Type::Duration,
			ColumnBuffer::IdentityId(_) => Type::IdentityId,
			ColumnBuffer::Uuid4(_) => Type::Uuid4,
			ColumnBuffer::Uuid7(_) => Type::Uuid7,
			ColumnBuffer::Blob {
				..
			} => Type::Blob,
			ColumnBuffer::Int {
				..
			} => Type::Int,
			ColumnBuffer::Uint {
				..
			} => Type::Uint,
			ColumnBuffer::Decimal {
				..
			} => Type::Decimal,
			ColumnBuffer::DictionaryId(_) => Type::DictionaryId,
			ColumnBuffer::Any(_) => Type::Any,
			ColumnBuffer::Option {
				inner,
				..
			} => Type::Option(Box::new(inner.get_type())),
		}
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		match self {
			ColumnBuffer::Bool(c) => c.is_defined(idx),
			ColumnBuffer::Float4(c) => c.is_defined(idx),
			ColumnBuffer::Float8(c) => c.is_defined(idx),
			ColumnBuffer::Int1(c) => c.is_defined(idx),
			ColumnBuffer::Int2(c) => c.is_defined(idx),
			ColumnBuffer::Int4(c) => c.is_defined(idx),
			ColumnBuffer::Int8(c) => c.is_defined(idx),
			ColumnBuffer::Int16(c) => c.is_defined(idx),
			ColumnBuffer::Uint1(c) => c.is_defined(idx),
			ColumnBuffer::Uint2(c) => c.is_defined(idx),
			ColumnBuffer::Uint4(c) => c.is_defined(idx),
			ColumnBuffer::Uint8(c) => c.is_defined(idx),
			ColumnBuffer::Uint16(c) => c.is_defined(idx),
			ColumnBuffer::Utf8 {
				container: c,
				..
			} => c.is_defined(idx),
			ColumnBuffer::Date(c) => c.is_defined(idx),
			ColumnBuffer::DateTime(c) => c.is_defined(idx),
			ColumnBuffer::Time(c) => c.is_defined(idx),
			ColumnBuffer::Duration(c) => c.is_defined(idx),
			ColumnBuffer::IdentityId(container) => container.get(idx).is_some(),
			ColumnBuffer::Uuid4(c) => c.is_defined(idx),
			ColumnBuffer::Uuid7(c) => c.is_defined(idx),
			ColumnBuffer::Blob {
				container: c,
				..
			} => c.is_defined(idx),
			ColumnBuffer::Int {
				container: c,
				..
			} => c.is_defined(idx),
			ColumnBuffer::Uint {
				container: c,
				..
			} => c.is_defined(idx),
			ColumnBuffer::Decimal {
				container: c,
				..
			} => c.is_defined(idx),
			ColumnBuffer::DictionaryId(c) => c.is_defined(idx),
			ColumnBuffer::Any(c) => c.is_defined(idx),
			ColumnBuffer::Option {
				bitvec,
				..
			} => idx < DataBitVec::len(bitvec) && DataBitVec::get(bitvec, idx),
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
				| Type::Uint | Type::Decimal
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

impl<S: Storage> ColumnBuffer<S> {
	pub fn none_count(&self) -> usize {
		match self {
			ColumnBuffer::Option {
				bitvec,
				..
			} => DataBitVec::count_zeros(bitvec),
			_ => 0,
		}
	}
}

impl<S: Storage> ColumnBuffer<S> {
	pub fn len(&self) -> usize {
		match self {
			ColumnBuffer::Option {
				inner,
				..
			} => inner.len(),
			_ => with_container!(self, |c| c.len()),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn capacity(&self) -> usize {
		match self {
			ColumnBuffer::Option {
				inner,
				..
			} => inner.capacity(),
			_ => with_container!(self, |c| c.capacity()),
		}
	}

	pub fn clear(&mut self) {
		match self {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				inner.clear();
				DataBitVec::clear(bitvec);
			}
			_ => with_container!(self, |c| c.clear()),
		}
	}

	pub fn as_string(&self, index: usize) -> String {
		match self {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				if index < DataBitVec::len(bitvec) && DataBitVec::get(bitvec, index) {
					inner.as_string(index)
				} else {
					"none".to_string()
				}
			}
			_ => with_container!(self, |c| c.as_string(index)),
		}
	}
}

impl ColumnBuffer {
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
			Type::Option(inner) => ColumnBuffer::Option {
				inner: Box::new(ColumnBuffer::with_capacity(*inner, capacity)),
				bitvec: BitVec::with_capacity(capacity),
			},
			Type::Any | Type::List(_) | Type::Record(_) | Type::Tuple(_) => {
				Self::any_with_capacity(capacity)
			}
		}
	}

	pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Value> + 'a> {
		Box::new((0..self.len()).map(move |i| self.get_value(i)))
	}
}
